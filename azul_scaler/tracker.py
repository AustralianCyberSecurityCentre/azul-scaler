"""The Tracker periodically samples Burrow to determine an ETA for a workload to reach the top of its queue."""

import asyncio
import datetime
import logging
import math
import statistics
import sys
from typing import Optional

from pydantic import BaseModel, Field

from azul_scaler import settings
from azul_scaler.burrower import Burrower
from azul_scaler.kv import KVStorage
from azul_scaler.types import ConsumerGroupType, PluginScalerMetadata


class ConsumerGroupProgress(BaseModel):
    """Current progress for a plugin through a topic."""

    # The number of events added in the reference period.
    added: int
    # The number of events consumed in the reference period.
    processed: int
    # The total number of events still pending in the queue.
    lag: int
    # The time deltas measured (in milliseconds) between the start and end of sampling periods.
    time_deltas: list[int]


class TrackedRegistration(BaseModel):
    """A registration event for a watch for a set of consumer groups."""

    last_seen: datetime.datetime
    metadata: PluginScalerMetadata = Field(discriminator="entity_type")


class TrackedInfo(TrackedRegistration):
    """A set of consumer groups that are being continuously tracked."""

    progress: ConsumerGroupProgress | None


INTERVAL_TIME = datetime.timedelta(seconds=30)


class WatchMatcher[T]:
    """Interface for matching watches to consumer groups in the cluster."""

    async def get_matching_consumer_groups(self, cluster_groups: list[str], metadata: T) -> list[str]:
        """Returns a list of consumer groups which match the given metadata."""
        raise Exception("Not implemented")


class PluginMatcher(WatchMatcher[PluginScalerMetadata]):
    """Consumer group identifier for Azul plugins."""

    def __init__(self):
        self.kv = KVStorage()
        self.logger = logging.getLogger("plugin_matcher")

    async def get_matching_consumer_groups(self, cluster_groups, metadata):
        """Identifies matching consumer groups for a specific plugin (there can be multiple - eg. live + expedite)."""
        # Convert the deployment key we have to a kafka prefix via redis & dispatcher
        prefix = await self.kv.get_plugin_prefix(metadata.plugin_name)
        if prefix is None:
            self.logger.warning("Rejecting client as they have an unknown deployment key: %s", metadata.plugin_name)
            return []

        # Per-plugin prefix
        prefix = prefix.lower()
        # Global prefix (usually the current topic partition)
        global_prefix = settings.settings.topic_prefix.lower()

        # Identify viable consumers based on the prefix provided
        viable_consumers = []
        for consumer in cluster_groups:
            lower_consumer = consumer.lower()
            if (
                lower_consumer.startswith(global_prefix)
                and prefix in lower_consumer
                and (metadata.include_historic or not lower_consumer.endswith("-historic"))
            ):
                viable_consumers.append(consumer)

        return viable_consumers


class Tracker:
    """Tracks sets of consumer groups ("watches") to determine processing throughput metrics."""

    _tracked_watches: dict[str, TrackedInfo]

    # Updates to consumer groups that haven't yet been applied.
    # Updating all consumer groups can take time. Pending updates contains consumer groups
    # that have just been seen and get periodically synced to tracked_consumer_groups.
    _pending_updates: list[tuple[str, TrackedRegistration]]
    _pending_updates_lock: asyncio.Lock

    def __init__(self):
        self._tracked_watches = {}
        self._pending_updates = []
        self._pending_updates_lock = asyncio.Lock()

        self._started = False

        self.burrower = Burrower()
        self.consumer_group_matchers: dict[ConsumerGroupType, WatchMatcher] = {
            ConsumerGroupType.Plugin: PluginMatcher()
        }

        self.logger = logging.getLogger("scaler_tracker")

    async def _determine_cluster(self):
        """Determines which Kafka cluster to use (the first one that Burrow is watching)."""
        clusters = await self.burrower.get_clusters()
        if len(clusters) == 0:
            raise Exception("Burrow returned no clusters - is it running?")

        self.cluster = clusters[0]
        self.logger.info("Using Kafka cluster: %s", self.cluster)

    async def _determine_progress_for_consumer_groups(self, consumers: list[str]) -> ConsumerGroupProgress:
        """Given some list of names for Kafka consumer groups, this polls those consumer groups for progress stats.

        This summmarises the number of added events (from e.g. file submissions), the number of processed events
        (i.e how far a plugin has gotten in processing existing events), the lag count (the total number of events
        not yet processed) and the timeframes that Burrow has sampled this data for each consumer group (to be used
        for calculating a per minute metric; depending on how busy a consumer group is, Burrow's timeframes for
        sampling might slightly change).
        """
        consumers_detail = await asyncio.gather(
            *[self.burrower.get_consumer_detail(self.cluster, consumer) for consumer in consumers]
        )

        added = 0
        processed = 0
        lag = 0

        time_deltas = []

        # Python emits timestamps on a second level, burrow uses millisecond level timestmaps
        mins_15_ago = (datetime.datetime.now() - datetime.timedelta(minutes=15)).timestamp() * 1e3

        for detail in consumers_detail:
            for topic in detail.values():
                for partition in topic:
                    offsets = sorted(
                        (x for x in partition.offsets if x is not None and x.timestamp > mins_15_ago),
                        key=lambda x: x.timestamp,
                    )

                    # Calculate the delta from the latest to the earliest in the set of partitions
                    # If less than 2 offsets, we can't calculate a delta (this is either due to a new partition or
                    # outdated data)
                    if len(offsets) >= 2:
                        oldest = offsets[0]
                        latest = offsets[-1]

                        added += (latest.offset + latest.lag) - (oldest.offset + oldest.lag)
                        processed += latest.offset - oldest.offset
                        lag += latest.lag

                        time_deltas.append(latest.timestamp - oldest.timestamp)

        return ConsumerGroupProgress(added=added, processed=processed, lag=lag, time_deltas=time_deltas)

    async def _update_group(self, consumers: list[str], name: str, group: TrackedInfo):
        """Updates a single tracker instance.

        This fetches information about its consumer groups and determines summarised information about
        their latency & queue sizes.
        """
        # Filter the available consumer groups for this tracked group
        matcher = self.consumer_group_matchers[group.metadata.entity_type]
        matching_groups = await matcher.get_matching_consumer_groups(consumers, group.metadata)

        if len(matching_groups) == 0:
            self.logger.warning("Failed to produce any consumer group matches for: %s", name)
            # Drop existing metadata as this might be misleading
            group.progress = None
            return

        current_progress = await self._determine_progress_for_consumer_groups(matching_groups)

        self.logger.debug("%s progress update: %s", name, current_progress)

        group.progress = current_progress

    async def _tick(self):
        """Iterate over all registered watches, downloads information from Burrow and stores for later."""
        # Apply pending changes
        async with self._pending_updates_lock:
            for [name, update] in self._pending_updates:
                if name in self._tracked_watches:
                    tracked_group = self._tracked_watches[name]
                    if tracked_group.last_seen < update.last_seen:
                        tracked_group.last_seen = update.last_seen
                else:
                    self.logger.info("Starting to track new consumer group: %s", name)
                    self._tracked_watches[name] = TrackedInfo(progress=None, **update.model_dump())

            self._pending_updates.clear()

            # Determine any consumer groups to age off (after 10 minutes as that should be enough
            # for a Kubernetes node upgrade if we have been disrupted)
            to_delete = []
            cur_time = datetime.datetime.now()
            for [name, group] in self._tracked_watches.items():
                if (cur_time - group.last_seen) > datetime.timedelta(minutes=10):
                    self.logger.info("Aging off consumer group not heard from in a while: %s", name)
                    to_delete.append(name)

            for name in to_delete:
                del self._tracked_watches[name]

        self.logger.debug("Finished processing pending updates")

        # It is safe for async tasks to read from tracked consumer groups now (this isn't threading;
        # we won't get half-written lists here)
        # Fetch all consumer groups
        consumers = await self.burrower.get_consumers(self.cluster)

        self.logger.debug("Iterating consumer groups")

        # Update consumer groups in parallel
        tasks = [self._update_group(consumers, name, group) for [name, group] in self._tracked_watches.items()]
        if len(tasks) > 0:
            await asyncio.gather(*tasks)

        self.logger.info("Updated consumer group information")

    async def start(self):
        """Starts the background event loop for the Tracker.

        This periodically samples Burrow for information about registered watches and compiles
        this information for processing on-demand.

        This coroutine will never complete.
        """
        try:
            if self._started:
                raise Exception("Cannot start a tracker more than once!")

            self._started = True

            await self._determine_cluster()

            self.logger.info("Determined cluster")

            # Update consumer groups every ~30 seconds (if Burrow keeps up)
            last_started: datetime.datetime | None = None

            while True:
                if last_started is not None:
                    last_op_time = datetime.datetime.now() - last_started
                    if last_op_time < INTERVAL_TIME:
                        # Last operation took less than 30 seconds. Sleep to rate limit.
                        sleep_time = INTERVAL_TIME - last_op_time
                        duration = sleep_time.total_seconds()
                        self.logger.info("Consumer tracker sleeping for %.02f seconds", duration)
                        await asyncio.sleep(duration)

                self.logger.debug("Woken up; starting iteration")

                last_started = datetime.datetime.now()
                try:
                    await self._tick()
                except Exception:
                    self.logger.exception("Failed to tick tracker", stack_info=True)
        except Exception:
            self.logger.exception("Fatal error running tracker", stack_info=True)
            sys.exit(1)

    async def register_watch(self, name: str, metadata: PluginScalerMetadata):
        """Registers a consumer group(s) watch for statistics collection.

        This queues the addition of a new watch for the background processor so that it can start
        populating results for this watch with Burrow info.

        Watches actively age-off if not continuously registered for garbage collection reasons, and
        subsequently this must be called periodically (at least once every <10 minutes).
        """
        # FUTURE: You should just be able to add another metadata type in the type to accept
        #         different kinds of consumer groups
        cur_time = datetime.datetime.now()

        async with self._pending_updates_lock:
            self._pending_updates.append((name, TrackedRegistration(last_seen=cur_time, metadata=metadata)))

    async def get_watch_change(self, name: str) -> Optional[float]:
        """Gets the ETA for event completion in a given watch.

        This takes cached information from Burrow (from the background Watcher async task) to derive metrics
        based on how many files were processed & added in a given timeframe.
        """
        tracked = self._tracked_watches.get(name)
        if tracked is None:
            # Race condition between registering and waiting ~30 seconds for new data
            # This is fine, though don't let Kubernetes make any decisions yet (i.e return None)
            self.logger.warning("%s hasn't finished registration yet; unable to make a scaling decision", name)
            return None

        # Convert the high-level information into a time to completion metric
        progress = tracked.progress
        if progress is None:
            # There isn't any offset information available - typically due to the
            # queue not having had anything added/removed since burrow last restarted
            # Return 0 as it is possible this plugin has nothing in its queues
            self.logger.debug("Not enough info on progress for %s to make a scaling decision", name)
            return 0

        if progress.processed != 0:
            # Calculate a time factor for this measurement to normalise it to minute increments
            # Data from burrow can be sampled over a burrow defined time period (which can be more or less than
            # a minute), but we want to report minute increments to kubernetes for ease of configuration
            if len(progress.time_deltas) == 0:
                # No data was actually recorded from burrow
                self.logger.warning("%s doesn't have any time delta information!", name)
                time_delta = 1
            else:
                time_delta = statistics.mean(progress.time_deltas) / 60000

            self.logger.debug("%s: time factor of %.02f", name, time_delta)

            rate = (progress.processed - progress.added) / time_delta
            if rate < 1:
                # We aren't making progress - more events have been added to this plugin's
                # queue than have been consumed
                if progress.lag <= 2:
                    # We don't have much of a backlog, so just default to 1
                    rate = 1
                else:
                    self.logger.info("%s is falling behind (rate: %d, lag: %d)", name, rate, progress.lag)
                    # Scale this to increase the presented ETA such that a higher rate
                    # inflates the reported lag
                    # e.g. 101 events added with 1 processed -> rate of -100
                    #      -> ~10.05
                    rate = math.sqrt(-rate + 1)
                    # Cap this rate at 100 - if you are above this there is no further signal
                    # we can send to the HPA to tell them we are falling behind bad
                    if rate > 100:
                        rate = 100
                    #      -> 1 - 0.10
                    #      -> 0.90
                    # The higher the rate, the greater the impact at division time below
                    rate = 1 - (rate / 100)

                    if rate < 0.0001:
                        # FP rounding - just report the raw lag
                        rate = 1

            time_to_completion = progress.lag / rate
        else:
            # We've processed 0 files in the burrow sample period. Nice.
            # Report the raw lag as this will at least signal to Kubernetes the approximate need to scale
            # up or down
            time_to_completion = progress.lag

        if time_to_completion > 1000000:
            # Magic constant to report that we are doomed
            time_to_completion = 9999999

        return time_to_completion
