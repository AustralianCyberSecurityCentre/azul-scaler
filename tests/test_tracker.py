import datetime
import math
from typing import Optional
from unittest.mock import AsyncMock

import pytest

from azul_scaler.burrower import ConsumerTopicDetail, ConsumerTopicPartitionDetail
from azul_scaler.tracker import Tracker
from azul_scaler.types import ConsumerGroupType, PluginScalerMetadata


def _fake_get_consumer_detail(cluster: str, consumer: str) -> dict[str, list[ConsumerTopicDetail]]:
    """Handles the get_consumer_detail call for Burrow."""
    assert cluster == "local"

    timestamp_a = int((datetime.datetime.now() - datetime.timedelta(minutes=1)).timestamp() * 1e3)
    timestamp_b = int((datetime.datetime.now() - datetime.timedelta(minutes=2)).timestamp() * 1e3)

    responses = {
        "correctglobalprefix-correctprefix": {
            "topic1": [
                ConsumerTopicDetail(
                    current_lag=1,
                    offsets=[
                        ConsumerTopicPartitionDetail(lag=1, offset=1, timestamp=timestamp_b, observedAt=timestamp_b),
                        ConsumerTopicPartitionDetail(lag=1, offset=2, timestamp=timestamp_a, observedAt=timestamp_a),
                    ],
                ),
                ConsumerTopicDetail(
                    current_lag=2,
                    offsets=[
                        ConsumerTopicPartitionDetail(lag=2, offset=1, timestamp=timestamp_b, observedAt=timestamp_b),
                        ConsumerTopicPartitionDetail(lag=2, offset=2, timestamp=timestamp_a, observedAt=timestamp_a),
                    ],
                ),
            ]
        },
        "correctglobalprefix-correctprefix-historic": {
            "topic1": [
                ConsumerTopicDetail(
                    current_lag=3,
                    offsets=[
                        ConsumerTopicPartitionDetail(lag=4, offset=1, timestamp=timestamp_b, observedAt=timestamp_b),
                        ConsumerTopicPartitionDetail(lag=3, offset=2, timestamp=timestamp_a, observedAt=timestamp_a),
                    ],
                )
            ]
        },
        "notcorrectprefix-correctprefix": {
            "topic1": [
                ConsumerTopicDetail(
                    current_lag=8,
                    offsets=[
                        ConsumerTopicPartitionDetail(lag=8, offset=1, timestamp=timestamp_b, observedAt=timestamp_b),
                        ConsumerTopicPartitionDetail(lag=8, offset=2, timestamp=timestamp_a, observedAt=timestamp_a),
                    ],
                )
            ]
        },
        "correctglobalprefix-fakeprefix": {
            "topic1": [
                ConsumerTopicDetail(
                    current_lag=16,
                    offsets=[
                        ConsumerTopicPartitionDetail(lag=16, offset=1, timestamp=timestamp_b, observedAt=timestamp_b),
                        ConsumerTopicPartitionDetail(lag=16, offset=2, timestamp=timestamp_a, observedAt=timestamp_a),
                    ],
                )
            ]
        },
    }
    return responses[consumer]


def _fake_get_consumer_detail_lagging(cluster: str, consumer: str) -> dict[str, list[ConsumerTopicDetail]]:
    """Handles the get_consumer_detail call for Burrow if we were falling behind."""
    assert cluster == "local"

    timestamp_a = int((datetime.datetime.now() - datetime.timedelta(minutes=1)).timestamp() * 1e3)
    timestamp_b = int((datetime.datetime.now() - datetime.timedelta(minutes=2)).timestamp() * 1e3)

    responses = {
        "correctglobalprefix-correctprefix": {
            "topic1": [
                ConsumerTopicDetail(
                    current_lag=1,
                    offsets=[
                        ConsumerTopicPartitionDetail(lag=2, offset=100, timestamp=timestamp_b, observedAt=timestamp_b),
                        ConsumerTopicPartitionDetail(lag=9, offset=112, timestamp=timestamp_a, observedAt=timestamp_a),
                    ],
                ),
                ConsumerTopicDetail(
                    current_lag=2,
                    offsets=[
                        ConsumerTopicPartitionDetail(lag=4, offset=100, timestamp=timestamp_b, observedAt=timestamp_b),
                        ConsumerTopicPartitionDetail(lag=6, offset=104, timestamp=timestamp_a, observedAt=timestamp_a),
                    ],
                ),
            ]
        },
        "correctglobalprefix-correctprefix-historic": {
            "topic1": [
                ConsumerTopicDetail(
                    current_lag=3,
                    offsets=[
                        ConsumerTopicPartitionDetail(lag=8, offset=100, timestamp=timestamp_b, observedAt=timestamp_b),
                        ConsumerTopicPartitionDetail(lag=7, offset=103, timestamp=timestamp_a, observedAt=timestamp_a),
                    ],
                )
            ]
        },
        "notcorrectprefix-correctprefix": {
            "topic1": [
                ConsumerTopicDetail(
                    current_lag=8,
                    offsets=[
                        ConsumerTopicPartitionDetail(
                            lag=16, offset=100, timestamp=timestamp_b, observedAt=timestamp_b
                        ),
                        ConsumerTopicPartitionDetail(
                            lag=16, offset=103, timestamp=timestamp_a, observedAt=timestamp_a
                        ),
                    ],
                )
            ]
        },
        "correctglobalprefix-fakeprefix": {
            "topic1": [
                ConsumerTopicDetail(
                    current_lag=16,
                    offsets=[
                        ConsumerTopicPartitionDetail(
                            lag=32, offset=100, timestamp=timestamp_b, observedAt=timestamp_b
                        ),
                        ConsumerTopicPartitionDetail(
                            lag=32, offset=103, timestamp=timestamp_a, observedAt=timestamp_a
                        ),
                    ],
                )
            ]
        },
    }
    return responses[consumer]


def _fake_get_consumers(cluster: str) -> list[str]:
    """Handles get_consumers for Burrow."""
    assert cluster == "local"
    return [
        "correctglobalprefix-correctprefix",
        "correctglobalprefix-correctprefix-historic",
        "notcorrectprefix-correctprefix",
        "correctglobalprefix-fakeprefix",
    ]


def _fake_get_plugin_prefix(deployment_key: str) -> Optional[str]:
    """Handles get_plugin_prefix for Redis."""
    if deployment_key == "plugin-test":
        return "correctprefix"
    else:
        return "fakeprefix"


async def _get_mocked_tracker() -> Tracker:
    tracker = Tracker()

    tracker.burrower.get_clusters = AsyncMock(return_value=["local"])
    tracker.burrower.get_consumers = AsyncMock(side_effect=_fake_get_consumers)
    tracker.burrower.get_consumer_detail = AsyncMock(side_effect=_fake_get_consumer_detail)

    tracker.consumer_group_matchers[ConsumerGroupType.Plugin].kv.get_plugin_prefix = AsyncMock(  # type: ignore
        side_effect=_fake_get_plugin_prefix
    )

    # We don't want to call start because its an infinite loop
    await tracker._determine_cluster()

    return tracker


async def test_plugin_registration():
    """Validates that plugins get registered in the tracker correctly."""
    tracker = await _get_mocked_tracker()

    assert len(tracker._pending_updates) == 0

    await tracker.register_watch(
        "plugin-test", PluginScalerMetadata(plugin_name="plugin-test", target_size=100, include_historic=False)
    )

    assert len(tracker._pending_updates) == 1

    # Run a tracker loop so it gets registered
    await tracker._tick()

    assert len(tracker._pending_updates) == 0
    assert "plugin-test" in tracker._tracked_watches


async def test_plugin_non_historic():
    """Tests that a plugin with an increasing backlog is correctly reported as falling behind."""

    tracker = await _get_mocked_tracker()
    await tracker.register_watch(
        "plugin-test", PluginScalerMetadata(plugin_name="plugin-test", target_size=100, include_historic=False)
    )
    await tracker._tick()

    # We should have a result:
    result = await tracker.get_watch_change("plugin-test")
    assert result is not None
    assert math.floor(result) == 3


async def test_plugin_historic():
    """Tests that a plugin with an increasing backlog is correctly reported as falling behind."""

    tracker = await _get_mocked_tracker()
    await tracker.register_watch(
        "plugin-test", PluginScalerMetadata(plugin_name="plugin-test", target_size=100, include_historic=True)
    )
    await tracker._tick()

    # We should have a result:
    result = await tracker.get_watch_change("plugin-test")
    # live + historic
    assert result is not None
    assert math.floor(result) == 6


async def test_plugin_lagging():
    """Tests that a plugin with an increasing backlog is correctly reported as falling behind."""

    tracker = await _get_mocked_tracker()
    await tracker.register_watch(
        "plugin-test", PluginScalerMetadata(plugin_name="plugin-test", target_size=100, include_historic=False)
    )
    await tracker._tick()

    tracker.burrower.get_consumer_detail = AsyncMock(side_effect=_fake_get_consumer_detail_lagging)
    await tracker._tick()

    # We should have a result:
    result = await tracker.get_watch_change("plugin-test")
    assert result is not None
    assert math.floor(result) == 15


async def test_plugin_unregistered():
    """Tests that a plugin that hasn't been registered behaves correctly."""

    tracker = await _get_mocked_tracker()

    # Plugin should be registered but won't give us any scaling decisions yet (as requires multiple
    # samples)
    result = await tracker.get_watch_change("plugin-test")
    assert result is None
