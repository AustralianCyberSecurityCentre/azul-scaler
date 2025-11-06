"""Backend interface for determining metrics for scaling operations."""

import logging

from azul_scaler.grpc.externalscaler_pb2 import MetricSpec, ScaledObjectRef
from azul_scaler.tracker import ConsumerGroupType, Tracker
from azul_scaler.types import PluginScalerMetadata


class Scaler[T]:
    """Generic interface for scaling deployments in Azul."""

    async def startup(self):
        """Async startup for the scaler."""
        pass

    async def parse_scaler_metadata(self, _scaled_object: ScaledObjectRef) -> T:
        """Parses scaler metadata into an internal object."""
        raise NotImplementedError("Not implemented!")

    async def is_active(self, _metadata: T) -> bool:
        """Determines if a plugin is active (i.e has files waiting)."""
        raise NotImplementedError("Not implemented!")

    async def get_metric_spec(self, _metadata: T) -> list[MetricSpec]:
        """Determines what metrics can be determined from this scaler."""
        raise NotImplementedError("Not implemented!")

    async def get_metric(self, _metadata: T, metric_name: str) -> int:
        """Determines the value of a metric. May throw exceptions if needed."""
        raise NotImplementedError("Not implemented!")


class PluginScaler(Scaler[PluginScalerMetadata]):
    """Scaler interface for Azul plugins."""

    def __init__(self, tracker: Tracker, *kargs, **kwargs):
        super().__init__(*kargs, **kwargs)

        self.logger = logging.getLogger("plugin_scaler")
        self.tracker = tracker

    async def _register_plugin(self, metadata: PluginScalerMetadata):
        """Updates registration info for the given plugin."""
        return await self.tracker.register_watch(metadata.plugin_name, metadata)

    async def parse_scaler_metadata(self, scaled_object: ScaledObjectRef) -> PluginScalerMetadata:
        """Parses metadata passed via gRPC."""
        return PluginScalerMetadata(
            entity_type=ConsumerGroupType.Plugin,
            plugin_name=scaled_object.scalerMetadata["pluginName"],
            target_size=int(scaled_object.scalerMetadata["targetSize"]),
            include_historic=scaled_object.scalerMetadata["includeHistoric"].lower().strip() == "true",
        )

    async def is_active(self, metadata: PluginScalerMetadata) -> bool:
        """Determines if a plugin is active (i.e has files waiting)."""
        await self._register_plugin(metadata)
        # FUTURE: Enable scaling to 0 by setting this as False if nil files found
        return True

    async def get_metric_spec(self, metadata: PluginScalerMetadata) -> list[MetricSpec]:
        """Determines what metrics can be determined from this scaler."""
        return [MetricSpec(metricName="backlog", targetSize=metadata.target_size)]

    async def get_metric(self, metadata: PluginScalerMetadata, metric_name: str) -> int:
        """Determines the value of a metric. May throw exceptions if needed."""
        if metric_name != "backlog":
            self.logger.warning("Rejecting client as they have an invalid metricName %s", metric_name)
            raise NameError(name=metric_name)

        await self._register_plugin(metadata)

        rate_of_change = await self.tracker.get_watch_change(metadata.plugin_name)

        if rate_of_change is None:
            raise IndexError("No scaling info found.")

        return round(rate_of_change)
