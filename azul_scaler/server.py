"""Implements the GRPC contract."""

import asyncio
import logging
from typing import AsyncGenerator, Optional

import grpc

from azul_scaler.grpc import externalscaler_pb2_grpc
from azul_scaler.grpc.externalscaler_pb2 import (
    GetMetricSpecResponse,
    GetMetricsRequest,
    GetMetricsResponse,
    IsActiveResponse,
    MetricValue,
    ScaledObjectRef,
)
from azul_scaler.scaler import PluginScaler
from azul_scaler.tracker import Tracker


class ExternalScalerServicer(externalscaler_pb2_grpc.ExternalScalerServicer):
    """Implementation of a KEDA external scaler, analysing Burrow metrics for plugins or other deployments."""

    def __init__(self, *kargs, **kwargs):
        super().__init__(*kargs, **kwargs)

        self.logger = logging.getLogger("scaler_servicer")
        self.tracker = Tracker()
        self.scalers = {"plugin": PluginScaler(self.tracker)}

    async def startup(self):
        """Starts up scalers."""
        for scaler in self.scalers.values():
            await scaler.startup()
            self.logger.info("Started scaler: %s", scaler.__class__.__name__)
        self._tracker_task = asyncio.create_task(self.tracker.start())

    async def stop(self):
        """Shuts down workloads."""
        self._tracker_task.cancel()

    def _handle_invalid_entity_type(self, entity_type: str, context: grpc.ServicerContext):
        """Handles invalid entity types."""
        self.logger.warning("Rejecting client as they have an invalid entityType %s", entity_type)
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
        context.set_details("Invalid entity type!")
        return None

    async def IsActive(self, request: ScaledObjectRef, context: grpc.ServicerContext) -> Optional[IsActiveResponse]:
        """One-off requests to determine if something is active."""
        entity_type = request.scalerMetadata["entityType"]

        scaler = self.scalers.get(entity_type)
        if scaler is None:
            return self._handle_invalid_entity_type(entity_type, context)

        metadata = await scaler.parse_scaler_metadata(request)

        result = await scaler.is_active(metadata)
        self.logger.debug("IsActive(): %s, %b", metadata, result)

        return IsActiveResponse(result=result)

    async def StreamIsActive(
        self, request: ScaledObjectRef, context: grpc.ServicerContext
    ) -> AsyncGenerator[IsActiveResponse, None] | None:
        """Streaming requests for a particular HPA to determine if something is active."""
        entity_type = request.scalerMetadata["entityType"]

        scaler = self.scalers.get(entity_type)
        if scaler is None:
            return self._handle_invalid_entity_type(entity_type, context)

        metadata = await scaler.parse_scaler_metadata(request)

        while True:
            # This repeats infinitely until the client disconnects
            result = await scaler.is_active(metadata)
            self.logger.debug("StreamIsActive(): %s, %b", metadata, result)

            # We only need to submit these semi-frequently
            await asyncio.sleep(10)

    async def GetMetricSpec(
        self, request: ScaledObjectRef, context: grpc.ServicerContext
    ) -> Optional[GetMetricSpecResponse]:
        """Determines a list of metrics that can be used by a HPA."""
        entity_type = request.scalerMetadata["entityType"]

        scaler = self.scalers.get(entity_type)
        if scaler is None:
            return self._handle_invalid_entity_type(entity_type, context)

        metadata = await scaler.parse_scaler_metadata(request)

        result = await scaler.get_metric_spec(metadata)
        self.logger.debug("GetMetricSpec(): %s, %s", metadata, result)

        return GetMetricSpecResponse(metricSpecs=result)

    async def GetMetrics(
        self, request: GetMetricsRequest, context: grpc.ServicerContext
    ) -> Optional[GetMetricsResponse]:
        """Fulfills metrics for a metric spec request."""
        entity_type = request.scaledObjectRef.scalerMetadata["entityType"]

        scaler = self.scalers.get(entity_type)
        if scaler is None:
            return self._handle_invalid_entity_type(entity_type, context)

        metadata = await scaler.parse_scaler_metadata(request.scaledObjectRef)

        try:
            result = await scaler.get_metric(metadata, request.metricName)
        except Exception as e:
            self.logger.warning("GetMetric(name=%s) failed: %s", request.scaledObjectRef.name, repr(e))
            context.set_code(grpc.StatusCode.DATA_LOSS)
            context.set_details(repr(e))
            return None

        self.logger.debug("GetMetric(): %s, %d", metadata, result)

        return GetMetricsResponse(metricValues=[MetricValue(metricName=request.metricName, metricValue=result)])
