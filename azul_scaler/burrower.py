"""Pokes Burrow for information."""

from typing import Annotated, Literal, Optional, Type, TypeVar, Union

import httpx
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from azul_scaler import settings


class BurrowSuccessResponse(BaseModel):
    """Underlying common model for all successful Burrow responses."""

    error: Literal[False]
    message: str


class BurrowErrorResponse(BaseModel):
    """Generic model for failing Burrow responses."""

    error: Literal[True]
    message: str


class BurrowListClustersResponse(BurrowSuccessResponse):
    """Response to /v1/clusters."""

    clusters: list[str]


class BurrowListConsumersResponse(BurrowSuccessResponse):
    """Response to /v1/clusters/{cluster}/consumer."""

    consumers: list[str]


class ConsumerTopicPartitionDetail(BaseModel):
    """Details on a consumer groups progress in a specific topic partition."""

    lag: int
    offset: int
    timestamp: int
    observedAt: int


class ConsumerTopicDetail(BaseModel):
    """Details on a consumer groups progress in a given topic."""

    model_config = ConfigDict(populate_by_name=True)

    current_lag: int = Field(validation_alias="current-lag", serialization_alias="current-lag")
    offsets: list[Optional[ConsumerTopicPartitionDetail]]


class BurrowListConsumerDetailResponse(BurrowSuccessResponse):
    """Response to /v1/clusters/{cluster}/consumer/{consumer}."""

    topics: dict[str, list[ConsumerTopicDetail]]


# A 'generic' that extends BurrowSuccessResponse
T = TypeVar("T", bound=BurrowSuccessResponse)


class Burrower:
    """Wrapper for the Burrow API."""

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30)

    async def _make_request(self, endpoint: str, format: Type[T]) -> T:
        """Makes a HTTP request to Burrow, validating the Burrow response before returning."""
        base_url = httpx.URL(settings.settings.burrow_url)
        request_url = base_url.join(endpoint)

        result = await self.client.get(request_url)

        # Discriminate between an error object (which is missing fields) and the actual
        # type we are looking for
        adapter = TypeAdapter(Annotated[Union[BurrowErrorResponse, format], Field(..., discriminator="error")])

        parsed_model = adapter.validate_json(result.content)

        if isinstance(parsed_model, BurrowErrorResponse):
            raise Exception("Burrow Error", parsed_model.message)

        return parsed_model

    async def get_clusters(self) -> list[str]:
        """Returns a list of Kafka clusters available."""
        resp = await self._make_request("/v3/kafka", BurrowListClustersResponse)
        return resp.clusters

    async def get_consumers(self, cluster: str) -> list[str]:
        """Returns a list of consumer groups available."""
        resp = await self._make_request("/v3/kafka/%s/consumer" % cluster, BurrowListConsumersResponse)
        return resp.consumers

    async def get_consumer_detail(self, cluster: str, consumer: str) -> dict[str, list[ConsumerTopicDetail]]:
        """Returns details on a consumer group."""
        resp = await self._make_request(
            "/v3/kafka/%s/consumer/%s" % (cluster, consumer), BurrowListConsumerDetailResponse
        )
        return resp.topics
