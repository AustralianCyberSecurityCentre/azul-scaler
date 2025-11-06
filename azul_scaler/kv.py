"""Key/Value storage wrappers."""

from typing import Optional

import redis.asyncio as redis
from pydantic import BaseModel

from azul_scaler import settings


class DeployedPlugin(BaseModel):
    """A Redis object for deployed plugins."""

    kafka_prefix: str


class KVStorage:
    """Wrapper for Redis."""

    def __init__(self):
        if settings.settings.redis_username != "":
            username = settings.settings.redis_username
            password = settings.settings.redis_password
        else:
            username = None
            password = None

        url = settings.settings.redis_url
        if ":/" not in url:
            # Assume TCP if not specified
            url = "redis://%s" % url

        # 2 = deployed plugins
        self.redis = redis.from_url(
            url=url,
            username=username,
            password=password,
            db=2,
        )

    async def get_plugin_prefix(self, deployment_key: str) -> Optional[str]:
        """Fetches a plugin's prefix from Redis."""
        data = await self.redis.get(deployment_key)
        if data is not None:
            return DeployedPlugin.model_validate_json(data).kafka_prefix
        else:
            return None
