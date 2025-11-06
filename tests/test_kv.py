"""Basic test for Redis wrapper."""

import json
from unittest.mock import AsyncMock

from azul_scaler.kv import KVStorage


async def test_get_plugin_prefix():
    storage = KVStorage()

    storage.redis.get = AsyncMock(return_value=json.dumps({"kafka_prefix": "abcd1234"}))

    res = await storage.get_plugin_prefix("test")

    assert res == "abcd1234"
