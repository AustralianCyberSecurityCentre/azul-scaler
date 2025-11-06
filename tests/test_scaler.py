"""Tests deployment scalers."""

from unittest.mock import AsyncMock

import pytest

from azul_scaler import settings
from azul_scaler.grpc.externalscaler_pb2 import ScaledObjectRef
from azul_scaler.scaler import PluginScaler
from azul_scaler.tracker import Tracker
from azul_scaler.types import ConsumerGroupType


def _fake_register_consumer_group(name, metadata, fake_store):
    """Mocks registering a new set of consumer group watcher."""
    fake_store[name] = metadata


def _fake_get_consumer_group_change(name, fake_store):
    """Mocks measuring rates of change."""
    if name not in fake_store:
        raise Exception("Expected registration!")

    if name == "plugin-test":
        if fake_store[name].include_historic:
            # many events in expedite + live + historic
            return 4.0
        else:
            # less events in expedite + live
            return 2.0
    else:
        # some unrelated plugin
        return 0.0


async def _get_mocked_scaler() -> PluginScaler:
    """Builds a scaler with mocked network calls."""
    settings.settings.topic_prefix = "correctglobalprefix"

    tracker = Tracker()
    tracker.start = AsyncMock(return_value=None)

    valid_names = {}

    tracker.register_watch = AsyncMock(
        side_effect=lambda name, metadata: _fake_register_consumer_group(name, metadata, valid_names)
    )
    tracker.get_watch_change = AsyncMock(side_effect=lambda name: _fake_get_consumer_group_change(name, valid_names))

    scaler = PluginScaler(tracker)

    await scaler.startup()

    return scaler


def _get_fake_metadata(include_historic: bool = False) -> ScaledObjectRef:
    """Generates fake KEDA metadata."""
    return ScaledObjectRef(
        name="plugin-test",
        namespace="azul-test-namespace",
        scalerMetadata={
            "entityType": "plugin",
            "pluginName": "plugin-test",
            "targetSize": "1234",
            "includeHistoric": "true" if include_historic else "false",
        },
    )


async def test_metadata_parsing():
    """Validates that metadata from KEDA gets parsed correctly."""
    scaler = await _get_mocked_scaler()

    metadata = await scaler.parse_scaler_metadata(_get_fake_metadata(include_historic=True))

    assert metadata.entity_type == ConsumerGroupType.Plugin
    assert metadata.plugin_name == "plugin-test"
    assert metadata.target_size == 1234
    assert metadata.include_historic == True

    metadata = await scaler.parse_scaler_metadata(_get_fake_metadata(include_historic=False))

    assert metadata.entity_type == ConsumerGroupType.Plugin
    assert metadata.plugin_name == "plugin-test"
    assert metadata.target_size == 1234
    assert metadata.include_historic == False


async def test_is_active():
    """Validates that a plugin can be activated."""
    scaler = await _get_mocked_scaler()
    metadata = await scaler.parse_scaler_metadata(_get_fake_metadata())
    res = await scaler.is_active(metadata)
    assert res


async def test_get_metric_spec():
    """Validates that metric specs are reported correctly."""
    scaler = await _get_mocked_scaler()
    metadata = await scaler.parse_scaler_metadata(_get_fake_metadata())
    res = await scaler.get_metric_spec(metadata)
    assert len(res) == 1
    assert res[0].metricName == "backlog"
    assert res[0].targetSize == 1234


async def test_get_metric_non_historic():
    """Validates that metrics are reported correctly for non historic cases."""
    scaler = await _get_mocked_scaler()
    metadata = await scaler.parse_scaler_metadata(_get_fake_metadata())
    res = await scaler.get_metric(metadata, "backlog")
    assert res == 2


async def test_get_metric_historic():
    """Validates that metrics are reported correctly for historic cases."""
    scaler = await _get_mocked_scaler()
    metadata = await scaler.parse_scaler_metadata(_get_fake_metadata(include_historic=True))
    print(metadata)
    res = await scaler.get_metric(metadata, "backlog")
    assert res == 4


async def test_get_invalid_metric():
    """Validates that invalid metrics do not get responses."""
    scaler = await _get_mocked_scaler()
    metadata = await scaler.parse_scaler_metadata(_get_fake_metadata(include_historic=True))

    with pytest.raises(NameError):
        await scaler.get_metric(metadata, "notbacklog")
