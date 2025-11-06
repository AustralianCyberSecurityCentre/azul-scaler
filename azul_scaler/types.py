"""Various shared types."""

from enum import IntEnum
from typing import Literal

from pydantic import BaseModel


class ConsumerGroupType(IntEnum):
    """What kind of consumer group this is."""

    # FUTURE: Add support for other kinds of workloads - e.g. ingestors.
    Plugin = 1


class PluginScalerMetadata(BaseModel):
    """Metadata identified by the ScaledObject in Kubernetes/KEDA for plugins."""

    # metadata identifier
    entity_type: Literal[ConsumerGroupType.Plugin] = ConsumerGroupType.Plugin
    # the name of the plugin's deployment - e.g. 'plugin-tika'
    plugin_name: str
    # dictates the desired number of files in the queue. If this is exceeded, the plugin
    # will scale up, and if there are less files then the plugin will scale down (to a minimum of 1).
    target_size: int
    # if the historical backlog should be included
    include_historic: bool
