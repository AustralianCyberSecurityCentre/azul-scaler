"""A metric provider for KEDA, used to derive the performance of Azul components."""

import importlib.resources
import sys

# https://github.com/protocolbuffers/protobuf/issues/1491
sys.path.append(str(importlib.resources.files(None) / "grpc"))
