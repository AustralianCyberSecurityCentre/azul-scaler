# Azul Scaler

Dynamically scales Azul components based on system throughput.

THIS IS EXPERIMENTAL AND IS A WORK IN PROGRESS.

Do not enable this unless you know what you are doing, and are willing to watch your HPAs
constantly.

This matches Kafka consumer group names, provided by dispatcher in Redis and determines a metric
similar to an ETA for a given plugin to guide system scaling decisions.

## Architecture

The scaler utilises both a 'frontend' gRPC server to service requests from KEDA (the
custom metrics server utilised by Azul to provide scaling information to Kubernetes HPAs),
as well as a Tracker which periodically polls backend data sources (currently just Kafka) for
information on how well system components are performing.

The lifecycle of a plugin being scaled through this looks like the following:

```mermaid
graph TD
    Kubernetes --> KEDA;

    subgraph AzulScaler[Azul Scaler]
        subgraph Tracker
            registerWatch;

            subgraph Watcher[Watcher Thread]
                BurrowLookup[[Burrow lookup loop]];
            end

            registerWatch --Register new plugin--> BurrowLookup;

            StoredMetrics[(Stored Metrics)];
            BurrowLookup --Information about plugin--> StoredMetrics;

            getWatchChange;

            StoredMetrics --Information about plugin--> getWatchChange;
        end

        subgraph grpc[gRPC Server]
            isActive --Plugin name--> registerWatch;
            getMetric;
        end

    end

    KEDA --Periodic call (to determine if alive)--> isActive;
    getMetric --Periodic call (to fetch scaling numbers)--> KEDA;

    getWatchChange --Numerical lag value--> getMetric;
```

## Installation

```bash
pip install azul-scaler
```

To build the Protobuf protocol, run the following:

```bash
pip install grpcio-tools
mkdir -p azul_scaler/grpc/
python -m grpc_tools.protoc -I protos --python_out=azul_scaler/grpc --pyi_out=azul_scaler/grpc --grpc_python_out=azul_scaler/grpc protos/externalscaler.proto
```

## Usage

By default, launching the default entrypoint (defined in `pyproject.toml`) will start up a unencrypted
gRPC listener on port :8090. This can be configured via environmental variables listed in `settings.py`.

This can be used with KEDA directly - create a service for this scaler, and use KEDA's external
scaler with the scalerAddress pointed at the service (including the namespace for cross-namespace
connectivity).

## Model

The scaler has a series of parameters that need to be configured in order to use them.

These parameters differ between the entity type used (currently only 'plugin').

| KEY             | VALUE                                                             |
| --------------- | ----------------------------------------------------------------- |
| entityName      | 'plugin'                                                          |
| pluginName      | The name of the plugin deployment.                                |
| targetSize      | The maximum consumer lag before scaling (pass as string to KEDA). |
| includeHistoric | If the historical backlog should be included ('true' or 'false'). |

## Dependency management

Dependencies are managed in the pyproject.toml and debian.txt file.

Version pinning is achieved using the `uv.lock` file.
Because the `uv.lock` file is configured to use a private UV registry, external developers using UV will need to delete the existing `uv.lock` file and update the project configuration to point to the publicly available PyPI registry instead.

To add new dependencies it's recommended to use uv with the command `uv add <new-package>`
    or for a dev package `uv add --dev <new-dev-package>`

The tool used for linting and managing styling is `ruff` and it is configured via `pyproject.toml`

The debian.txt file manages the debian dependencies that need to be installed on development systems and docker images.

Sometimes the debian.txt file is insufficient and in this case the Dockerfile may need to be modified directly to
install complex dependencies.
