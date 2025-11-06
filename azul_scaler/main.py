"""Dynamically scales Azul components based on system throughput."""

import asyncio
import logging
import logging.config
from typing import Optional

import grpc.aio
import grpc_channelz.v1.channelz

from azul_scaler import settings
from azul_scaler.grpc import externalscaler_pb2_grpc
from azul_scaler.server import ExternalScalerServicer


class ExternalScalerServer:
    """Managing class for the external scaler gRPC server."""

    server: Optional[grpc.aio.Server]

    def __init__(self):
        self.server = None

    async def setup(self):
        """Configures the server for running."""
        if self.server is not None:
            raise RuntimeError("Attempt to call setup() twice!")

        logging.basicConfig(
            level=logging.DEBUG if settings.settings.debug else logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
            handlers=[logging.StreamHandler()],
        )

        # Filter HTTPX noise
        logging.getLogger("httpx").setLevel(logging.WARN)

        self.server = grpc.aio.server()

        self.service = ExternalScalerServicer()
        await self.service.startup()

        externalscaler_pb2_grpc.add_ExternalScalerServicer_to_server(self.service, self.server)
        if settings.settings.debug:
            grpc_channelz.v1.channelz.add_channelz_servicer(self.server)

        self.server.add_insecure_port("[::]:%d" % settings.settings.port)

    async def start(self):
        """Starts indefinitely listening for connections."""
        if self.server is None:
            raise RuntimeError("Attempt to call start() before setup()!")

        logging.info("Binding to :%d..." % settings.settings.port)
        await self.server.start()
        logging.info("Ready for requests!")
        await self.server.wait_for_termination()

    async def stop(self):
        """Safely terminates the gRPC server."""
        if self.server is None:
            raise RuntimeError("Attempt to call stop() before setup()!")

        logging.info("Starting graceful shutdown...")
        # Shuts down the server with 5 seconds of grace period. During the
        # grace period, the server won't accept new connections and allow
        # existing RPCs to continue within the grace period.
        await self.server.stop(5)
        await self.service.stop()


def main():
    """Starts the gRPC server from a sync context."""
    server = ExternalScalerServer()

    # We need to stand up our own loop in order to be able to run
    # the shutdown function afterwards regardless of termination reasons,
    # as otherwise gRPC will indefinitely hang
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(server.setup())

        try:
            loop.run_until_complete(server.start())
        except KeyboardInterrupt:
            # A Ctrl-C doesn't need a stack trace
            pass
        finally:
            loop.run_until_complete(server.stop())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
