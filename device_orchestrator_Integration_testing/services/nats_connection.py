import os
from dotenv import load_dotenv
from nats.aio.client import Client as NATS
load_dotenv()


class NATSConnection:
    """Class to manage a connection to a NATS server."""

    def __init__(self):
        self.server_url = os.getenv("NATS_SERVER_URL", "nats://0.0.0.0:4222")
        self.nc = NATS()

    async def connect(self):
        """Establish a connection to the NATS server."""
        try:
            print(f"Connecting to NATS server at {self.server_url}...")

            await self.nc.connect(servers=[self.server_url])

            print(f"Successfully connected to {self.server_url}")

            return self.nc
        except Exception as e:
            print(f"Failed to connect to NATS: {e}")
            raise

    async def close(self):
        """Close the NATS connection."""
        if self.nc.is_connected:
            await self.nc.close()

            print("NATS connection closed.")
