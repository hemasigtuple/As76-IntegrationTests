import asyncio
import json
import os
from dotenv import load_dotenv
from services.nats_connection import NATSConnection
from services.config import (
    generate_orchestrator_command_start_pre_processing_slide_data,
    generate_orachestrator_scan_pbs_aoi_scan_started
)

# Load environment variables
load_dotenv()

async def handler_scan_pbs_pbs_aoi_scan():
    """Listens for scan messages, triggers start_pbs_job, and continues execution."""
    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()
    estimated_total_fov_count = None
    received_fov_count = 0  # Counter to track FOV messages

    async def handle_scan_messages(msg):
        nonlocal estimated_total_fov_count, received_fov_count
        received_command = json.loads(msg.data.decode())
        print("Received message:", received_command)

        if received_command.get("type") == "pbs_aoi_scan_started":
            print(f"Received type: 'pbs_aoi_scan_started'")
            estimated_total_fov_count = received_command["data"].get("estimated_total_fov_count")

            # Subscribe to listen for FOV messages
            await nats_client.subscribe("nila.garuda.recon.command", cb=handle_scan_messages)

        elif received_command.get("type") == "reconstruct_pbs_fov":
            received_fov_count += 1
            print(f"Processing FOV {received_fov_count}/{estimated_total_fov_count}")

            # Check if all FOVs have been received
            if received_fov_count == estimated_total_fov_count:
                print("All FOVs received. Triggering next step...")

                trigger_next_step = generate_orchestrator_command_start_pre_processing_slide_data(
                    correlation_id=None, trace_id=received_command["metadata"]["trace_id"]
                )

                await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_next_step).encode())
                print("Published Start Pre-Processing Slide Data command to nila.device-job-orchestrator.commands")

    # Subscribe to the initial scan event
    await nats_client.subscribe("nila.garuda.scanner.event.>", cb=handle_scan_messages)

    try:
        while True:
            await asyncio.sleep(1)  # Keep the listener running
    except KeyboardInterrupt:
        pass

    await nats_client.close()


if __name__ == "__main__":
    asyncio.run(handler_scan_pbs_pbs_aoi_scan())
