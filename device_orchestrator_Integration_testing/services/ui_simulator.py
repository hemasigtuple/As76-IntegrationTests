import asyncio
import json

from services.nats_connection import NATSConnection
from services.config import (
    generate_orchestrator_command_next_on_place_tray_screen,
    generate_orchestrator_command_start_pre_processing_slide_data,
)


async def ui_inspector_listener():
    """Listens for UI responses, triggers start_pbs_job, and continues execution."""
    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    async def handle_ui_messages(msg):
        received_command = json.loads(msg.data.decode())
        print("Received UI command:", received_command)

        trigger_next_step = None

        if received_command.get("type") == "show_place_tray_dialog":
            print("Received 'show_place_tray_dialog' and Triggering next_on_place_tray.")
            trigger_next_step = generate_orchestrator_command_next_on_place_tray_screen(
                correlation_id=None, trace_id=received_command["metadata"]["trace_id"]
            )
        elif received_command.get("type") == "show_pbs_slides_details_entry_dialog":
            print("Received 'show_pbs_slides_details_entry_dialog' and Triggering Start Pre-processing Slide Data.")
            trigger_next_step = generate_orchestrator_command_start_pre_processing_slide_data(
                correlation_id=None, trace_id=received_command["metadata"]["trace_id"]
            )

        if trigger_next_step:
            await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_next_step).encode())
            print("Published a command to nila.device-job-orchestrator.commands")

    # Subscribe to UI responses
    await nats_client.subscribe("nila.console.commands", cb=handle_ui_messages)

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass

    await nats_client.close()


if __name__ == "__main__":
    asyncio.run(ui_inspector_listener())
