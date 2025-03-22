import asyncio
import json

from services.config import generate_orchestrator_command_start_macro_scan, \
    generate_orchestrator_command_pbs_start_slot_scan
from services.nats_connection import NATSConnection

async  def scanner_inspector_listener():
    """Listens for Scanner responses, triggers stat_pbs_job, and continues execution."""
    # NATS connection
    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()


    async def handle_scanner_messages(msg):
        received_command = json.loads(msg.data.decode())
        print(f"Received scanner  command: {received_command}")

        trigger_next_step_scan = None

        if received_command.get("type") == "scan_status":
            print(f"Received scan status command and trigger start_macro_scan command")

            trigger_next_step_scan = generate_orchestrator_command_start_macro_scan(
                correlation_id=None, trace_id=received_command["metadata"]["trace_id"]
            )

        elif received_command.get("type") == "show_scan_progress_page":
            print(f"Received show_scan_progress_page command and trigger pbs_start_slot_scan command")

            trigger_next_step_scan = generate_orchestrator_command_pbs_start_slot_scan(
                correlation_id=None, trace_id=received_command["metadata"]["trace_id"]
            )

        if trigger_next_step_scan:
            await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_next_step_scan).encode())
            print("Published next step scan command to nila.device-job-orchestrator.commands")

        # Subscribe to UI responses
    await nats_client.subscribe("nila.console.commands", cb=handle_scanner_messages)

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass

    await nats_client.close()


if __name__ == "__main__":
    asyncio.run(scanner_inspector_listener())




