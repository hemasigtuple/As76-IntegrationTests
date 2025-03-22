import pytest
import json
import asyncio
from services.nats_connection import NATSConnection
from services.config import generate_orchestrator_command_start_pbs_job
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from utils.logger_config import get_logger

logger = get_logger(__name__)  # Module-specific logger

async def clean_nats_messages(nats_client):
    """Clears out stale messages before running a new test."""
    msg_drain = []

    async def drain_old_messages(msg):
        msg_drain.append(msg)

    temp_sub = await nats_client.subscribe("nila.console.commands", cb=drain_old_messages)
    await asyncio.sleep(2)  # Allow time for any old messages to arrive
    await temp_sub.unsubscribe()
    await nats_client.flush()
    logger.info(f"Cleared {len(msg_drain)} stale messages.")



@pytest.mark.asyncio
async def test_show_initiating_scan_page_command_issued():
    """Integration test for the show Scan  Initiating scan  commands issued workflow with message verification."""
    received_message = []

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()
    # **Clean stale messages before subscribing**
    await clean_nats_messages(nats_client)

    # Subscribe to UI behavior
    async def handle_console_commands(msg):

        received_command = json.loads(msg.data.decode())
        logger.info(f"The UI has received the command response : {received_command} \n")

        if received_command["type"] == "show_initiating_scan_page":
            logger.info(f"The UI Received command type is : {received_command['type']} \n")

            received_message.append(received_command)

    # Subscribe to the UI command topic
    logger.info("Subscribed to the nila.console.commands")
    subscription = await nats_client.subscribe("nila.console.commands", cb=handle_console_commands)

    trigger_start_pbs_job_commands = generate_orchestrator_command_start_pbs_job().copy()
    expected_command = trigger_start_pbs_job_commands

    logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {expected_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(expected_command).encode())

    # Ensure messages are flushed
    await nats_client.flush()

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(20)

    # Unsubscribe after waiting
    await subscription.unsubscribe(1)
    await nats_client.flush()  # Ensure all messages are processed before validation

    # Verify UI received the correct command
    logger.info(f"Verify the orchestrator issued command : {received_message}")

    # assert len(received_message) >= 1, "No status updates received from UI."
    assert received_message , f"NO response to the UI"
    assert received_message[0]["type"] == "show_initiating_scan_page"

    # Validate trace ID in metadata
    expected_trace_id = expected_command["metadata"]["trace_id"]
    received_trace_id = received_message[0]["metadata"].get("trace_id")

    assert received_trace_id == expected_trace_id, f"Expected trace ID '{expected_trace_id}', but got '{received_trace_id}'"
    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_show_scan_progress_page_command_issued():
    """Integration test for the show_scan_progress_page command workflow with message verification."""

    received_messages = []

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    # Handler function to process received messages
    async def handle_console_commands(msg):
        received_command = json.loads(msg.data.decode())
        logger.info(f"UI received message: {received_command}")

        if received_command.get("type") == "show_scan_progress_page":
            logger.info(f"Update received message: {received_command['type']}")
            received_messages.append(received_command)

    # Subscribe to the UI command topic
    logger.info("Subscribed to nila.console.commands")
    subscription = await nats_client.subscribe("nila.console.commands", cb=handle_console_commands)

    # Expected command being sent
    expected_command = ORCHESTRATOR_COMMAND_START_PBS_JOB.copy()

    # Publishing command
    logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {expected_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(expected_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(20)

    # Unsubscribe after waiting
    await subscription.unsubscribe(1)

    # Verify the message received
    logger.info(f"Verify the Received Messages: {received_messages}")
    # assert len(received_messages)>=1 ,"No response received from UI."
    assert received_messages, "NO response to the UI"

    for received_message in received_messages:
        assert received_message["type"] == "show_scan_progress_page", "Incorrect command type received."

        # Extract slot details
        slot_details = received_message["data"].get("slot_details", [])
        assert isinstance(slot_details, list) and slot_details, "slot_details should be a non-empty list."

        # Validate each slot entry
        for slot_data in slot_details:
            slot_id = slot_data.get("slot_id")
            assert isinstance(slot_id, int) and 1 <= slot_id <= 6, f"Invalid slot_id: {slot_id}"

            # Validate slot_image
            slot_image = slot_data.get("slot_image")
            assert isinstance(slot_image, dict) and "path" in slot_image and slot_image["path"], \
                f"Invalid slot_image: {slot_image}"

            # Validate smear_info
            smear_info = slot_data.get("smear_info")
            assert isinstance(smear_info, dict) and smear_info, f"Invalid smear_info: {smear_info}"
            assert smear_info == expected_command["data"]["smear_info"], \
                f"Mismatch in smear_info. Received: {smear_info}, Expected: {expected_command['data']['smear_info']}"

            # Validate slot_scan_status
            slot_scan_status = slot_data.get("slot_scan_status")
            assert isinstance(slot_scan_status, dict) and "scanning" in slot_scan_status and \
                   isinstance(slot_scan_status["scanning"], str), f"Invalid slot_scan_status: {slot_scan_status}"

        # Validate trace ID in metadata
        expected_trace_id = expected_command["metadata"]["trace_id"]
        received_trace_id = received_message["metadata"].get("trace_id")
        assert received_trace_id == expected_trace_id, \
            f"Expected trace ID '{expected_trace_id}', but got '{received_trace_id}'"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_show_pbs_slot_scan_started_info_issued_command():
    """Integration test for the Show PBS Slot Started Info command issued workflow with message verification."""

    received_message = []

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()


    async def handle_console_commands(msg):
        received_command = json.loads(msg.data.decode())

        logger.info(f"Console received message: {received_command}")
        if received_command["type"] == "show_pbs_slot_scan_started_info":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    logger.info("Subscribing to 'nila.console.commands...")
    sub = await nats_client.subscribe("nila.console.commands", cb=handle_console_commands)

    start_pbs_job_command = ORCHESTRATOR_COMMAND_START_PBS_JOB

    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")
    # Verify UI received the correct command
    assert len(received_message) >= 1, "No status updates received from UI."
    assert received_message[0]["type"] == "show_pbs_slot_scan_started_info"

    # Ensure 'data' exists in received_message
    assert "data" in received_message[0], "'data' field is missing in received message"

    # Extract 'data' dictionary
    data = received_message[0]["data"]

    # Ensure 'slot_id' is a string containing a number within the range 1-6
    assert isinstance(data.get("slot_id"), str) and data["slot_id"].isdigit() and 1 <= int(data["slot_id"]) <= 6, \
        f"slot_id '{data.get('slot_id')}' is out of expected range (1-6)"

    # Ensure 'aoi_count' is a positive integer
    assert isinstance(data.get("aoi_count"), int) and data["aoi_count"] > 0, \
        f"aoi_count '{data.get('aoi_count')}' is invalid (must be a positive integer)"

    # Ensure 'slide_id' is a non-empty string that starts with "SIG"
    assert isinstance(data.get("slide_id"), str) and data["slide_id"].startswith("SIG"), \
        f"slide_id '{data.get('slide_id')}' is invalid (must start with 'SIG')"

    # Ensure 'monolayer_status' is a valid string and not empty
    assert isinstance(data.get("monolayer_status"), str) and data["monolayer_status"].strip(), \
        f"monolayer_status '{data.get('monolayer_status')}' is empty or invalid"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_show_monolayer_started_info_issued_command():
    """Integration test for the Show Monolayer Started Info command issued workflow with message verification."""

    received_message = []

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    async def handle_console_commands(msg):
        received_command = json.loads(msg.data.decode())

        logger.info(f"Console received message: {received_command}")
        if received_command["type"] == "":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    logger.info("Subscribing to 'nila.console.commands...")
    sub = await nats_client.subscribe("nila.console.commands", cb=handle_console_commands)

    trigger_start_pbs_job_command = ORCHESTRATOR_COMMAND_START_PBS_JOB

    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify UI received the correct command
    assert len(received_message) >= 1, "No status updates received from UI."
    assert received_message[0]["type"] == "show_monolayer_started_info"

    # Ensure 'data' exists in received_message
    assert "data" in received_message[0], "'data' field is missing in received message"

    # Extract 'data' dictionary
    data = received_message[0]["data"]

    # Ensure 'slide_id' is a non-empty string that starts with "SIG"
    assert isinstance(data.get("slide_id"), str) and data["slide_id"].startswith("SIG"), \
        f"slide_id '{data.get('slide_id')}' is invalid (must start with 'SIG')"

    # Ensure 'slot_id' is a string containing a number within the range 1-6
    assert isinstance(data.get("slot_id"), str) and data["slot_id"].isdigit() and 1 <= int(data["slot_id"]) <= 6, \
        f"slot_id '{data.get('slot_id')}' is out of expected range (1-6)"

    # Ensure 'procedure_step_label' is a non-empty string
    assert isinstance(data.get("procedure_step_label"), str) and data["procedure_step_label"].strip(), \
        f"procedure_step_label '{data.get('procedure_step_label')}' is empty or invalid"

    # Ensure 'workitem_state' is a valid non-empty string
    assert isinstance(data.get("workitem_state"), str) and data["workitem_state"].strip(), \
        f"workitem_state '{data.get('workitem_state')}' is empty or invalid"

    # Ensure 'response_status' is a string representing a numeric HTTP status (e.g., "200")
    assert isinstance(data.get("response_status"), str) and data["response_status"].isdigit(), \
        f"response_status '{data.get('response_status')}' is invalid (must be a numeric string like '200')"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_show_pbs_slot_scan_status_command():
    """Integration test for the Show PBS Slot Scan command issued workflow with message verification."""

    received_message = []

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    async def handle_console_commands(msg):
        received_command = json.loads(msg.data.decode())

        logger.info(f"Console received message: {received_command}")
        if received_command["type"] == "show_pbs_slot_scan_status":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    logger.info("Subscribing to 'nila.console.commands...")
    sub = await nats_client.subscribe("nila.console.commands", cb=handle_console_commands)

    trigger_start_pbs_job_command = ORCHESTRATOR_COMMAND_START_PBS_JOB

    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify UI received the correct command
    assert len(received_message) >= 1, "No status updates received from UI."
    assert received_message[0]["type"] == "show_pbs_slot_scan_status"

    # Ensure 'data' exists in received_message
    assert "data" in received_message[0], "'data' field is missing in received message"

    # Extract 'data' dictionary
    data = received_message[0]["data"]

    # Ensure 'slot_id' is a string containing a number within the range 1-6
    assert isinstance(data.get("slot_id"), str) and data["slot_id"].isdigit() and 1 <= int(data["slot_id"]) <= 6, \
        f"slot_id '{data.get('slot_id')}' is out of expected range (1-6) or invalid"

    # Ensure 'scan_status' is a non-empty string
    assert isinstance(data.get("scan_status"), str) and data["scan_status"].strip(), \
        f"scan_status '{data.get('scan_status')}' is empty or invalid"

    # Ensure 'slide_id' is a non-empty string
    assert isinstance(data.get("slide_id"), str) and data["slide_id"].strip(), \
        f"slide_id '{data.get('slide_id')}' is empty or invalid"

    # Ensure 'status' is a non-empty string
    assert isinstance(data.get("status"), str) and data["status"].strip(), \
        f"status '{data.get('status')}' is empty or invalid"

    # Ensure 'inclusion' is a non-empty string
    assert isinstance(data.get("inclusion"), str) and data["inclusion"].strip(), \
        f"inclusion '{data.get('inclusion')}' is empty or invalid"
    # Validate Specific field: Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job_command["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job_command['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_show_pbs_main_scan_summary_info_command():
    """Integration test for the Show PBS main Scan Summary Info command issued workflow with message verification."""

    received_message = []

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    async def handle_console_commands(msg):
        received_command = json.loads(msg.data.decode())

        logger.info(f"Console received message: {received_command}")
        if received_command["type"] == "show_pbs_main_scan_summary_info":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    logger.info("Subscribing to 'nila.console.commands...")
    sub = await nats_client.subscribe("nila.console.commands", cb=handle_console_commands)

    trigger_start_pbs_job_command = ORCHESTRATOR_COMMAND_START_PBS_JOB

    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify UI received the correct command
    assert len(received_message) >= 1, "No status updates received from UI."
    assert received_message[0]["type"] == "show_pbs_main_scan_summary_info"

    # Expected scan statuses
    valid_scan_statuses = {
        "Barcode error",
        "Empty slot",
        "Scan completed",
        "Scan field",
        "Scan aborted",
        "Scan cancelled",
        "Generating report",
        "Report failed",
        "Report generated"
    }

    # Ensure 'data' exists in received_message
    assert "data" in received_message[0], "'data' field is missing in received message"

    # Extract 'data' dictionary
    data = received_message[0]["data"]

    # Ensure 'slot_details' exists and is a list
    assert "slot_details" in data and isinstance(data["slot_details"], list), \
        "'slot_details' is missing or not a list"

    # Validate each slot in 'slot_details'
    for slot in data["slot_details"]:
        assert isinstance(slot, dict), "Each entry in 'slot_details' should be a dictionary"

        # Ensure 'slot_id' is a string containing a number within the range 1-6
        assert "slot_id" in slot and isinstance(slot["slot_id"], str) and slot["slot_id"].isdigit() and 1 <= int(
            slot["slot_id"]) <= 6, \
            f"slot_id '{slot.get('slot_id')}' is out of expected range (1-6) or invalid"

        # Ensure 'slide_id' is a string
        assert "slide_id" in slot and isinstance(slot["slide_id"], str), \
            f"slide_id '{slot.get('slide_id')}' is missing or invalid"

        # # Ensure 'slide_id' is a non-empty string that starts with "SIG"
        # assert isinstance(slot["slide_id"], str) and data["slide_id"].startswith("SIG"), \
        #     f"slide_id '{slot.get('slide_id')}' is invalid (must start with 'SIG')"

        # Ensure 'scan_status' exists and is a valid value
        assert "scan_status" in slot and isinstance(slot["scan_status"], str) and slot[
            "scan_status"] in valid_scan_statuses, \
            f"Invalid scan_status '{slot.get('scan_status')}'. Expected one of: {valid_scan_statuses}"

    # Ensure 'tray_status' exists and contains 'scan_status'
    assert "tray_status" in data and isinstance(data["tray_status"], dict), \
        "'tray_status' is missing or not a dictionary"
    assert "scan_status" in data["tray_status"] and isinstance(data["tray_status"]["scan_status"], str), \
        "'tray_status' does not contain a valid 'scan_status'"

    # Validate Specific field: Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job_command["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job_command['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_show_tray_ejected_message_command():
    """Integration test for the Show Tray Ejected Message command issued workflow with message verification."""

    received_message = []

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    async def handle_show_tray_ejected_message(msg):
        received_command = json.loads(msg.data.decode())

        logger.info(f"Console received message: {received_command}")
        if received_command["type"] == "show_tray_ejected_message":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    logger.info("Subscribing to 'nila.console.commands...")
    sub = await nats_client.subscribe("nila.console.commands", cb=handle_show_tray_ejected_message)

    trigger_start_pbs_job_command = ORCHESTRATOR_COMMAND_START_PBS_JOB

    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify UI received the correct command
    assert len(received_message) >= 1, "No status updates received from UI."
    assert received_message[0]["type"] == "show_tray_ejected_message"

    # Ensure 'data' exists in received_message
    assert "data" in received_message[0], "'data' field is missing in received message"

    # Validate Specific field: Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job_command["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job_command['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()






