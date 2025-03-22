import uuid

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

@pytest.mark.asyncio
async def test_report_initialized_command_issued(nats_client):
    """Integration test for the Report Initialized command issued workflow with message verification."""

    received_message = []
    trace_id = str(uuid.uuid4())
    async def handle_cloud_bridge_commands(msg):
        received_command = json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_command}")
        if received_command["type"] == "report_initialized":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    logger.info("Subscribing to 'nila.cloud_bridge.commands...")
    sub = await nats_client.subscribe("nila.cloud_bridge.commands", cb=handle_cloud_bridge_commands)

    trigger_start_pbs_job =generate_orchestrator_command_start_pbs_job(correlation_id=None, trace_id=trace_id)

    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify Cloud Bridger received the correct command
    assert len(received_message) >= 1, "No status updates received from Cloud Bridge."
    assert received_message[0]["type"] == "report_initialized"

    # Ensure 'data' exists
    assert "data" in received_message[0], "'data' field is missing in received message."

    # Extract 'data'
    data = received_message[0]["data"]

    # Validate 'slot_id' (exists, is a string, numeric, and within range 1-6)
    assert "slot_id" in data and isinstance(data["slot_id"], str) and data["slot_id"].isdigit() and 1 <= int(
        data["slot_id"]) <= 6, \
        f"Invalid slot_id '{data.get('slot_id')}'. Expected range: 1 to 6"

    # Validate 'slide_id' (exists and is a non-empty string)
    assert "slide_id" in data and isinstance(data["slide_id"], str) and data["slide_id"].strip(), \
        f"'slide_id' is missing or empty"

    # Validate 'status' (exists and is a non-empty string)
    assert "status" in data and isinstance(data["status"], str) and data["status"].strip(), \
        f"'status' is missing or empty"

    # Validate Specific field: Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_ready_report_command_issued(nats_client):
    """Integration test for the Report Initialized command issued workflow with message verification."""

    received_message = []
    trace_id = str(uuid.uuid4())

    async def handle_cloud_bridge_commands(msg):
        received_command= json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_command}")
        if received_command["type"] == "ready_report":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    logger.info("Subscribing to ' nila.cloud_bridge.commands...")
    sub = await nats_client.subscribe(" nila.cloud_bridge.commands", cb=handle_cloud_bridge_commands)

    command = generate_orchestrator_command_start_pbs_job(correlation_id=None, trace_id=trace_id)

    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify Cloud Bridge received the correct command
    assert len(received_message) >= 1, "No status updates received from Cloud Bridge."
    assert received_message[0]["type"] == "ready_report"
    assert "data" in received_message[0], "'data' field is missing in received message"

    # Extract 'data' dictionary
    data = received_message[0]["data"]

    # Validate 'slot_id' (should be an integer and within range 1 to 6)
    assert "slot_id" in data and isinstance(data["slot_id"], int) and 1 <= data["slot_id"] <= 6, \
        f"Invalid 'slot_id': {data.get('slot_id')}"

    # Validate message content field by field
    expected_data = command["data"]
    received_data = received_message[0]["data"]

    #Check for Multiple report
    for expected_report, received_report in received_message:
        assert received_report["type"] == "report_ready"
        expected_data = expected_report["data"]
        received_data = received_report["data"]

    # Assert specific field: patient_name
    assert received_data["patient_name"] == expected_data[
        "patient_name"], f"Mismatch in 'patient_name': expected {expected_data['patient_name']}, got {received_data['patient_name']}"

    # Assert metadata field: trace_id
    assert received_message[0]["metadata"]["trace_id"] == command["metadata"]["trace_id"], \
        f"Expected trace ID to be '{command['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()
