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
async def test_start_pbs_slot_analysis_command_issued(nats_client):
    """Integration test for the Start PBS  Slot Analysis  command issued workflow with message verification."""

    received_message = []
    trace_id = str(uuid.uuid4())

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    async def handle_analyser_commands(msg):
        received_event = json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_event}")
        if received_event["type"] == "start_pbs_slot_analysis":
            logger.info(f"Received message type: {received_event['type']}")
            received_message.append(received_event)

    logger.info(f"Subscribing to 'nila.garuda.analyser.command...")
    sub = await nats_client.subscribe("nila.garuda.analyser.command", cb=handle_analyser_commands)

    trigger_start_pbs_job_command = generate_orchestrator_command_start_pbs_job(correlation_id=None, trace_id=trace_id)

    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info(f"Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify Analyser received the correct command
    assert received_message, f"No status updates received from Analyser."
    assert received_message[0]["type"] == "start_pbs_slot_analysis"

    # Ensure 'data' exists
    assert "data" in received_message[0], "'data' field is missing in received message"

    # Extract 'data'
    data = received_message[0]["data"]

    # Validate 'slot_id'
    assert "slot_id" in data and isinstance(data["slot_id"], str) and data["slot_id"].isdigit() and 1 <= int(
        data["slot_id"]) <= 6, \
        f"Invalid slot_id '{data.get('slot_id')}'. Expected range: 1 to 6"

    # Validate 'aoi_id'
    assert "aoi_id" in data and isinstance(data["aoi_id"], str) and data["aoi_id"].isdigit(), \
        f"Invalid aoi_id '{data.get('aoi_id')}'. Expected a numeric string"

    # Ensure 'aoi_image' exists and is a dictionary
    assert "aoi_image" in data and isinstance(data["aoi_image"], dict), "'aoi_image' field is missing or invalid"

    # Validate 'path' inside 'aoi_image' (exists and is not empty)
    assert "path" in data["aoi_image"] and isinstance(data["aoi_image"]["path"], str) and data["aoi_image"][
        "path"].strip(), \
        f"'path' is missing or empty in 'aoi_image'"

    # Validate 'recon_purge_folder' inside 'aoi_image' (exists and is not empty)
    assert "recon_purge_folder" in data["aoi_image"] and isinstance(data["aoi_image"]["recon_purge_folder"], str) and \
           data["aoi_image"]["recon_purge_folder"].strip(), \
        f"'recon_purge_folder' is missing or empty in 'aoi_image'"

    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job_command["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job_command['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_pbs_slot_analysis_completed_event_issued(nats_client):
    """Integration test for the  PBS  Slot Analysis Completed event issued workflow with message verification."""

    received_message = []
    trace_id = str(uuid.uuid4())

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    async def handle_analyser_events(msg):
        received_event = json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_event}")
        if received_event["type"] == "pbs_slot_analysis_completed":
            logger.info(f"Received message type: {received_event['type']}")
            received_message.append(received_event)

    logger.info(f"Subscribing to nila.garuda.analyser.event...")
    sub = await nats_client.subscribe("nila.garuda.analyser.event", cb=handle_analyser_events)

    start_pbs_job_command = generate_orchestrator_command_start_pbs_job(correlation_id=None, trace_id=trace_id)

    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info(f"Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify Analyser received event the correct command
    assert received_message, f"No status updates received from Analyser."
    assert received_message[0]["type"] == "pbs_slot_analysis_completed"

    # Ensure 'data' exists
    assert "data" in received_message[0], f"'data' field is missing in received message"

    # Extract 'data'
    data = received_message[0]["data"]

    # Validate 'slot_id' (exists, is a string, numeric, and within range 1-6)
    assert "slot_id" in data and isinstance(data["slot_id"], str) and data["slot_id"].isdigit() and 1 <= int(
        data["slot_id"]) <= 6, \
        f"Invalid slot_id '{data.get('slot_id')}'. Expected range: 1 to 6"

    # Validate 'aoi_id' (exists, is a string, and numeric)
    assert "aoi_id" in data and isinstance(data["aoi_id"], str) and data["aoi_id"].isdigit(), \
        f"Invalid aoi_id '{data.get('aoi_id')}'"

    # Validate 'analysis_status' (exists and is a string)
    assert "analysis_status" in data and isinstance(data["analysis_status"], str) and data["analysis_status"].strip(), \
        f"'analysis_status' is missing or empty"

    # Validate 'analyser_purge_folder' (exists and is not empty)
    assert "analyser_purge_folder" in data and isinstance(data["analyser_purge_folder"], str) and data[
        "analyser_purge_folder"].strip(), \
        f"'analyser_purge_folder' is missing or empty"
    # Assert specific: field Trace ID
    assert received_message[0]["metadata"]["trace_id"] == start_pbs_job_command["metadata"]["trace_id"], \
        f"Expected trace ID to be '{start_pbs_job_command['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

