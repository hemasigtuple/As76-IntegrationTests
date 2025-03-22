import pytest
import json
import asyncio
from services.nats_connection import NATSConnection
from services.config import ORCHESTRATOR_COMMAND_START_PBS_JOB
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
from utils.logger_config import get_logger

logger = get_logger(__name__)  # Module-specific logger
@pytest.mark.asyncio
async def test_start_pbs_reconstruction_issued_command_issued():
    """Integration test for the Start PBS Reconstructor command issued workflow with message verification."""

    received_message = []

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    async def handle_reconstructor_commands(msg):
        received_event = json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_event}")
        if received_event["type"] == "start_pbs_reconstruction":
            logger.info("update received message", received_event["type"])
            received_message.append(received_event)

    logger.info("Subscribing to 'nila.garuda.recon.command...")
    sub = await nats_client.subscribe("nila.garuda.recon.command", cb=handle_reconstructor_commands)

    trigger_start_pbs_job_command = ORCHESTRATOR_COMMAND_START_PBS_JOB

    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify Reconstructor received the correct command
    assert len(received_message) >= 1, "No status updates received from Reconstructor."
    assert received_message[0]["type"] == "start_pbs_reconstruction"

    # Extract 'data' dictionary
    data = received_message[0]["data"]

    # Ensure 'pbs_main_scan_session_id' is a non-empty string
    assert isinstance(data.get("pbs_main_scan_session_id"), str) and data["pbs_main_scan_session_id"].strip(), \
        f"pbs_main_scan_session_id '{data.get('pbs_main_scan_session_id')}' is empty or invalid"

    # Ensure 'slot_id' is a numeric string
    # assert isinstance(data.get("slot_id"), str) and data["slot_id"].isdigit(), \
    #     f"slot_id '{data.get('slot_id')}' is invalid"

    # Ensure slot_id is a number and within valid range (1-6)
    assert data.get('slot_id').isdigit(), f"slot_id '{data.get('slot_id')}' is not a digit"
    assert 1 <= int(data.get("slot_id")) <= 6, f"slot_id '{data.get('slot_id')}' is out of range (1-6)"

    # Ensure 'aoi_id' is a numeric string
    assert isinstance(data.get("aoi_id"), str) and data["aoi_id"].isdigit(), \
        f"aoi_id '{data.get('aoi_id')}' is invalid"

    # Ensure 'qi_check' is a string representing a boolean ("True" or "False")
    assert data.get("qi_check") in {"True", "False"}, \
        f"qi_check '{data.get('qi_check')}' is invalid (must be 'True' or 'False')"

    # Ensure 'scan_pattern' is a non-empty string
    assert isinstance(data.get("scan_pattern"), str) and data["scan_pattern"].strip(), \
        f"scan_pattern '{data.get('scan_pattern')}' is empty or invalid"

    # Ensure 'aoi_dir_path' is a valid non-empty string
    assert isinstance(data.get("aoi_dir_path"), str) and data["aoi_dir_path"].strip(), \
        f"aoi_dir_path '{data.get('aoi_dir_path')}' is empty or invalid"

    # Ensure 'illumination_profile' is a valid non-empty string
    assert isinstance(data.get("illumination_profile"), str) and data["illumination_profile"].strip(), \
        f"illumination_profile '{data.get('illumination_profile')}' is empty or invalid"

    # Validate Specific field: Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job_command["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job_command['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_reconstruct_pbs_fov_issued_command_issued():
    """Integration test for the Reconstructor PBS FOV command issued workflow with message verification."""

    received_message = []

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    async def handle_reconstructor_commands(msg):
        received_command = json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_command}")
        if received_command["type"] == "reconstruct_pbs_fov":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    logger.info("Subscribing to 'nila.garuda.recon.command ...")
    sub = await nats_client.subscribe("nila.garuda.recon.command", cb=handle_reconstructor_commands)

    start_pbs_job_command = ORCHESTRATOR_COMMAND_START_PBS_JOB

    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify Reconstructor received the correct command
    assert len(received_message) >= 1, "No status updates received from Reconstructor."
    assert received_message[0]["type"] == "reconstruct_pbs_fov"

    # Ensure 'data' exists in received_message
    assert "data" in received_message[0], "'data' field is missing in received message"

    # Extract 'data' dictionary
    data = received_message[0]["data"]

    # Ensure 'slot_id' is a string containing a number within the range 1-6
    assert isinstance(data.get("slot_id"), str) and data["slot_id"].isdigit() and 1 <= int(data["slot_id"]) <= 6, \
        f"slot_id '{data.get('slot_id')}' is out of expected range (1-6)"

    # Ensure 'aoi_id' is a string containing only digits (assuming it's an ID)
    assert isinstance(data.get("aoi_id"), str) and data["aoi_id"].isdigit(), \
        f"aoi_id '{data.get('aoi_id')}' is invalid (must be a numeric string)"

    # Ensure 'fov_data' exists and is a dictionary
    assert isinstance(data.get("fov_data"), dict), \
        f"fov_data '{data.get('fov_data')}' is missing or not a dictionary"

    # Ensure 'path' exists in 'fov_data' and is a valid non-empty string
    assert isinstance(data["fov_data"].get("path"), str) and data["fov_data"]["path"].strip(), \
        f"path '{data['fov_data'].get('path')}' is empty or invalid"


    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_complete_pbs_reconstruction_command_issued():
    """Integration test for the Complete PBS Reconstructor  command issued workflow with message verification."""

    received_message = []

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    async def handle_reconstructor_commands(msg):
        received_event = json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_event}")
        if received_event["type"] == "complete_pbs_reconstruction":
            logger.info("update received message", received_event["type"])
            received_message.append(received_event)

    logger.info("Subscribing to 'nila.garuda.recon.command...")
    sub = await nats_client.subscribe("nila.garuda.recon.command", cb=handle_reconstructor_commands)

    trigger_start_pbs_job = ORCHESTRATOR_COMMAND_START_PBS_JOB

    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify Reconstructor received the correct command
    assert len(received_message) >= 1, "No status updates received from Reconstructor."
    assert received_message[0]["type"] == "complete_pbs_reconstruction"

    # Ensure 'data' exists in received_message
    assert "data" in received_message[0], "'data' field is missing in received message."

    # Extract 'data' dictionary
    data = received_message[0]["data"]

    # Ensure 'slot_id' is a string containing a number within the range 1-6
    assert isinstance(data.get("slot_id"), str) and data["slot_id"].isdigit() and 1 <= int(data["slot_id"]) <= 6, \
        f"slot_id '{data.get('slot_id')}' is out of expected range (1-6) or invalid"

    # Ensure 'aoi_id' is a string containing only digits (assuming it's an ID)
    assert isinstance(data.get("aoi_id"), str) and data["aoi_id"].isdigit(), \
        f"aoi_id '{data.get('aoi_id')}' is invalid (must be a numeric string)"

    # Ensure 'metadata' and 'trace_id' exist before assertion
    assert "metadata" in received_message[0] and "trace_id" in received_message[0]["metadata"], \
        "Missing 'metadata' or 'trace_id' in received message"

    # Validate Specific field: Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_pbs_aoi_reconstruction_completed_event_issued():
    """Integration test for the PBS Reconstruction Completed event issued workflow with message verification."""

    received_message = []

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    async def handle_reconstructor_events(msg):
        received_event = json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_event}")
        if received_event["type"] == "pbs_aoi_reconstruction_completed":
            logger.info("update received message", received_event["type"])
            received_message.append(received_event)

    logger.info("Subscribing to 'nila.garuda.recon.event ...")
    sub = await nats_client.subscribe("nila.garuda.recon.event", cb=handle_reconstructor_events)

    trigger_start_pbs_job = ORCHESTRATOR_COMMAND_START_PBS_JOB

    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify Reconstructor received the correct command
    assert len(received_message) >= 1, "No status updates received from Reconstructor."
    assert received_message[0]["type"] == "pbs_aoi_reconstruction_completed"

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

    # Validate Specific field: Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()








