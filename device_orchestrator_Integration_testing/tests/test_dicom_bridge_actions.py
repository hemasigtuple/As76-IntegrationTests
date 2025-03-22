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
async def test_create_uwl_workitem_command_issued(nats_client):
    """Integration test for the Create UWL workItem  command issued workflow with message verification."""

    received_message = []
    trace_id = str
    # Subscribe to DICOM Bridge behavior
    async def handle_dicom_bridge_commands(msg):
        received_command = json.loads(msg.data.decode())
        logger.info(f"DICOM Bridge received message: {received_command}")
        if received_command["type"] == "create_uwl_workitem":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    # async def handle_console_commands(msg):
    #     received_command = json.loads(msg.data.decode())
    #      logger.info(received_command["type"], received_command)

    # Subscribe to the DICOM bridge command topic
    logger.info("Subscribed to nila.dicom_bridge.commands")
    sub = await nats_client.subscribe("nila.dicom_bridge.commands", cb=handle_dicom_bridge_commands)

    # sub2 = await nats_client.subscribe("nila.console.commands", cb=handle_console_commands)

    trigger_start_pbs_job = generate_orchestrator_command_start_pbs_job(correlation_id = None, trace_id = trace_id)

    logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Run the infinite loop for message handling
    async def infinite_loop():
        while True:
            await asyncio.sleep(1)
            logger.info("Listening for more messages create_uwl_work-item...")

    # Run the infinite loop in the background
    loop_task = asyncio.create_task(infinite_loop())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(100)

    # Unsubscribe after waiting for the messages
    await sub.unsubscribe(1)

    # Wait for the infinite loop task to finish (this is more for cleanup)
    loop_task.cancel()

    # Verify DICOM Bridge received the correct command
    logger.info(f"Verify the Received Messages: {received_message}")

    # assert len(received_message) >=1, "No status updates received to DICOM Bridge."
    assert received_message, f"No status updates received to DICOM Bridge"
    assert received_message[0]["type"] == "create_uwl_workitem", f"Unexpected message type: {received_message[0]['type']}"

    # Verify received received_message against expected data
    for received_messages in received_message:
        assert received_messages["type"] == "create_uwl_workitem", f"Unexpected message type: {received_messages['type']}"
        received_data = received_messages[0]["data"]
        expected_data = trigger_start_pbs_job["data"]

        for key in expected_data:
            assert received_data[key] == expected_data[
                key], f"Mismatch for field '{key}': expected {expected_data[key]}, got {received_data[key]}"

        # Ensure slot_id is a number and within valid range (1-6)
        assert received_data["slot_id"].isdigit(), f"slot_id '{received_data['slot_id']}' is not a digit"
        assert 1 <= int(received_data["slot_id"]) <= 6, f"slot_id '{received_data['slot_id']}' is out of range (1-6)"

        # Ensure slide_id follows expected format (Example: Alphanumeric, starts with "SIG")
        assert isinstance(received_data["slide_id"], str) and received_data["slide_id"].startswith("SIG"), \
            f"slide_id '{received_data['slide_id']}' is invalid"

        # Ensure procedure_step_label is a string and not empty
        assert isinstance(received_data["procedure_step_label"], str) and received_data["procedure_step_label"].strip(), \
            f"procedure_step_label '{received_data['procedure_step_label']}' is empty or invalid"

        # Assert specific field: procedure_step_label as per Expected Data
        assert received_data["procedure_step_label"] == expected_data["procedure_step_label"], \
            f"Mismatch in 'procedure_step_label': expected {expected_data['procedure_step_label']}, got {received_data['procedure_step_label']}"

    # Assert metadata field: trace_id
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
            f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_uwl_workitem_created_event_issued(nats_client):
    """Integration test for the UWL workitem created event issued workflow with message verification."""
    received_message = []

    trace_id = str(uuid.uuid4())
    # Subscribe to Dicom bridge behavior
    async def handle_dicom_bridge_events(msg):
        received_event = json.loads(msg.data.decode())
        # logger.info(f"UI received message: {received_event}")
        if received_event["type"] == "uwl_workitem_created":
            # logger.info("update received message", received_event["type"])
            received_message.append(received_event)

    # Subscribe to the Dicom Bridge event topic
    logger.info("Subscribed to nila.dicom_bridge.events")
    sub = await nats_client.subscribe("nila.dicom_bridge.events", cb=handle_dicom_bridge_events)

    trigger_start_pbs_job = generate_orchestrator_command_start_pbs_job(correlation_id = None, trace_id = trace_id)

    logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(20)

    # Unsubscribe after waiting for the messages
    await sub.unsubscribe(1)

    # Verify Dicom bridge received the correct command
    logger.info(f"Verify the Received Messages: {received_message}")

    assert len(received_message) >= 1, "No status updates received from Dicom bridge."
    #assert received_message, f"No status updates received from DICOM Bridge."
    assert received_message[0]["type"] == "uwl_workitem_created"

    # Assert Data in response Message
    assert "data" in received_message, f"Data key not found in received message."

    received_event_data = received_message[0]["data"]
    expected_event_data = trigger_start_pbs_job[0]["data"]

    response_code = received_event_data.get("response_code")
    assert isinstance(response_code, int), f"Invalid response code: {response_code}"

    #Verify the Response code is 201
    assert response_code == 201, f"Invalid response code: {response_code}"

    # Compare Response code received_event_data and expected_event_data
    assert received_event_data["response_code"] == expected_event_data["response_code"], \
        f"Mismatch in response_code. Received: {received_event_data['response_code']}, Expected: {expected_event_data['response_code']}"

    # Assert Specific field : Trace Id
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_update_uwl_workitem_state_command_issued(nats_client):
    """Integration test for the Update UWL workItem sate command issued workflow with message verification."""
    received_message = []
    trace_id = str(uuid.uuid4())

    # Subscribe to DICOM bridge behavior
    async def handle_dicom_bridge_commands(msg):
        received_command = json.loads(msg.data.decode())
        logger.info(f" DICOM Bridge received message: {received_command}")
        if received_command["type"] == "update_uwl_workitem_state":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    # Subscribe to the DICOM Bridge command topic
    logger.info("Subscribed to nila.dicom_bridge.commands")
    sub = await nats_client.subscribe("nila.dicom_bridge.commands", cb=handle_dicom_bridge_commands)


    trigger_start_pbs_job = generate_orchestrator_command_start_pbs_job(correlation_id = None, trace_id = trace_id)

    logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(5)

    # Unsubscribe after waiting for the messages
    await sub.unsubscribe(1)

    # Verify ASTM received the correct command
    logger.info(f"Verify the Received Messages: {received_message}")
    assert len(received_message) >= 1, "No status updates received from DICOM bridge."
    assert received_message[0]["type"] == "update_uwl_workitem_state"

    # Ensure slot_id is a number and within valid range (1-6)
    assert received_message[0]["data"]["slot_id"].isdigit(), f"slot_id '{received_message[0]['data']['slot_id']}' is not a digit"
    assert 1 <= int(
        received_message[0]["data"]["slot_id"]) <= 6, f"slot_id '{received_message[0]['slot_id']['data']}' is out of range (1-6)"

    # Ensure slide_id follows expected format (Example: Alphanumeric, starts with "SIG")
    assert isinstance(received_message[0]["data"]["slide_id"], str) and received_message[0]["data"][
        "slide_id"].startswith("SIG"), \
        f"slide_id '{received_message[0]['data']['slide_id']}' is invalid"

    # Ensure procedure_step_label is a string and not empty
    assert isinstance(received_message[0]["data"]["procedure_step_label"], str) and received_message[0]["data"]["procedure_step_label"].strip(), \
        f"procedure_step_label '{received_message[0]['data']['procedure_step_label']}' is empty or invalid"

    # Assert specific field: procedure_step_label as per Expected Data
    assert received_message[0]["data"]["procedure_step_label"] == trigger_start_pbs_job["procedure_step_label"], \
        f"Mismatch in 'procedure_step_label': expected {trigger_start_pbs_job['procedure_step_label']}, got {received_message[0]['data']['procedure_step_label']}"

    # Validate Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_uwl_workitem_state_updated_event_issued(nats_client):
    """Integration test for the UWL Workitem State Updated event  issued workflow with message verification."""
    received_message = []
    trace_id = str(uuid.uuid4())

    # Subscribe to Scanner behavior
    async def handle_dicom_bridge_events(msg):
        received_event = json.loads(msg.data.decode())
        logger.info(f"DICOM Bridge received message: {received_event}")
        if received_event["type"] == "uwl_workitem_state_updated":
            logger.info("update received message", received_event["type"])
            received_message.append(received_event)

    # Subscribe to the DICOM Bridge event topic
    logger.info("Subscribed to nila.dicom_bridge.events....")
    sub = await nats_client.subscribe("nila.dicom_bridge.events", cb=handle_dicom_bridge_events)

    trigger_start_pbs_job = generate_orchestrator_command_start_pbs_job(correlation_id = None, trace_id = trace_id)

    logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(20)

    # Unsubscribe after waiting for the messages
    await sub.unsubscribe(1)

    # Verify DICOm Bridge received the correct command
    logger.info(f"Verify the Received Messages: {received_message}")
    assert len(received_message) >= 1, "No status updates received from DICOM Bridge."
    assert received_message[0]["type"] == "uwl_workitem_state_updated"

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

    # Ensure 'wor-kitem_state' is a valid non-empty string
    assert isinstance(data.get("workitem_state"), str) and data["workitem_state"].strip(), \
        f"workitem_state '{data.get('workitem_state')}' is empty or invalid"

    # Ensure 'response_status' is a string representing a numeric HTTP status (e.g., "200")
    assert isinstance(data.get("response_status"), str) and data["response_status"].isdigit(), \
        f"response_status '{data.get('response_status')}' is invalid (must be a numeric string like '200')"

    # Validate Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_upload_dicom_object_command_issued(nats_client):
    """Integration test for the Create UWL workItem  command issued workflow with message verification."""
    received_message = []
    trace_id = str(uuid.uuid4())

    # Subscribe to DICOM bridge behavior
    async def handle_dicom_bridge_commands(msg):
        received_command = json.loads(msg.data.decode())
        logger.info(f"DICOM Bridge received message: {received_command}")
        if received_command["type"] == "upload_dicom_object":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    # Subscribe to the DICOM bridge command topic
    logger.info("Subscribed to nila.dicom_bridge.commands...")
    sub = await nats_client.subscribe("nila.dicom_bridge.commands", cb=handle_dicom_bridge_commands)

    trigger_start_pbs_job = generate_orchestrator_command_start_pbs_job(correlation_id = None, trace_id = trace_id)

    logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(5)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    # Verify DICOM Bridge received the correct command
    logger.info(f"Verify the Received Messages: {received_message}")
    assert len(received_message) >= 1, "No status updates received from DICOM Bridge."
    assert received_message[0]["type"] == "upload_dicom_object"

    # Ensure 'data' exists
    assert "data" in received_message[0], "'data' field is missing in received message"

    # Extract 'data'
    data = received_message[0]["data"]

    # Validate 'slot_id' (exists, is a string, and within range 1-6)
    assert "slot_id" in data and isinstance(data["slot_id"], str) and data["slot_id"].isdigit() and 1 <= int(
        data["slot_id"]) <= 6, \
        f"Invalid slot_id '{data.get('slot_id')}'. Expected range: 1 to 6"

    # Validate 'dir_path' (exists and is not empty)
    assert "dir_path" in data and isinstance(data["dir_path"], str) and data["dir_path"].strip(), \
        f"'dir_path' is missing or empty"

    # Validate Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_dicom_object_uploaded_event_issued(nats_client):
    """Integration test for the DICOM Object Upload event issued workflow with message verification."""
    received_message = []
    trace_id = str(uuid.uuid4())
    # Subscribe to DICOM Bridge behavior
    async def handle_dicom_bridge_events(msg):
        received_event= json.loads(msg.data.decode())
        logger.info(f"DICOM Bridge received message: {received_event}")
        if received_event["type"] == "dicom_object_uploaded":
            logger.info("update received message", received_event["type"])
            received_message.append(received_event)

    # Subscribe to the DICOM bridge command topic
    logger.info("Subscribed to nila.dicom_bridge.events...")
    sub = await nats_client.subscribe("nila.dicom_bridge.events", cb=handle_dicom_bridge_events)


    trigger_start_pbs_job =generate_orchestrator_command_start_pbs_job(correlation_id = None, trace_id = trace_id)

    logger.info(f"Publishing command  Start PBS Command: {trigger_start_pbs_job}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(5)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    # Verify DICOM Bridge  event received the correct command
    logger.info(f"Verify the Received Messages: {received_message}")

    assert len(received_message) >= 1, "No status updates received from DICOM Bridge."
    assert received_message[0]["type"] == "dicom_object_uploaded"

    # Extract 'data'
    data = received_message[0]["data"]

    # Validate 'slot_id' (exists, is a string, and within range 1-6)
    assert "slot_id" in data and isinstance(data["slot_id"], str) and data["slot_id"].isdigit() and 1 <= int(
        data["slot_id"]) <= 6, \
        f"Invalid slot_id '{data.get('slot_id')}'. Expected range: 1 to 6"

    # Validate Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_get_study_size_command_issued(nats_client):
    """Integration test for the Get Study Size command issued workflow with message verification."""

    received_message = []

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    async def handle_dicom_bridge_commands(msg):
        received_command = json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_command}")
        if received_command["type"] == "get_study_size":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    logger.info("Subscribing to 'nila.dicom_bridge.commands...")
    sub = await nats_client.subscribe("nila.dicom_bridge.commands", cb=handle_dicom_bridge_commands)

    trigger_start_pbs_job = ORCHESTRATOR_COMMAND_START_PBS_JOB
    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")

    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify Dicom Convertor received the correct command
    assert len(received_message) >= 1, "No status updates received from Dicom Converto."
    assert received_message[0]["type"] == "get_study_size"

    # Ensure 'data' exists in received_message
    assert "data" in received_message[0], "'data' field is missing in received message"

    # Extract 'data' dictionary
    data = received_message[0]["data"]

    # Validate 'slot_id' (should be an integer and within range 1 to 6)
    assert "slot_id" in data and isinstance(data["slot_id"], int) and 1 <= data["slot_id"] <= 6, \
        f"Invalid 'slot_id': {data.get('slot_id')}"
    # Validate Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_study_size_calculated_event_issued(nats_client):
    """Integration test for the  Study Size calculated event issued workflow with message verification."""
    received_message = []

    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    async def handle_dicom_bridge_events(msg):
        received_event = json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_event}")
        if received_event["type"] == "study_size_calculated":
            logger.info("update received message", received_event["type"])
            received_message.append(received_event)

    logger.info("Subscribing to 'nila.dicom_bridge.events...")
    sub = await nats_client.subscribe("nila.dicom_bridge.events", cb=handle_dicom_bridge_events)
    trigger_start_pbs_job = generate_orchestrator_command_start_pbs_job()
    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")

    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify DICOM Convertor event received the correct command
    assert len(received_message) >= 1, "No status updates received from DICOM Convertor."
    assert received_message[0]["type"] == "study_size_calculated"

    # Ensure 'data' exists in received_message
    assert "data" in received_message[0], "'data' field is missing in received message"

    # Extract 'data' dictionary
    data = received_message[0]["data"]

    # Validate 'slot_id' (should be an integer and within range 1 to 6)
    assert "slot_id" in data and isinstance(data["slot_id"], int) and 1 <= data["slot_id"] <= 6, \
        f"Invalid 'slot_id': {data.get('slot_id')}"

    # Validate Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()























