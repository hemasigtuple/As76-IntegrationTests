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
async def test_show_macro_scan_progress_screen_command_issued(nats_client):
    """Integration test verifying 'start PBS job' triggers, "show Macro scan progress screen" commands issued workflow with message verification."""
    received_message = []

    #Initialize and connect to Nats client
    nats_conn= NATSConnection()
    nats_client = await nats_conn.connect()

    trace_id = str(uuid.uuid4())

    #subscribe to Console behavior
    async def handle_console_commands(msg):

        received_command=json.loads(msg.data.decode())
        logger.info(f"UI received message:{received_command}")

        if received_command["type"] == "show_macro_scan_progress_screen":
            logger.info(f"Received message type: {received_command['type']}")
            received_message.append(received_command)

    logger.info(f"Subscribe to nila.console.commands")
    # Subscribe to the UI command  topic
    sub = await nats_client.subscribe("nila.console.commands", cb=handle_console_commands)

    trigger_start_pbs_job_command =  generate_orchestrator_command_start_pbs_job(correlation_id=None, trace_id=trace_id)

    logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())



    # Run the infinite loop for message handling
    async def infinite_loop():
        while True:
            await asyncio.sleep(1)
            logger.info(f"Listening for more messages show_macro_scan_progress_screen ...")

    # Run the infinite loop in the background
    loop_task = asyncio.create_task(infinite_loop())

    # Wait for messages to propagate
    logger.info(f"Waiting for messages to propagate...")
    await asyncio.sleep(30)

    # Unsubscribe after waiting for the messages
    await sub.unsubscribe(1)


    # Wait for the infinite loop task to finish (this is more for cleanup)
    loop_task.cancel()

    # Verify scanner received the correct command
    logger.info(f"Verify the Received Messages: {received_message}")
    assert len(received_message) >= 1, f"No status updates received from Console."
    assert received_message[0]["type"] == "show_macro_scan_progress_screen"

    # Assert specific field : Trace II
    assert received_message[0]["metadata"]["trace_id"] == trace_id, \
    f"Expected trace ID to be '{trace_id}', but got {received_message[0]['metadata'].get('trace_id')}"

    await nats_client.flush()
    await nats_client.drain()
    # Close the Nats connection
    await nats_conn.close()

@pytest.mark.asyncio
async def test_show_getting_ready_for_scan_message_command_issued(nats_client):
    """Integration test verifying 'start PBS job' triggers, then 'show getting ready for scan message' follows with expected UI response."""

    received_message = []

    # Initialize and connect the NATS client
    nats_conn = NATSConnection()
    nats_client = await nats_conn.connect()

    trace_id = str(uuid.uuid4())

    # Subscribe to scanner behavior
    async def handle_console_commands(msg):
        received_command = json.loads(msg.data.decode())
        logger.info(f"UI received message: {received_command}")

        if received_command["type"] == "show_getting_ready_for_scan_message":
            logger.info(f"Received message type: {received_command['type']}")
            received_message.append(received_command)

    logger.info(f"Subscribe to nila.console.commands")

    trigger_start_pbs_job_command = generate_orchestrator_command_start_pbs_job(correlation_id=None, trace_id=trace_id)

    logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())

    # Subscribe to the UI command topic
    sub = await nats_client.subscribe("nila.console.commands", cb=handle_console_commands)

    # Run the infinite loop for message handling
    async def infinite_loop():
        while True:
            await asyncio.sleep(1)
            logger.info(f"Listening for more messages show_getting_ready_for_scan_message ...")

    # Run the infinite loop in the background
    loop_task = asyncio.create_task(infinite_loop())

    # Wait for messages to propagate
    logger.info(f"Waiting for messages to propagate...")
    await asyncio.sleep(60)
    await nats_client.flush()

    # Unsubscribe after waiting for the messages
    await sub.unsubscribe(1)

    # Wait for the infinite loop task to finish (this is more for cleanup)
    loop_task.cancel()

    # Verify UI received the correct command
    logger.info(f"Verify the Received Messages: {received_message}")
    assert len(received_message) >= 1,"No status updates received from UI."
    assert received_message[0]["type"] == "show_getting_ready_for_scan_message"

    #Assert specific  field: Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trace_id, \
    f"Expected trace ID to be '{trace_id}', but got {received_message[0]['metadata'].get('trace_id')}"

    await nats_client.flush()
    await nats_client.drain()

    # Close the Nats connection
    await nats_conn.close()

@pytest.mark.asyncio
async def test_start_macro_scan_command_issued(nats_client):
    """Integration test verifying 'start PBS job' triggers, then 'start macro scan command' follows with expected scanner response."""

    received_message = []

    # Initialize and connect the NATS client
    # nats_conn = NATSConnection()
    # nats_client = await nats_conn.connect()

    trace_id = str(uuid.uuid4())

    # Subscribe to scanner behavior
    async def handle_scanner_commands(msg):
        received_command = json.loads(msg.data.decode())
        logger.info(f"Scanner received message: {received_command}")

        if received_command["type"] == "start_macro_scan":
            logger.info(f"Received message type: {received_command['type']}")
            received_message.append(received_command)

    # Subscribe to the Scanner command topic
    logger.info(f"Subscribe to nila.garuda.scanner.command")
    sub = await nats_client.subscribe("nila.garuda.scanner.command", cb=handle_scanner_commands)

    trigger_start_pbs_job_command = generate_orchestrator_command_start_pbs_job(correlation_id=None, trace_id=trace_id)

    logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())


    # Run the infinite loop for message handling
    async def infinite_loop():
        while True:
            await asyncio.sleep(1)
            logger.info(f"Listening for more messages start_macro_scan ...")

    # Run the infinite loop in the background
    loop_task = asyncio.create_task(infinite_loop())

    # Wait for messages to propagate
    logger.info(f"Waiting for messages to propagate...")
    await asyncio.sleep(10)

    # Unsubscribe after waiting for the messages
    await sub.unsubscribe(1)


    # Wait for the infinite loop task to finish (this is more for cleanup)
    loop_task.cancel()

    # Verify scanner received the correct command
    logger.info(f"Verify the Received Messages: {received_message}")
    assert len(received_message) >=1, "No status updates received from scanner."
    assert received_message[0]["type"] == "start_macro_scan"

    assert received_message[0]["metadata"]["trace_id"] == trace_id, \
        f"Expected trace ID to be '{trace_id}', but got {received_message[0]['metadata'].get('trace_id')}"

    await nats_client.flush()
    await nats_client.drain()
    #Close the Nats connection
    await nats_client.close()


@pytest.mark.asyncio
async def test_show_macro_scan_in_progress_info_command_issued(nats_client):
    """Integration test for the show getting ready for scan messages  Tray commands issued workflow with message verification."""
    received_message = []
    trace_id = str(uuid.uuid4())

    # Subscribe to Console behavior
    async def handle_console_commands(msg):
        received_command = json.loads(msg.data.decode())
        logger.info(f"UI received commands: {received_command}")

        if received_command["type"] == "show_macro_scan_in_progress_info":
            logger.info(f"Received message type: {received_command['type']}")
            received_message.append(received_command)

    logger.info(f"Subscribe to nila.console.commands")

    command =  generate_orchestrator_command_start_pbs_job(correlation_id=None, trace_id=trace_id)

    logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(command).encode())

    # Subscribe to the UI command after tray ejected topic
    sub = await nats_client.subscribe("nila.console.commands", cb=handle_console_commands)

    # Run the infinite loop for message handling
    async def infinite_loop():
        while True:
            await asyncio.sleep(1)
            logger.info(f"Listening for more messages show_macro_scan_in_progress_info ...")

    # Run the infinite loop in the background
    loop_task = asyncio.create_task(infinite_loop())

    # Wait for messages to propagate
    logger.info(f"Waiting for messages to propagate...")
    await asyncio.sleep(30)

    # Unsubscribe after waiting for the messages
    await sub.unsubscribe(1)

    # Wait for the infinite loop task to finish (this is more for cleanup)
    loop_task.cancel()

    # Verify console received the correct command
    logger.info(f"Verify the Received Messages: {received_message}")

    assert len(received_message) >=1, "No status updates received from console."
    assert received_message[0]["type"] == "show_macro_scan_in_progress_info"

    # Assert specific field : Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trace_id, \
        f"Expected trace ID to be '{trace_id}', but got {received_message[0]['metadata'].get('trace_id')}"

    await nats_client.flush()
    await nats_client.drain()
    # Close the Nats connection
    await nats_client.close()

@pytest.mark.asyncio
async def test_slot_macro_scan_completed_event_issued(nats_client):
    """Integration test verifying 'start PBS job' triggers, then 'slot_macro_scan_completed' follows with expected scanner response."""

    received_messages = []

    trace_id = str(uuid.uuid4())

    # Subscribe to scanner behavior
    async def handle_scanner_events(msg):
        received_event = json.loads(msg.data.decode())
        logger.info(f"Scanner received message: {received_event}")

        if received_event["type"] == "slot_macro_scan_completed":
            logger.info(f"Received message type: {received_event['type']}")
            received_messages.append(received_event)

    logger.info("Subscribing to 'nila.garuda.scanner.event.>'")
    await nats_client.subscribe("nila.garuda.scanner.event.>", cb=handle_scanner_events)

    # Publish command to trigger scanner process
    trigger_start_pbs_job_command = generate_orchestrator_command_start_pbs_job(correlation_id=None, trace_id=trace_id)
    logger.info(f"Publishing command to 'nila.device-job-orchestrator.commands': {trigger_start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands",
                              json.dumps(trigger_start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for scanner messages to propagate...")
    await asyncio.sleep(10)

    # Validate received messages
    assert len(received_messages) > 0, f"Expected at least 6 messages, but got {len(received_messages)}"

    for index, message in enumerate(received_messages):
        logger.info(f"Validating message {index + 1}/{len(received_messages)}: {message}")

        assert message["type"] == "slot_macro_scan_completed", f"Unexpected message type: {message['type']}"

        data = message["data"]

        #Asser slot_id
        assert "slot_id" in data and isinstance(data["slot_id"], str) and data["slot_id"].isdigit(), \
            f"Invalid 'slot_id': {data.get('slot_id')}"

        slot_id = int(data["slot_id"])
        assert 1 <= slot_id <= 6, f"slot_id {slot_id} is out of range!"

        # Assert slot_image
        assert "slot_image" in data, "'slot_image' key not found in the response"
        assert data["slot_image"].get("path"), "'slot_image' path is empty"
        assert isinstance(data["slot_image"].get("ppm"),
                          str), f"Invalid 'slot_image.ppm': {data['slot_image'].get('ppm')}"
        # Assert smear_info
        assert "smear_info" in data, "'smear_info' key not found in the response"
        assert isinstance(data["smear_info"], dict), "'smear_info' is not a dictionary"

        #Assert specific field: Trace ID
        assert message["metadata"]["trace_id"] == trace_id, \
            f"Expected trace ID '{trace_id}', but got {message['metadata'].get('trace_id')}"

    logger.info(f" All {len(received_messages)} messages passed validation successfully!")

    await nats_client.flush()
    await nats_client.drain()

    # Close NATS connection
    await nats_client.close()


@pytest.mark.asyncio
async def test_show_pbs_slides_details_entry_dialog_command_issued(nats_client):
    """Integration test verifying 'start PBS job' triggers, then 'show pbs slide details entry dialog' follows with expected UI response."""

    received_message = []
    trace_id = str(uuid.uuid4())
    # Subscribe to UI behavior
    async def handle_console_commands(msg):
        received_command = json.loads(msg.data.decode())
        logger.info(f"UI received message: {received_command}")

        if received_command.get("type") == "show_pbs_slides_details_entry_dialog":
            logger.info(f"Received message type: {received_command['type']}")
            received_message.append(received_command)

    logger.info(f"Subscribing to 'nila.console.commands'...")
    sub = await nats_client.subscribe("nila.console.commands", cb=handle_console_commands)

    trigger_start_pbs_job_command = generate_orchestrator_command_start_pbs_job(correlation_id=None, trace_id = trace_id)
    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())

    # Run the infinite loop for message handling
    async def infinite_loop():
        while True:
            await asyncio.sleep(1)
            logger.info(f"Listening for more messages show_pbs_slides_details_entry_dialog ...")

    # Run the infinite loop in the background
    loop_task = asyncio.create_task(infinite_loop())

    # Wait for messages to propagate
    logger.info(f"Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting for the messages
    await sub.unsubscribe(1)

    # Wait for the infinite loop task to finish (this is more for cleanup)
    loop_task.cancel()

    #looking for Received Message
    logger.info(f"Verify the Received Messages: {received_message}")

    # Check if received_message is not empty
    assert received_message, "No messages received from UI."

    # Verify the first received message
    message = received_message[0]

    assert message["type"] == "show_pbs_slides_details_entry_dialog", \
        f"Unexpected message type: {message['type']}"

    assert "data" in message and isinstance(message["data"], dict), \
        "'data' key is missing or not a dictionary"

    slide_data = message["data"].get("slide_data", [])
    assert isinstance(slide_data, list) and slide_data, "'slide_data' should be a non-empty list"

    # Extract max_slot_id dynamically from response
    max_slot_id = max(int(slot["slot_id"]) for slot in slide_data if "slot_id" in slot)

    # Extract required keys dynamically
    required_keys = set(slide_data[0].keys()) if slide_data else {"slot_id", "slot_image"}

    # Extract slot_image_keys dynamically
    slot_image_keys = set(slide_data[0]["slot_image"].keys()) if slide_data and "slot_image" in slide_data[0] else {
        "path", "ppm"}

    # Validate slots
    valid_slot_ids = {str(i) for i in range(1, max_slot_id + 1)}

    for slot in slide_data:
        assert isinstance(slot, dict), "Each entry in 'slide_data' should be a dictionary"
        assert required_keys.issubset(slot.keys()), f"Missing keys in slot data: {set(required_keys) - slot.keys()}"

        assert str(slot["slot_id"]) in valid_slot_ids, \
            f"Unexpected slot_id: {slot['slot_id']}. Expected: {valid_slot_ids}"

        slot_image = slot["slot_image"]
        assert isinstance(slot_image, dict), \
            f"'slot_image' should be a dictionary in slot_id {slot['slot_id']}"

        assert slot_image_keys.issubset(slot_image.keys()), \
            f"Missing slot_image keys: {set(slot_image_keys) - slot_image.keys()}"

    #Assert specific field : TraceID
    assert received_message[0]["metadata"]["trace_id"] == trace_id, \
        f"Expected trace ID to be '{trace_id}', but got {received_message[0]['metadata'].get('trace_id')}"

    await nats_client.flush()
    await nats_client.drain()
    # Close the NATS connection
    await nats_client.close()


@pytest.mark.asyncio
async def test_pbs_start_slot_scan_command_issued(nats_client):
    """Integration test for the PBS start scan command issued workflow with message verification."""
    received_message = []
    trace_id=str(uuid.uuid4())


    # Subscribe to Scanner behavior
    async def handle_scanner_commands(msg):
        received_command = json.loads(msg.data.decode())
        logger.info(f"Scanner received message: {received_command}")

        if received_command["type"] == "pbs_start_slot_scan":
            logger.info(f"Received message type: {received_command['type']}")
            received_message.append(received_command)

    # Subscribe to the scanner command  topic
    logger.info(f"Subscribed to nila.garuda.scanner.command")
    sub = await nats_client.subscribe("nila.garuda.scanner.command", cb=handle_scanner_commands)

    trigger_start_pbs_job = generate_orchestrator_command_start_pbs_job(correlation_id=None,  trace_id = trace_id)

    logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Wait for messages to propagate
    logger.info(f"Waiting for messages to propagate...")
    await asyncio.sleep(20)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    # Verify scanner received the correct command
    logger.info(f"Verify the Received Messages: {received_message}")

    #assert len(received_message) >= 1, "No status updates received from scanner."
    assert received_message, "No status updates received from scanner"
    assert received_message[0]["type"] == "pbs_start_slot_scan"

    # Assert Data in response Message
    assert "data" in received_message, f"Data key not found in received message."

    received_data=received_message[0]["data"]
    expected_data=trigger_start_pbs_job["data"]

    # Validate slot_id range 1 to 6
    slot_id = received_data.get("slot_id")
    assert isinstance(slot_id, int) and 1 <= slot_id <= 6, f"Invalid slot_id: {slot_id}"

    # Validate smear_info
    smear_info = received_data.get("smear_info")
    assert isinstance(smear_info, dict) and smear_info, f"Invalid smear_info: {smear_info}"

    # Validate smear_info presence and emptiness in  received message
    assert "smear_info" in smear_info, f"Missing 'smear_info' in received_data: {smear_info}"
    assert received_data["smear_info"], f"'smear_info' is empty in received_data: {smear_info}"

    # Compare smear_info in received_data and expected_data
    assert received_data["smear_info"] == expected_data["smear_info"], \
        f"Mismatch in smear_info. Received: {received_data['smear_info']}, Expected: {expected_data['smear_info']}"

    # Validate Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_pbs_slot_scan_started_event_issued(nats_client):
    """Integration test for the PBS slot scan started event  issued workflow with message verification."""
    received_message = []
    trace_id = str(uuid.uuid4())

    # Subscribe to Scanner behavior
    async def handle_scanner_events(msg):
        received_event = json.loads(msg.data.decode())
        logger.info(f"Scanner received message: {received_event}")
        if received_event["type"] == "pbs_slot_scan_started":
            logger.info(f"Received message type: {received_event['type']}")
            received_message.append(received_event)

    # Subscribe to the Scanner event topic
    logger.info(f"Subscribed to nila.garuda.scanner.event.>")
    sub = await nats_client.subscribe("nila.garuda.scanner.event.>", cb=handle_scanner_events)


    trigger_start_pbs_job = generate_orchestrator_command_start_pbs_job(correlation_id="",  trace_id = trace_id)

    logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Wait for messages to propagate
    logger.info(f"Waiting for messages to propagate...")
    await asyncio.sleep(20)

    # Unsubscribe after waiting for the messages
    await sub.unsubscribe(1)

    # Verify Scanner received the correct command
    logger.info(f"Verify the Received Messages: {received_message}")

    #assert len(received_message) >= 1, "No status updates received from Scanner."
    assert received_message, f"No status updates received from scanner"
    assert received_message[0]["type"] == "pbs_slot_scan_started"
    assert "data" in received_message[0], f"Data key not found in received message."

    # Ensure slot_id is a number and within valid range (1-6)
    assert received_message[0]["slot_id"].isdigit(), f"slot_id '{received_message[0]['slot_id']}' is not a digit"
    assert 1 <= int(received_message[0]["slot_id"]) <= 6, f"slot_id '{received_message[0]['slot_id']}' is out of range (1-6)"

    # Ensure aio_count is number and not empty
    assert isinstance(received_message[0]['aio_count'], int) and received_message[0]['aio_count'], \
        f"aio_count '{received_message[0]['aio_count']}' is empty or invalid"

    # Validate Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_pbs_aoi_scan_started_event_issued(nats_client):
    """Integration test for the PBS AOI Scan Started event issued workflow with message verification."""

    received_message = []
    trace_id = str(uuid.uuid4())

    async def handle_scanner_events(msg):
        received_event = json.loads(msg.data.decode())
        logger.info(f"Scanner received message: {received_event}")
        if received_event["type"] == "pbs_aoi_scan_started":
            logger.info(f"Received message type: {received_event['type']}")
            received_message.append(received_event)

    logger.info(f"Subscribing to 'nila.garuda.scanner.event...")
    sub = await nats_client.subscribe("nila.garuda.scanner.event", cb=handle_scanner_events)

    trigger_start_pbs_job = generate_orchestrator_command_start_pbs_job(correlation_id=None, trace_id = trace_id)

    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Wait for messages to propagate
    logger.info(f"Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify Scanner received the correct command
    assert len(received_message) >= 1, "No status updates received from Scanner."
    assert received_message[0]["type"] == "pbs_aoi_scan_started"
    # Ensure 'data' exists in received_message
    assert "data" in received_message[0], "'data' field is missing in received message"

    # Extract 'data' dictionary
    data = received_message[0]["data"]

    # Ensure 'pbs_main_scan_session_id' is a non-empty string
    assert isinstance(data.get("pbs_main_scan_session_id"), str) and data["pbs_main_scan_session_id"].strip(), \
        f"pbs_main_scan_session_id '{data.get('pbs_main_scan_session_id')}' is empty or invalid"

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
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"
    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_raw_fov_captured_event_issued(nats_client):
    """Integration test for the Raw FOV Captured event issued workflow with message verification."""
    received_message = []
    trace_id = str(uuid.uuid4())


    # Subscribe to Scanner behavior
    async def handle_scanner_events(msg):
        received_event = json.loads(msg.data.decode())
        logger.info(f" Scanner received message: {received_event}")
        if received_event["type"] == "raw_fov_captured":
            logger.info(f"Received message type: {received_event['type']}")
            received_message.append(received_event)


    # Subscribe to the Scanner event topic
    logger.info(f"Subscribed to nila.garuda.scanner.event.>")
    sub = await nats_client.subscribe("nila.garuda.scanner.event.>", cb=handle_scanner_events)


    trigger_start_pbs_job = generate_orchestrator_command_start_pbs_job(correlation_id=None,  trace_id = trace_id)

    logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

    # Wait for messages to propagate
    logger.info(f"Waiting for messages to propagate...")
    await asyncio.sleep(20)

    # Unsubscribe after waiting for the messages
    await sub.unsubscribe(1)

    # Verify Scanner received the correct command
    logger.info(f"Verify the Received Messages: {received_message}")
    assert len(received_message) >= 1, f"No status updates received from Scanner."
    assert received_message[0]["type"] == "raw_fov_captured"

    # Ensure 'data' exists in received_message
    assert "data" in received_message[0], "'data' field is missing in received message."

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

    # Ensure 'metadata' and 'trace_id' exist before assertion
    assert "metadata" in received_message[0] and "trace_id" in received_message[0]["metadata"], \
        "Missing 'metadata' or 'trace_id' in received message"

    # Validate Specific field: Trace ID
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_pbs_aoi_scan_completed_event_issued(nats_client):
        """Integration test for the PBS AOI Scan Completed event issued workflow with message verification."""
        received_message = []
        trace_id = str(uuid.uuid4())

        nats_conn = NATSConnection()
        nats_client = await nats_conn.connect()

        # Subscribe to Scanner behavior
        async def handle_scanner_events(msg):
            received_event = json.loads(msg.data.decode())
            logger.info(f" Scanner received message: {received_event}")
            if received_event["type"] == "pbs_aoi_scan_completed":
                logger.info(f"Received message type: {received_event['type']}")
                received_message.append(received_event)

        # Subscribe to the Scanner event topic
        logger.info(f"Subscribed to nila.garuda.scanner.event.>")
        sub = await nats_client.subscribe("nila.garuda.scanner.event.>", cb=handle_scanner_events)

        trigger_start_pbs_job = generate_orchestrator_command_start_pbs_job(correlation_id=None, trace_id = trace_id)

        logger.info(f"Publishing command to nila.device-job-orchestrator.commands: {trigger_start_pbs_job}")
        await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job).encode())

        # Wait for messages to propagate
        logger.info(f"Waiting for messages to propagate...")
        await asyncio.sleep(20)

        # Unsubscribe after waiting for the messages
        await sub.unsubscribe(1)

        # Verify Scanner received the correct command
        logger.info(f"Verify the Received Messages: {received_message}")
        assert len(received_message) >= 1, "No status updates received from Scanner."
        assert received_message[0]["type"] == "pbs_aoi_scan_completed"

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

        # Ensure 'scan_status' is a valid non-empty string
        assert isinstance(data.get("scan_status"), str) and data["scan_status"].strip(), \
            f"scan_status '{data.get('scan_status')}' is empty or invalid"

        # Ensure 'scanner_purge_folder' exists and is a valid non-empty string
        assert isinstance(data.get("scanner_purge_folder"), str) and data["scanner_purge_folder"].strip(), \
            f"scanner_purge_folder '{data.get('scanner_purge_folder')}' is empty or invalid"

        # Ensure 'metadata' and 'trace_id' exist before assertion
        assert "metadata" in received_message[0] and "trace_id" in received_message[0]["metadata"], \
            "Missing 'metadata' or 'trace_id' in received message."

        # Validate Specific field: Trace ID
        assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job["metadata"]["trace_id"], \
            f"Expected trace ID to be '{trigger_start_pbs_job['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

        # Close the NATS client
        await nats_client.close()
