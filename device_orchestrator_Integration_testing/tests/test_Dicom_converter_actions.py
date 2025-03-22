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
async def test_create_dicom_files_command_issued(nats_client):
    """Integration test for the Create Dicom Files  command issued workflow with message verification."""

    received_message = []

    async def handle_dicom_convertor_commands(msg):
        received_command = json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_command}")
        if received_command["type"] == "create_dicom_files":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    logger.info("Subscribing to 'nila.dicom-converter.commands...")
    sub = await nats_client.subscribe("nila.dicom-converter.commands", cb=handle_dicom_convertor_commands)

    trigger_start_pbs_job_command = generate_orchestrator_command_start_pbs_job()
    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")

    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify Dicom Convertor received the correct command
    assert len(received_message) >= 1, "No status updates received from Dicom Converto."
    assert received_message[0]["type"] == "create_dicom_files"

    # Ensure 'data' exists
    assert "data" in received_message[0], "'data' field is missing in received message."

    # Extract 'data'
    data = received_message[0]["data"]

    # Validate 'slot_id' (exists, is a string, and within range 1-6)
    assert "slot_id" in data and isinstance(data["slot_id"], str) and data["slot_id"].isdigit() and 1 <= int(
        data["slot_id"]) <= 6, \
        f"Invalid slot_id '{data.get('slot_id')}'. Expected range: 1 to 6"

    # Validate 'slide_id' (exists and is a string)
    assert "slide_id" in data and isinstance(data["slide_id"], str), \
        f"Invalid slide_id '{data.get('slide_id')}'. Expected a string"

    # Validate 'image_path' (exists and is not empty)
    assert "image_path" in data and isinstance(data["image_path"], str) and data["image_path"].strip(), \
        f"'image_path' is missing or empty"

    # Validate 'macro_image_path' (exists and is not empty)
    assert "macro_image_path" in data and isinstance(data["macro_image_path"], str) and data[
        "macro_image_path"].strip(), \
        f"'macro_image_path' is missing or empty"

    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job_command["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job_command['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_dicom_files_created_event_issued(nats_client):
    """Integration test for the DICOM Files created event issued workflow with message verification."""

    received_message = []

    async def handle_dicom_convertor_events(msg):
        received_event = json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_event}")
        if received_event["type"] == "dicom_files_created":
            logger.info("update received message", received_event["type"])
            received_message.append(received_event)

    logger.info("Subscribing to 'nila.dicom-converter.events.*.* ...")
    sub = await nats_client.subscribe("nila.dicom-converter.events.*.* ", cb=handle_dicom_convertor_events)

    trigger_start_pbs_job_command = generate_orchestrator_command_start_pbs_job()
    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")

    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify DICOM Convertor event received the correct command
    assert len(received_message) >= 1, "No status updates received from DICOM Convertor."
    assert received_message[0]["type"] == "dicom_files_created"

    # Ensure 'data' exists
    assert "data" in received_message[0], "'data' field is missing in received message."

    # Extract 'data'
    data = received_message[0]["data"]

    # Validate 'slot_id' (exists, is a string, and within range 1-6)
    assert "slot_id" in data and isinstance(data["slot_id"], str) and data["slot_id"].isdigit() and 1 <= int(
        data["slot_id"]) <= 6, \
        f"Invalid slot_id '{data.get('slot_id')}'. Expected range: 1 to 6"

    # Validate 'macro_image_path' (exists and is not empty)
    assert "macro_image_path" in data and isinstance(data["macro_image_path"], str) and data[
        "macro_image_path"].strip(), \
        f"'macro_image_path' is missing or empty"

    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job_command["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job_command['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_create_patch_annotation_dicom_files_command_issued(nats_client):
    """Integration test for the Create patch annotation DICOM Files command issued workflow with message verification."""

    received_message = []

    async def handle_dicom_convertor_commands(msg):
        received_command = json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_command}")
        if received_command["type"] == "create_patch_annotation_dicom_files":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    logger.info("Subscribing to 'nila.dicom-converter.commands...")
    sub = await nats_client.subscribe("nila.dicom-converter.commands", cb=handle_dicom_convertor_commands)

    trigger_start_pbs_job_command = generate_orchestrator_command_start_pbs_job()
    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")
    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify Dicom Convertor received the correct command
    assert len(received_message) >= 1, "No status updates received from Dicom Converto."
    assert received_message[0]["type"] == "create_patch_annotation_dicom_files"

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
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job_command["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job_command['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_patch_annotation_dicom_files_created_event_issued(nats_client):
    """Integration test for the Patch Annotation DICOM files Created  event issued workflow with message verification."""

    received_message = []

    async def handle_dicom_convertor_events(msg):
        received_event = json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_event}")
        if received_event["type"] == "patch_annotation_dicom_files_created":
            logger.info("update received message", received_event["type"])
            received_message.append(received_event)

    logger.info("Subscribing to nila.dicom-converter.events.*...")
    sub = await nats_client.subscribe("nila.dicom-converter.events.*", cb=handle_dicom_convertor_events)

    trigger_start_pbs_job_command = generate_orchestrator_command_start_pbs_job()
    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")

    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify DICOM Convertor event received the correct command
    assert len(received_message) >= 1, "No status updates received from DICOM Convertor."
    assert received_message[0]["type"] == "patch_annotation_dicom_files_created"

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

    # Assert specific field Trace Id
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job_command["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job_command['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_create_dicom_files_for_initialize_and_analysis_data_command_issued(nats_client):
    """Integration test for the Create Dicom Files for Initialize and analysis Data command issued workflow with message verification."""

    received_message = []

    async def handle_dicom_convertor_commands(msg):
        received_command = json.loads(msg.data.decode())
        logger.info(f"Reconstructor received message: {received_command}")

        if received_command["type"] == "create_dicom_files_for_initialize_and_analysis_data":
            logger.info("update received message", received_command["type"])
            received_message.append(received_command)

    logger.info("Subscribing to 'nila.dicom-converter.commands...")
    sub = await nats_client.subscribe("nila.dicom-converter.commands", cb=handle_dicom_convertor_commands)

    trigger_start_pbs_job_command = generate_orchestrator_command_start_pbs_job()
    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")

    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify Dicom Convertor received the correct command
    assert len(received_message) >= 1, "No status updates received from Dicom Converto."
    assert received_message[0]["type"] == "create_dicom_files_for_initialize_and_analysis_data"

    # Ensure 'data' exists
    assert "data" in received_message[0], "'data' field is missing in received message"

    # Extract 'data'
    data = received_message[0]["data"]

    # Validate 'slot_id' (exists, is a string, numeric, and within range 1-6)
    assert "slot_id" in data and isinstance(data["slot_id"], str) and data["slot_id"].isdigit() and 1 <= int(
        data["slot_id"]) <= 6, \
        f"Invalid slot_id '{data.get('slot_id')}'. Expected range: 1 to 6"

    # Validate 'aoi_id' (exists and is a non-empty string)
    assert "aoi_id" in data and isinstance(data["aoi_id"], str) and data["aoi_id"].strip(), \
        f"'aoi_id' is missing or empty"

    # Validate 'analysis_status' (exists and is a non-empty string)
    assert "analysis_status" in data and isinstance(data["analysis_status"], str) and data["analysis_status"].strip(), \
        f"'analysis_status' is missing or empty"

    # Validate 'analysis_data' (exists and is a valid string)
    assert "analysis_data" in data and isinstance(data["analysis_data"], str) and data["analysis_data"].strip(), \
        f"'analysis_data' is missing or empty"

    # Validate 'analyser_purge_folder' (exists and is a valid string)
    assert "analyser_purge_folder" in data and isinstance(data["analyser_purge_folder"], str) and data[
        "analyser_purge_folder"].strip(), \
        f"'analyser_purge_folder' is missing or empty"

    # Validate 'macro_image' (exists and is a valid string)
    assert "macro_image" in data and isinstance(data["macro_image"], str) and data["macro_image"].strip(), \
        f"'macro_image' is missing or empty"

    # Validate 'analysis_message' (exists and contains necessary fields)
    assert "analysis_message" in data and isinstance(data["analysis_message"], dict), \
        f"'analysis_message' is missing or not a dictionary"

    assert "metadata" in data["analysis_message"] and isinstance(data["analysis_message"]["metadata"], dict), \
        f"'metadata' is missing in 'analysis_message'"

    assert "trace_id" in data["analysis_message"]["metadata"]['trace_id'] == trigger_start_pbs_job_command["analysis_message"]["metadata"]['trace_id'], \
        f"Expected trace ID to be '{trigger_start_pbs_job_command['analysis_message']['metadata']['trace_id']}', but got {received_message[0]['analysis_message']['metadata'].get('trace_id')}"

    assert "type" in data["analysis_message"] and isinstance(data["analysis_message"]["type"], str) and \
           data["analysis_message"]["type"].strip(), \
        f"'type' is missing or empty in 'analysis_message'"

    assert "data" in data["analysis_message"] and isinstance(data["analysis_message"]["data"], dict), \
        f"'data' is missing in 'analysis_message'"

    # Validate 'pbs_report_initialized' (exists and contains necessary fields)
    assert "pbs_report_initialized" in data and isinstance(data["pbs_report_initialized"], dict), \
        f"'pbs_report_initialized' is missing or not a dictionary"

    assert "metadata" in data["pbs_report_initialized"] and isinstance(data["pbs_report_initialized"]["metadata"],
                                                                       dict), \
        f"'metadata' is missing in 'pbs_report_initialized'"

    assert "type" in data["pbs_report_initialized"] and isinstance(data["pbs_report_initialized"]["type"], str) and \
           data["pbs_report_initialized"]["type"].strip(), \
        f"'type' is missing or empty in 'pbs_report_initialized'"

    assert "data" in data["pbs_report_initialized"] and isinstance(data["pbs_report_initialized"]["data"], dict), \
        f"'data' is missing in 'pbs_report_initialized'"

    # Validate 'slide_id' in 'pbs_report_initialized'
    assert "slide_id" in data["pbs_report_initialized"]["data"] and isinstance(
        data["pbs_report_initialized"]["data"]["slide_id"], str) and data["pbs_report_initialized"]["data"][
               "slide_id"].strip(), \
        f"'slide_id' is missing or empty in 'pbs_report_initialized'"

    # Validate 'slot_id' in 'pbs_report_initialized'
    assert "slot_id" in data["pbs_report_initialized"]["data"] and isinstance(
        data["pbs_report_initialized"]["data"]["slot_id"], str) and data["pbs_report_initialized"]["data"][
               "slot_id"].isdigit() and 1 <= int(data["pbs_report_initialized"]["data"]["slot_id"]) <= 6, \
        f"Invalid slot_id '{data['pbs_report_initialized']['data'].get('slot_id')}' in 'pbs_report_initialized'. Expected range: 1 to 6"
    # Validate 'macro_image'
    assert "macro_image" in data and isinstance(data["macro_image"], str) and data["macro_image"].strip(), \
        f"'macro_image' is missing or empty"

    # Validate 'macro_image' in pbs_report_initialized
    assert "macro_image" in data["pbs_report_initialized"]["data"] and isinstance(
        data["pbs_report_initialized"]["data"]["macro_image"], str) and data["pbs_report_initialized"]["data"][
               "macro_image"].strip(), \
        f"'macro_image' is missing or empty in 'pbs_report_initialized'"

    # Assert specific field Trace Id
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job_command["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job_command['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()

@pytest.mark.asyncio
async def test_dicom_files_for_initialize_and_analysis_data_created_event_issued(nats_client):
    """Integration test for the DICOM Files  for Initialize and analysis Data Created event issued workflow with message verification."""

    received_message = []

    async def handle_dicom_convertor_events(msg):
        received_event = json.loads(msg.data.decode())

        logger.info(f"Reconstructor received message: {received_event}")
        if received_event["type"] == "dicom_files_for_initialize_and_analysis_data_created":
            logger.info("update received message", received_event["type"])
            received_message.append(received_event)

    logger.info("Subscribing to 'nila.dicom-converter.events.* ...")
    sub = await nats_client.subscribe("nila.dicom-converter.events.*", cb=handle_dicom_convertor_events)

    trigger_start_pbs_job_command = generate_orchestrator_command_start_pbs_job()
    logger.info(f"Publishing to nila.device-job-orchestrator.commands: {trigger_start_pbs_job_command}")

    await nats_client.publish("nila.device-job-orchestrator.commands", json.dumps(trigger_start_pbs_job_command).encode())

    # Wait for messages to propagate
    logger.info("Waiting for messages to propagate...")
    await asyncio.sleep(25)

    # Unsubscribe after waiting
    await sub.unsubscribe(1)

    logger.info(f"Verify the Received Messages: {received_message}")

    # Verify DICOM Convertor event received the correct command
    assert len(received_message) >= 1, "No status updates received from DICOM Convertor."
    assert received_message[0]["type"] == "dicom_files_for_initialize_and_analysis_data_created"

    assert "data" in received_message[0], "Missing 'data' field"

    data = received_message[0]["data"]

    # Validate 'slot_id' (should be an integer and within range 1 to 6)
    assert "slot_id" in data and isinstance(data["slot_id"], int) and 1 <= data["slot_id"] <= 6, \
        f"Invalid 'slot_id': {data.get('slot_id')}"

    # Validate 'slide_id' (should be None or a string)
    assert "slide_id" in data and (data["slide_id"] is None or isinstance(data["slide_id"], str)), \
        f"Invalid 'slide_id': {data.get('slide_id')}"

    # Validate 'aoi_id' (should be an integer)
    assert "aoi_id" in data and isinstance(data["aoi_id"], int), \
        f"Invalid 'aoi_id': {data.get('aoi_id')}"

    # Validate 'patient_name' (should be a non-empty string)
    assert "patient_name" in data and isinstance(data["patient_name"], str) and data["patient_name"].strip(), \
        f"Invalid 'patient_name': {data.get('patient_name')}"

    # Assert specific field Trace Id
    assert received_message[0]["metadata"]["trace_id"] == trigger_start_pbs_job_command["metadata"]["trace_id"], \
        f"Expected trace ID to be '{trigger_start_pbs_job_command['metadata']['trace_id']}', but got {received_message[0]['metadata'].get('trace_id')}"

    # Close the NATS client
    await nats_client.close()
