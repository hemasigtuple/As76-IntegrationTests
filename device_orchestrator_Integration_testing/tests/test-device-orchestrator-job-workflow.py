import json
import os
import uuid
import asyncio
from datetime import datetime, timezone
import pytest
from dotenv import load_dotenv
from tests.conftest import logger
from nats.aio.client import Client as NATS

# Load environment variables from .env file
load_dotenv()

# Define subject mapping for NATS messages
SUBJECTS = {
    #commands
    "scanner_commands": "nila.garuda.scanner.command",
    "orchestrator_commands": "nila.device-job-orchestrator.commands",
    "console_commands": "nila.console.commands",
    "dicom_bridge_commands": "nila.dicom_bridge.commands",
    "analyser_commands": "nila.garuda.analyser.command",
    "reconstructor_commands": "nila.garuda.recon.command",
    "dicom_converter_commands": "nila.dicom-converter.commands",
    "cloud_bridge_commands": "nila.cloud_bridge.commands",

    #events
    # "scanner_events": "nila.garuda.scanner.event.>",
    # "dicom_bridge_events":"nila.dicom_bridge.events",
    # "analyser_event":"nila.garuda.analyser.event.*.*",
    # "reconstructor_event":"nila.garuda.recon.event.*.*",
    # "dicom_converter_events":"nila.dicom-converter.events.*.*",
    # "cloud_bridge_events":"nila.cloud_bridge.events",

}


# Create a MessageQueue class for storing messages
# class MessageQueue:
#     def __init__(self):
#         self.queue = asyncio.Queue()
#
#     async def put(self, message):
#         await self.queue.put(message)
#
#     async def get(self):
#         return await self.queue.get()
#
#     def qsize(self):
#         return self.queue.qsize()


# Utility function to get NATS URL from environment variable
def get_nats_url():
    return os.getenv("NATS_SERVER_URL", "nats://0.0.0.0:4222")


# Utility function to get NATS subject by key
def get_subject(key):
    return SUBJECTS[key]


# Function to clear all messages from the queue
async def clear_message_queue(queue):
    """Removes all old messages from the queue before running a test."""
    while not queue.empty():
        await queue.get()


# Fixture to provide a single NATS connection for the entire test class
@pytest.fixture(scope="class")
async def nats_connection():
    """Fixture to provide a single NATS connection for the entire test class"""
    nc = NATS()
    await nc.connect(servers=[get_nats_url()])
    logger.info(f"Connected to {get_nats_url()}")
    yield nc
    await nc.drain()
    await nc.close()


# Fixture to set up the PBS job before running tests
@pytest.fixture(scope="class")
async def setup_pbs_job(nats_connection):
    """Fixture to start the PBS job once before all test cases"""
    test_instance = TestPbsJobWorkflow()
    test_instance._nc = nats_connection
    test_instance.received_messages = asyncio.Queue()
    test_instance.message_received = asyncio.Event()
    test_instance._trace_id = str(uuid.uuid4())


    # Subscribe to relevant subjects before sending the job
    #commands subscribe
    await test_instance.listen_to_messages("scanner_commands")
    await test_instance.listen_to_messages("console_commands")
    await test_instance.listen_to_messages("dicom_bridge_commands")
    await test_instance.listen_to_messages("analyser_commands")
    await test_instance.listen_to_messages("reconstructor_commands")
    await test_instance.listen_to_messages("dicom_converter_commands")
    await test_instance.listen_to_messages("cloud_bridge_commands")

    #events subscribe
    # await test_instance.listen_to_messages("scanner_events")
    # await test_instance.listen_to_messages("dicom_bridge_events")
    # await test_instance.listen_to_messages("analyser_event")
    # await test_instance.listen_to_messages("reconstructor_event")
    # await test_instance.listen_to_messages("dicom_converter_events")
    # await test_instance.listen_to_messages("cloud_bridge_events")

    # Initiate PBS job once
    job_id, trace_id = await test_instance.initiate_start_pbs_job()

    test_instance._last_id = job_id
    test_instance._trace_id = trace_id

    return test_instance  # Return the test instance with state


# Base class for NATS integration tests
class TestBase:
    """Base class for NATS integration tests"""

    received_messages = None
    message_received = None
    _trace_id = None
    _nc = None
    _last_id = None

    # Listen to messages on a given subject
    async def listen_to_messages(self, subject_key):
        """Subscribe and listen to messages, blocking until a message is received"""
        subject = get_subject(subject_key)

        async def message_handler(msg):
            logger.info(f"Received a message on {subject}: {msg.data.decode()}")
            await self.received_messages.put(msg)
            self.message_received.set()  # Signal message received

        await self._nc.subscribe(subject, cb=message_handler)
        await self._nc.flush()  # Ensure subscription is active
        logger.info(f"Subscribed to {subject}...")

    # Initiate the start of a PBS job by publishing a message
    async def initiate_start_pbs_job(self):
        self._last_id = str(uuid.uuid4())
        """Publish the start_pbs_job command"""
        logger.info(f"Publishing start_pbs_job command...")

        start_pbs_command = {
            "id": self._last_id,
            "type": "start_pbs_job",
            "version": 1,
            "data": {
                "tray_type": "all_pbs",
                "scanned_by": "",
                "workflow_mode": "manual",
                "timezone": ""
            },
            "metadata": {
                "correlation_id": None,
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
                "trace_id": self._trace_id
            }
        }

        await self._nc.publish(
            get_subject("orchestrator_commands"),
            json.dumps(start_pbs_command).encode()
        )
        logger.info(f"Published start_pbs_job command...{start_pbs_command}")
        await asyncio.sleep(0.1)
        return self._last_id, self._trace_id


# Test class for PBS Job Workflow
@pytest.mark.usefixtures("nats_connection")
class TestPbsJobWorkflow(TestBase):
    """Test cases for PBS Job Workflow"""
    # """Initialize message queues for each test method"""

    @pytest.mark.asyncio
    async def test_eject_tray_command(self, setup_pbs_job):
        """Test that an eject_tray command is issued after starting a PBS job"""
        test_instance = setup_pbs_job
        expected_type = "eject_tray"

        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)
        #await asyncio.sleep(1)
        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        message = await test_instance.received_messages.get()
        message_data = json.loads(message.data.decode())

        # Debugging: Print all message data
        print(f"Received message: {json.dumps(message_data, indent=2)}")
        print(f"last_message: {message}")
        print(f"last message received:{message_data}")

        # Store the message in the appropriate queue
        if message_data["type"] == expected_type:
            print(f"{message_data['type']}")
        else:
            await test_instance.received_messages.put(message_data)  # Store unwanted messages

        # Assertions
        assert message_data["type"] == expected_type
        assert message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_tray_ejecting_dialog_command(self, setup_pbs_job):
        """Test that a show_tray_ejecting_dialog command is issued to the console"""
        test_instance = setup_pbs_job
        expected_type = "show_tray_ejecting_dialog"

        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")
        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")
        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
          print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Not relevant for Orchestrator Test")
    async def test_tray_ejected_scanner_event(self, setup_pbs_job):
        """Test that a tray_ejected event is published after tray ejection"""
        test_instance = setup_pbs_job
        expected_type = "tray_ejected"

        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")

        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")

        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_place_tray_dialog_command(self, setup_pbs_job):
        """Test that a show_place_tray_dialog command is issued to the console"""
        test_instance = setup_pbs_job
        expected_type = "show_place_tray_dialog"
        
        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
           print(f"{last_message_data['type']}") # Store valid message
        else:
            await test_instance.received_messages.put(last_message_data)

        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_macro_scan_progress_screen_command_issued(self, setup_pbs_job):
        """Test that a show_macro_scan_progress_screen command is issued to the console"""
        test_instance = setup_pbs_job
        expected_type = "show_macro_scan_progress_screen"
        
        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages

        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_getting_ready_for_scan_message_command_issued(self, setup_pbs_job):
        """Test that a show_getting_ready_for_scan_message is issued to the console"""
        test_instance = setup_pbs_job
        expected_type = "show_getting_ready_for_scan_message"
        
        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages

        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_start_macro_scan_command_issued(self, setup_pbs_job):
        """Test that a start_macro_scan command is issued to the console"""
        test_instance = setup_pbs_job
        expected_type = "start_macro_scan"

        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_macro_scan_in_progress_info_command_issued(self, setup_pbs_job):
        """Test that a show_macro_scan_in_progress_info command is issued to the console"""
        test_instance = setup_pbs_job
        expected_type = "show_macro_scan_in_progress_info"

        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_slot_macro_scan_completed_event_issued(self, setup_pbs_job):
        """Test that a slot_macro_scan_completed command is issued to the console"""
        test_instance = setup_pbs_job
        expected_type = "slot_macro_scan_completed"


        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")

        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")

        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_pbs_slides_details_entry_dialog_command_issued(self, setup_pbs_job):
        """Test that a show_pbs_slides_details_entry command is issued to the console"""

        test_instance = setup_pbs_job
        expected_type = "show_pbs_slides_details_entry_dialog"

        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_initiating_scan_page_command_issued(self, setup_pbs_job):
        """Test that a show_initiating_scan_page_command is issued to the console"""
        test_instance = setup_pbs_job
        expected_type = "show_initiating_scan_page"

        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_create_uwl_workitem_command_issued(self, setup_pbs_job):
        """Test that the create_uwl_workitem command is issued exactly 8 times."""

        test_instance = setup_pbs_job
        expected_type = "create_uwl_workitem"

        expected_procedure_steps = {
            "PBS Scan",
            "PBS Monolayer AOI Reconstruction",
            "PBS Feathered Edge AOI Reconstruction",
            "PBS Monolayer AOI Analysis",
            "PBS Feathered Edge AOI Analysis",
            "PBS DICOM Conversion",
            "PBS Report Review",
            "Approved CBC Data Sent to P8000"
        }

        received_procedure_steps = set()
        attempt = 0
        max_attempts = 20  # Prevents infinite loop

        while received_procedure_steps != expected_procedure_steps and attempt < max_attempts:
            try:
                await asyncio.wait_for(test_instance.message_received.wait(), timeout=10)
                last_message = await test_instance.received_messages.get()
                last_message_data = json.loads(last_message.data.decode())

                logger.info(f"Received message type: {last_message_data['type']}")

                if last_message_data["type"] == expected_type:
                    procedure_step_label = last_message_data["data"].get("procedure_step_label")

                    if procedure_step_label in expected_procedure_steps:
                        received_procedure_steps.add(procedure_step_label)
                        print(f" Received procedure step: {procedure_step_label}")
                    else:
                        logger.warning(f"Unexpected procedure step: {procedure_step_label}")

                else:
                    # Put back the unwanted message for later processing
                    await test_instance.received_messages.put(last_message)
                    logger.info(f"Unwanted message requeued: {last_message_data['type']}")

            except asyncio.TimeoutError:
                logger.warning("Timeout while waiting for messages.")

            attempt += 1

        logger.info(f"Final received steps: {received_procedure_steps}")

        assert received_procedure_steps == expected_procedure_steps, (
            f"Missing procedure_step_labels: {expected_procedure_steps - received_procedure_steps}"
        )

    @pytest.mark.asyncio
    async  def test_show_scan_progress_page_command_issued(self, setup_pbs_job):
            """Test that a show_scan_progress_page command is issued to the console"""
            test_instance = setup_pbs_job
            expected_type = "show_scan_progress_page"

            # Clear old messages before starting the test
            #await clear_message_queue(test_instance.received_messages)
            
            # Wait for a message
            await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

            # Retrieve the message
            last_message = await test_instance.received_messages.get()
            last_message_data = json.loads(last_message.data.decode())

            logger.info(last_message_data["type"])

            # Store the message in the appropriate queue
            if last_message_data["type"] == expected_type:
                 print(f"{last_message_data['type']}")
            else:
               await test_instance.received_messages.put(last_message) 
            # Assertions
            assert last_message_data["type"] == expected_type
            #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
            assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async  def test_pbs_start_slot_scan_command_issued(self,setup_pbs_job):
        """Test that a pbs_start_slot_scan command is issued to the console"""
        test_instance = setup_pbs_job
        expected_type ="pbs_start_slot_scan"

        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_uwl_workitem_created_event_issued(self, setup_pbs_job):
        """Test that a uwl_workitem_created command is issued to the dicom bridge"""
        test_instance = setup_pbs_job
        expected_type ="uwl_workitem_created"


        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")

        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")

        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_pbs_slot_scan_started_event_issued(self,setup_pbs_job):
        """Test that a pbs_slot_scan_started event is issued to the scsnner"""
        test_instance = setup_pbs_job
        expected_type="pbs_slot_scan_started"

        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")

        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")

        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_update_uwl_workitem_state_command_issued(self, setup_pbs_job):
        """Test that a single received message is validated or queued for the next test case."""
        test_instance = setup_pbs_job
        expected_type = "update_uwl_workitem_state"

        # #  Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)
        
        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        #assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_scan_progress_page_commands_issued(self, setup_pbs_job):
        """Test that 'show_scan_progress_page' message is received and validated."""

        test_instance = setup_pbs_job
        expected_type = "show_scan_progress_page"

         #  Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_pbs_aoi_scan_started_event_issued(self, setup_pbs_job):
        """Test that a pbs_aoi_scan_started event is issued to the scanner"""
        test_instance = setup_pbs_job
        expected_type = "pbs_aoi_scan_started"

        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")

        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")

        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id


    @pytest.mark.asyncio
    async def test_show_pbs_slot_scan_started_info_issued_command(self, setup_pbs_job):
        """Test that a show_pbs_slot_scan_started_info command is issued to the scanner"""

        test_instance = setup_pbs_job
        expected_type = "show_pbs_slot_scan_started_info"

        # # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id


       
    @pytest.mark.asyncio
    async def test_show_monolayer_started_info_issued_command(self, setup_pbs_job):
        """Test that a show_monolayer_started_info command is issued to the console"""
        test_instance = setup_pbs_job
        expected_type = "show_monolayer_started_info"

        #  Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)
        
        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message)
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_start_pbs_reconstruction_issued_command_issued(self, setup_pbs_job):
        """Test that a start_pbs_reconstruction command is issued to the RECONSTRUCTION command"""
        test_instance = setup_pbs_job
        expected_type = "start_pbs_reconstruction"

        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message)
            # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_raw_fov_captured_event_issued(self, setup_pbs_job):
        """Test that a raw_fov_captured event is issued from the scanner"""

        test_instance = setup_pbs_job
        expected_type = "raw_fov_captured"

        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")

        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")

        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    # @pytest.mark.asyncio
    # async def test_reconstruct_pbs_fov_command(self, setup_pbs_job):
    #     """Verify FOV reconstruction commands using asyncio.gather()"""
    #     test_instance = setup_pbs_job
    #     expected_count = 1627
    #     timeout = 600  #Corrected to 10 minutes
    #
    #     counts = {
    #         "raw": 0,
    #         "recon": 0
    #     }
    #
    #     start_time = asyncio.get_event_loop().time()
    #     lock = asyncio.Lock()
    #
    #     async def process_messages():
    #         nonlocal counts
    #
    #         while True:
    #             elapsed = asyncio.get_event_loop().time() - start_time
    #             if elapsed >= timeout:
    #                 break
    #
    #             try:
    #                 msg = await asyncio.wait_for(
    #                     test_instance.received_messages.get(),
    #                     timeout=timeout - elapsed
    #                 )
    #                 data = json.loads(msg.data.decode())
    #
    #                 async with lock:
    #                     if data["type"] == "raw_fov_captured":
    #                         counts["raw"] += 1
    #                         assert data["data"]["current_fov_count"] == counts["raw"]
    #                         assert float(data["data"]["estimated_total_fov_count"]) == 1628.0
    #
    #                     elif data["type"] == "reconstruct_pbs_fov":
    #                         counts["recon"] += 1
    #                         assert data["data"]["current_fov_count"] == recon_count # Fixed to integer
    #                         assert float(data["data"]["estimated_total_fov_count"]) == 1628.0
    #
    #                     else:
    #                         logger.warning(f"Unexpected message type: {data['type']}")
    #                         # Avoid re-enqueueing to prevent loops
    #
    #                     logger.info(f"Progress: Raw {counts['raw']}/1628, Recon {counts['recon']}/1628")
    #
    #                 if counts["raw"] >= expected_count and counts["recon"] >= expected_count:
    #                     break
    #
    #             except asyncio.TimeoutError:
    #                 break
    #
    #     processors = [process_messages(), process_messages()]
    #     await asyncio.gather(*processors)
    #
    #     assert counts["raw"] == expected_count, f"Missing {expected_count - counts['raw']} raw FOVs"
    #     assert counts["recon"] == expected_count, f"Missing {expected_count - counts['recon']} recon commands"

    @pytest.mark.asyncio
    async def test_update_uwl_workitem_state_commandss_issued(self, setup_pbs_job):
        """Test that a single received message is validated or queued for the next test case."""
        test_instance = setup_pbs_job
        expected_type = "update_uwl_workitem_state"

        # #  Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message)
            # Assertions
        assert last_message_data["type"] == expected_type

    @pytest.mark.asyncio
    async def test_reconstruct_pbs_fov_command(self, setup_pbs_job):
        """Verify FOV reconstruction commands"""
        test_instance = setup_pbs_job
        expected_count = 1628
        timeout = 600 # 10 minutes

        raw_count = 0
        recon_count = 0
        start_time = asyncio.get_event_loop().time()

        try:
            while raw_count < expected_count or recon_count < expected_count:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= timeout:
                    print("Test timeout reached")

                try:
                    msg = await asyncio.wait_for(
                        test_instance.received_messages.get(),
                        timeout=timeout - elapsed
                    )
                    print(f"msg received:{msg}\n")
                    data = json.loads(msg.data.decode())
                    print(f"data:{data}\n")

                    if data["type"] == "raw_fov_captured":
                        print(data["type"])
                        raw_count += 1
                        print(raw_count)
                        assert data["data"]["current_fov_count"] == raw_count
                        assert float(data["data"]["estimated_total_fov_count"]) == 1628.0
                    elif data["type"] == "reconstruct_pbs_fov":
                        print(data["type"])
                        recon_count += 1
                        print(recon_count)
                        assert data["data"]["current_fov_count"] == recon_count
                        assert float(data["data"]["estimated_total_fov_count"]) == 1628.0
                    else:
                        print(f"unexpected_message{data['type']}")
                        await test_instance.received_messages.put(msg)

                    logger.info(f"Progress: Raw {raw_count}/1628, Recon {recon_count}/1628")

                except asyncio.TimeoutError:
                    pytest.fail("Message timeout occurred")

        finally:
            assert raw_count == expected_count, f"Missing {expected_count - raw_count} raw FOVs"
            assert recon_count == expected_count, f"Missing {expected_count - recon_count} recon commands"

    # @pytest.mark.asyncio
    # async def test_reconstruct_pbs_fov_command(self, setup_pbs_job):
    #     """Verify FOV reconstruction commands"""
    #     test_instance = setup_pbs_job
    #     expected_count = 1628
    #     timeout = 0.2 # 10 minutes
    #
    #     raw_count = 0
    #     recon_count = 0
    #     start_time = asyncio.get_event_loop().time()
    #
    #     try:
    #         while raw_count < expected_count or recon_count < expected_count:
    #             elapsed = asyncio.get_event_loop().time() - start_time
    #             if elapsed >= timeout:
    #                 pytest.fail("Test timeout reached")
    #
    #             try:
    #                 msg = await asyncio.wait_for(
    #                     test_instance.received_messages.get(),
    #                     timeout=timeout - elapsed
    #                 )
    #                 data = json.loads(msg.data.decode())
    #
    #                 if data["type"] == "raw_fov_captured":
    #                     raw_count += 1
    #                     assert data["data"]["current_fov_count"] == raw_count
    #                     assert float(data["data"]["estimated_total_fov_count"]) == 1628.0
    #                 elif data["type"] == "reconstruct_pbs_fov":
    #                     recon_count += 1
    #                     assert data["data"]["current_fov_count"] == recon_count
    #                     assert float(data["data"]["estimated_total_fov_count"]) == 1628.0
    #                 else:
    #                     logger.debug(f"Ignored {data['type']}")
    #                     continue
    #
    #                 #logger.info(f"Progress: Raw {raw_count}/1628, Recon {recon_count}/1628")
    #                 print(f"Progress: Raw {raw_count}/1628, Recon {recon_count}/1628")
    #
    #             except asyncio.TimeoutError:
    #                 pytest.fail("Message timeout occurred")
    #
    #     finally:
    #         assert raw_count == expected_count, f"Missing {expected_count - raw_count} raw FOVs"
    #         assert recon_count == expected_count, f"Missing {expected_count - recon_count} recon commands"

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_pbs_aoi_scan_completed_event_issued(self, setup_pbs_job):
        """Test that a pbs_aoi_scan_completed event is issued from the scanner"""
        test_instance = setup_pbs_job

        expected_type = "pbs_aoi_scan_completed"

        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")

        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")

        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_complete_pbs_reconstruction_command_issued(self, setup_pbs_job):
        """Test that a complete_pbs_reconstruction command is issued to the reconstructor command"""

        test_instance = setup_pbs_job
        expected_type = "complete_pbs_reconstruction"

        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_pbs_slot_scan_status_command(self, setup_pbs_job):
        """Test that a show_pbs_slot_scan_status command is issued to the console"""

        test_instance = setup_pbs_job
        expected_type = "show_pbs_slot_scan_status"

        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"{last_message_data}\n")

        logger.info(last_message_data["type"])

        if last_message_data["type"] == expected_type:
            print(f"Excepted message Type:{last_message_data['type']}")
        else:
           print(f"Testcase received unexpected message: {last_message_data['type']}")

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_pbs_main_scan_summary_info_command(self, setup_pbs_job):
        """Test that a show_pbs_main_scan_summary_info command is issued to the console"""

        test_instance = setup_pbs_job
        expected_type = "show_pbs_main_scan_summary_info"
        
        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_eject_tray_commands_command(self, setup_pbs_job):
        """Test that a eject_tray command is issued to the console"""

        test_instance = setup_pbs_job
        expected_type = "eject_tray"
        
        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_tray_ejected_message_command(self, setup_pbs_job):
        """Test that a show_tray_ejected_message command is issued to the console"""

        test_instance = setup_pbs_job
        expected_type = "show_tray_ejected_message"

        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_pbs_aoi_reconstruction_completed_event_issued(self, setup_pbs_job):
        """Test that a pbs_aoi_reconstruction event is issued from the reconstructor"""

        test_instance = setup_pbs_job
        expected_type = "pbs_aoi_reconstruction"

        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")

        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")

        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_start_pbs_slot_analysis_command_issued(self, setup_pbs_job):
        """Test that a start_pbs_slot_analysis command is issued to the Analyser"""

        test_instance = setup_pbs_job
        expected_type = "start_pbs_slot_analysis"

        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_create_dicom_files_command_issued(self, setup_pbs_job):
        """Test that a create_dicom_files command is issued to the dicom convertor"""
        test_instance = setup_pbs_job
        expected_type = "create_dicom_files"

        #  Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_dicom_files_created_event_issued(self, setup_pbs_job):
        """Test that a dicom_files created event is issued from the dicom convertor"""

        test_instance = setup_pbs_job
        expected_type = "dicom_files_created"

        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")

        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")

        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_upload_dicom_object_command_issued(self, setup_pbs_job):
        """Test that a upload_dicom_object command is issued to the dicom bridge"""

        test_instance = setup_pbs_job
        expected_type = "upload_dicom_object"

        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_dicom_object_uploaded_event_issued(self, setup_pbs_job):
        """Test that a dicom_object_uploaded event is issued from the dicom bridge"""

        test_instance = setup_pbs_job
        expected_type = "dicom_object_uploaded"

        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")

        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")

        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id


    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_pbs_slot_analysis_completed_event_issued(self, setup_pbs_job):
        """Test that a pbs_slot_analysis event is issued from the analyser"""

        test_instance = setup_pbs_job
        expected_type = "pbs_slot_analysis"

        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")

        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")

        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_create_patch_annotation_dicom_files_command_issued(self, setup_pbs_job):
        """Test that a create_patch_annotation_dicom_files command is issued to the dicom convertor"""

        test_instance = setup_pbs_job
        expected_type = "create_patch_annotation_dicom_files"

        #  Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)
        
        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_report_initialized_command_issued(self, setup_pbs_job):
        """Test that a report_initialized command is issued to the cloud bridge"""

        test_instance = setup_pbs_job
        expected_type = "report_initialized"

        #  Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)
        
        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_patch_annotation_dicom_files_created_event_issued(self, setup_pbs_job):
        """Test that a patch_annotation_dicom_files_created event is issued from the dicom convertor"""

        test_instance = setup_pbs_job
        expected_type = "patch_annotation_dicom_files_created"

        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")

        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")

        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_create_dicom_files_for_initialize_and_analysis_data_command_issued(self, setup_pbs_job):
        """Test that a create_dicom_files command is issued to the dicom convertor"""

        test_instance = setup_pbs_job
        expected_type = "create_dicom_files_for_initialize_and_analysis"

        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_dicom_files_for_initialize_and_analysis_data_created_event_issued(self, setup_pbs_job):
        """Test that a dicom_files_for_initialize_and_analysis_data_created event issued from the dicom convertor"""

        test_instance = setup_pbs_job
        expected_type = "dicom_files_for_initialize_and_analysis_data_created"

        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")

        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")

        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_get_study_size_command_issued(self, setup_pbs_job):
        """Test that a get_study_size command is issued to the dicom convertor"""
        test_instance = setup_pbs_job
        expected_type = "get_study_size"

        #  Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)


        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_study_size_calculated_event_issued(self, setup_pbs_job):
        """Test that a study_size_calculated event is issued from the dicom convertor"""

        test_instance = setup_pbs_job
        expected_type = "study_size_calculated"

        # Clear old messages before starting the test
        # await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        print(f"Received message: {json.dumps(last_message_data, indent=2)}")

        print(f"last_message: {last_message}")
        print(f"last message received:{last_message_data}")

        print(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
            print(f"{last_message_data['type']}")
        else:
            await test_instance.received_messages.put(last_message_data)  # Store unwanted messages
        # Assertions
        assert last_message_data["type"] == expected_type
        # assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

        assert last_message_data["type"] == expected_type
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_ready_report_command_issued(self, setup_pbs_job):
        """Test that a ready_report command is issued to the cloud bridge"""

        test_instance = setup_pbs_job
        expected_type = "ready_report"

        # Clear old messages before starting the test
        #await clear_message_queue(test_instance.received_messages)

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        logger.info(last_message_data["type"])

        # Store the message in the appropriate queue
        if last_message_data["type"] == expected_type:
             print(f"{last_message_data['type']}")
        else:
           await test_instance.received_messages.put(last_message) 
        # Assertions
        assert last_message_data["type"] == expected_type
       #assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

       















































































