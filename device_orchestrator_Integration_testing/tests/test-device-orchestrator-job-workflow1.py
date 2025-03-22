import json
import os
import uuid
import asyncio
from datetime import datetime, timezone
import pytest
from dotenv import load_dotenv

from tests.conftest import logger

# Load environment variables from .env file
load_dotenv()

from nats.aio.client import Client as NATS

SUBJECTS = {
    "scanner_commands": "nila.garuda.scanner.command",
    "orchestrator_commands": "nila.device-job-orchestrator.commands",
    "console_commands": "nila.console.commands",
    "scanner_events": "nila.garuda.scanner.event.>",

}


def get_nats_url():
    return os.getenv("NATS_SERVER_URL", "nats://0.0.0.0:4222")


def get_subject(key):
    return SUBJECTS[key]

@pytest.fixture(scope="class")
async def nats_connection():
    """Fixture to provide a single NATS connection for the entire test class"""
    nc = NATS()
    await nc.connect(servers=[get_nats_url()])
    print(f"Connected to {get_nats_url()}")
    yield nc
    await nc.drain()
    await nc.close()

@pytest.fixture(scope="class")
async def setup_pbs_job(nats_connection):
    """Fixture to start the PBS job once before all test cases"""
    test_instance = TestPbsJobWorkflow()
    test_instance._nc = nats_connection
    test_instance.received_messages = asyncio.Queue()
    test_instance._trace_id = str(uuid.uuid4())
    test_instance.message_received = asyncio.Event()

    # Subscribe to relevant subjects before sending the job
    await test_instance.listen_to_messages("scanner_commands")
    await test_instance.listen_to_messages("console_commands")
    # await test_instance.listen_to_messages("scanner_events")

    # Initiate PBS job once
    job_id, trace_id = await test_instance.initiate_start_pbs_job()

    test_instance._last_id = job_id
    test_instance._trace_id = trace_id

    return test_instance  # Return the test instance with state

class TestBase:
    """Base class for NATS integration tests"""

    async def listen_to_messages(self, subject_key):
        """Subscribe and listen to messages, blocking until a message is received"""
        subject = get_subject(subject_key)

        async def message_handler(msg):
            print(f"Received a message on {subject}: {msg.data.decode()}")
            await self.received_messages.put(msg)
            self.message_received.set()  # Signal message received

        await self._nc.subscribe(subject, cb=message_handler)
        await self._nc.flush()  # Ensure subscription is active
        print(f"Subscribed to {subject}...")

    # async def listen_to_messages(self, subject_keys):
    #     """Subscribe and listen to multiple messages, blocking until a message is received"""
    #     if not isinstance(subject_keys, list):
    #         subject_keys = [subject_keys]  # Convert to list if a single key is provided
    #
    #     async def message_handler(msg):
    #         print(f"Received a message on {msg.subject}: {msg.data.decode()}")
    #         await self.received_messages.put(msg)
    #         self.message_received.set()  # Signal message received
    #
    #     for subject_key in subject_keys:
    #         subject = get_subject(subject_key)
    #         await self._nc.subscribe(subject, cb=message_handler)
    #         print(f"Subscribed to {subject}...")
    #
    #     await self._nc.flush()  # Ensure all subscriptions are active

    async def initiate_start_pbs_job(self):
        """Publish the start_pbs_job command"""
        print("Publishing start_pbs_job command...")
        self._last_id = str(uuid.uuid4())
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
        print("Published start_pbs_job command...")
        await asyncio.sleep(0.1)  # Small delay to ensure publishing completes
        return self._last_id, self._trace_id

@pytest.mark.usefixtures("nats_connection")
class TestPbsJobWorkflow(TestBase):
    """Test cases for PBS Job Workflow"""

    @pytest.mark.asyncio
    async def test_eject_tray_command(self, setup_pbs_job):
        """Test that an eject_tray command is issued after starting a PBS job"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        # Verify the message
        assert last_message_data["type"] == "eject_tray"
        assert last_message_data["metadata"]["correlation_id"] == test_instance._last_id
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_tray_ejecting_dialog_command(self, setup_pbs_job):
        """Test that a show_tray_ejecting_dialog command is issued to the console"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve the message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "show_tray_ejecting_dialog"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Not relevant for Orchestrator Test")
    async def test_tray_ejected_scanner_event(self, setup_pbs_job):
        """Test that a tray_ejected event is published after tray ejection"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "tray_ejected"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_place_tray_dialog_command(self, setup_pbs_job):
        """Test that a show_place_tray_dialog command is issued to the console"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "show_place_tray_dialog"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id


    @pytest.mark.asyncio
    async def test_show_macro_scan_progress_screen_command_issued(self, setup_pbs_job):
        """Test that a show_macro_scan_progress_screen command is issued to the console"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())
        print(last_message_data["type"] )
        print(last_message_data["metadata"]["trace_id"])
        print(test_instance._trace_id)

        assert last_message_data["type"] == "show_macro_scan_progress_screen"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id


    @pytest.mark.asyncio
    async def test_show_getting_ready_for_scan_message_command_issued(self, setup_pbs_job):
        """Test that a show_getting_ready_for_scan_message is issued to the console"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "show_getting_ready_for_scan_message"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id


    @pytest.mark.asyncio
    async def test_start_macro_scan_command_issued(self, setup_pbs_job):
        """Test that a start_macro_scan command is issued to the console"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "start_macro_scan"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id


    @pytest.mark.asyncio
    async def test_show_macro_scan_in_progress_info_command_issued(self, setup_pbs_job):
        """Test that a show_macro_scan_in_progress_info command is issued to the console"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "show_macro_scan_in_progress_info"
        #assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_slot_macro_scan_completed_event_issued(self, setup_pbs_job):
        """Test that a slot_macro_scan_completed command is issued to the console"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "slot_macro_scan_completed"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_pbs_slides_details_entry_dialog_command_issued(self, setup_pbs_job):
        """Test that a show_pbs_slides_details_entry command is issued to the console"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "show_pbs_slides_details_entry_dialog"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id








