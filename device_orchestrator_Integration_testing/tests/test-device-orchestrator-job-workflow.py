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
    "dicom_bridge_commands": "nila.dicom_bridge.commands",
    "analyser_commands": "nila.garuda.analyser.command",
    "reconstructor_commands": "nila.garuda.recon.command",
    "dicom_converter_commands": "nila.dicom-converter.commands",
    "cloud_bridge_commands": "nila.cloud_bridge.commands",
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
    logger.info(f"Connected to {get_nats_url()}")
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
    await test_instance.listen_to_messages("dicom_bridge_commands")
    await test_instance.listen_to_messages("analyser_commands")
    await test_instance.listen_to_messages("reconstructor_commands")
    await test_instance.listen_to_messages("dicom_converter_commands")
    await test_instance.listen_to_messages("cloud_bridge_commands")

    # Initiate PBS job once
    job_id, trace_id = await test_instance.initiate_start_pbs_job()

    test_instance._last_id = job_id
    test_instance._trace_id = trace_id

    return test_instance  # Return the test instance with state

class TestBase:
    """Base class for NATS integration tests"""
    received_messages = None
    message_received = None
    _trace_id = None
    _nc = None


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

    async def initiate_start_pbs_job(self):
        """Publish the start_pbs_job command"""
        logger.info(f"Publishing start_pbs_job command...")
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
        logger.info(f"Published start_pbs_job command...{start_pbs_command}")
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

    @pytest.mark.asyncio
    async def test_show_initiating_scan_page_command_issued(self, setup_pbs_job):
        """Test that a show_initiating_scan_page_command is issued to the console"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "show_initiating_scan_page"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_create_uwl_workitem_command_issued_multiple_times(self, setup_pbs_job):
        """Test that the create_uwl_workitem command is issued exactly 8 times"""

        test_instance = setup_pbs_job
        count = 0
        last_message_data = None
        for _ in range(8):
            await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)
            last_message = await test_instance.received_messages.get()
            last_message_data = json.loads(last_message.data.decode())
            if last_message_data["type"] == "create_uwl_workitem":
                count += 1

        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id
        assert count == 8, f"Expected 8 'create_uwl_workitem' messages, but received {count}"

    @pytest.mark.asyncio
    async  def test_show_scan_progress_page_command_issued(self, setup_pbs_job):
            """Test that a show_scan_progress_page command is issued to the console"""
            test_instance = setup_pbs_job

            # Wait for a message
            await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

            # Retrieve and verify message
            last_message = await test_instance.received_messages.get()
            last_message_data = json.loads(last_message.data.decode())

            assert last_message_data["type"] == "show_scan_progress_page"
            assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async  def test_pbs_start_slot_scan_command_issued(self,setup_pbs_job):
        """Test that a pbs_start_slot_scan command is issued to the console"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "pbs_start_slot_scan"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_uwl_workitem_created_event_issued(self, setup_pbs_job):
        """Test that a uwl_workitem_created command is issued to the dicom bridge"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "uwl_workitem_created"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_pbs_slot_scan_started_event_issued(self,setup_pbs_job):
        """Test that a pbs_slot_scan_started event is issued to the scsnner"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "pbs_slot_scan_started"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_update_uwl_workitem_state_command_issued (self, setup_pbs_job):
        """Test that a update_uwl_workitem_state command is issued to the dicom bridge"""
        test_instance = setup_pbs_job
        count = 0

        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        for _ in range(1, 19):
            if last_message_data["type"] == "update_uwl_workitem_state":
                count += 1

        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id
        assert count > 0, f"Expected 18 'update_uwl_workitem_state' messages, but received {count}"

    @pytest.mark.asyncio
    async def test_show_scan_progress_page_commands_issued(self, setup_pbs_job):
        """Test that a show_scan_progress_page command is issued to the console"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "show_scan_progress_page"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    # @pytest.mark.asyncio
    # async def test_update_uwl_workitem_state_command_issued(self, setup_pbs_job):
    #     """Test that an update_uwl_workitem_state command is issued to the dicom bridge"""
    #
    #     test_instance = setup_pbs_job
    #     count = 0
    #
    #     for _ in range(1,20):
    #         await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)
    #
    #         last_message = await test_instance.received_messages.get()
    #         last_message_data = json.loads(last_message.data.decode())
    #
    #         if last_message_data["type"] == "update_uwl_workitem_state":
    #             print(last_message_data["type"])
    #             count += 1
    #
    #     print(f"Total 'update_uwl_workitem_state' messages received: {count}")
    #
    #     # Ensure exactly 18 messages are received
    #     #assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id
    #     assert count > 0, f"Expected 18 'update_uwl_workitem_state' messages, but received {count}"

    # @pytest.mark.asyncio
    # async def test_update_uwl_workitem_state_command_issued(self, setup_pbs_job):
    #     """Test that update_uwl_workitem_state commands are issued and updated correctly with valid status"""
    #
    #     test_instance = setup_pbs_job
    #     max_attempts = 25
    #     received_updates = set()
    #
    #     expected_updates = {
    #         ("PBS Scan", "IN PROGRESS"),
    #         ("PBS Scan", "COMPLETED"),
    #         ("PBS Monolayer AOI Analysis", "COMPLETED"),
    #         ("PBS Monolayer AOI Analysis", "IN PROGRESS"),
    #         ("PBS Monolayer AOI Analysis (failed)", "COMPLETED"),
    #         ("PBS Feathered Edge AOI Analysis", "IN PROGRESS"),
    #         ("PBS Feathered Edge AOI Analysis", "COMPLETED"),
    #         ("PBS Monolayer AOI Reconstruction", "IN PROGRESS"),
    #         ("PBS Monolayer AOI Reconstruction", "COMPLETED"),
    #         ("PBS Feathered Edge AOI Reconstruction", "IN PROGRESS"),
    #         ("PBS Feathered Edge AOI Reconstruction", "COMPLETED"),
    #         ("PBS DICOM Conversion", "IN PROGRESS"),
    #         ("Approved CBC Data Sent to P8000", "IN PROGRESS"),
    #         ("Approved CBC Data Sent to P8000", "COMPLETED"),
    #         ("Approved CBC Data Sent to P8000", "CANCELLED"),
    #     }
    #
    #     for _ in range(max_attempts):
    #         try:
    #             await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)
    #         except asyncio.TimeoutError:
    #             break  # Exit loop if no new messages arrive within timeout
    #         test_instance.message_received.clear()  # Reset event for next message
    #
    #         last_message = await test_instance.received_messages.get()
    #         last_message_data = json.loads(last_message.data.decode())
    #
    #         # Uncomment if trace_id validation is needed
    #         if last_message_data["type"] == "update_uwl_workitem_state":
    #             procedure_step_label = last_message_data.get("procedure_step_label", "")
    #             status = last_message_data.get("status", "")
    #             received_updates.add((procedure_step_label, status))
    #         print(received_updates)
    #
    #     print(f"Total 'update_uwl_workitem_state' messages received: {len(received_updates)}")
    #     print(f"Received updates: {received_updates}")
    #
    #     assert received_updates, "No valid UWL work item updates were received."
    #     assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id
    #
    #     # missing_updates = expected_updates - received_updates
    #     # assert not missing_updates, f"Missing expected updates: {missing_updates}"
    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_pbs_aoi_scan_started_event_issued(self, setup_pbs_job):
        """Test that a pbs_aoi_scan_started event is issued to the scanner"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "pbs_aoi_scan_started"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_pbs_slot_scan_started_info_issued_command(self, setup_pbs_job):
        """Test that a show_pbs_slot_scan_started_info command is issued to the scanner"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "show_pbs_slot_scan_started_info"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id


    @pytest.mark.asyncio
    async def test_show_monolayer_started_info_issued_command(self, setup_pbs_job):
        """Test that a show_monolayer_started_info command is issued to the console"""
        test_instance = setup_pbs_job
        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "show_monolayer_started_info"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_start_pbs_reconstruction_issued_command_issued(self, setup_pbs_job):
        """Test that a start_pbs_reconstruction command is issued to the RECONSTRUCTION command"""
        test_instance = setup_pbs_job
        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "start_pbs_reconstruction"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id


    @pytest.mark.asyncio
    async def test_update_uwl_workitem_state_commands_issued(self, setup_pbs_job):
        """Test that a update_uwl_workitem_state command is issued to the dicom bridge"""
        test_instance = setup_pbs_job
        count = 0
        last_message_data = None

        for _ in range(1, 19):
            await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)
            last_message = await test_instance.received_messages.get()
            last_message_data = json.loads(last_message.data.decode())

            if last_message_data["type"] == "update_uwl_workitem_state":
                    count += 1

        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id
        assert count > 0, f"Expected 18 'update_uwl_workitem_state' messages, but received {count}"

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_uwl_workitem_state_updated_event_issued(self, setup_pbs_job):
        """Test that a uwl_workitem_state_updated event is issued to the dicom bridge"""
        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "uwl_workitem_state_updated"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_raw_fov_captured_event_issued(self, setup_pbs_job):
        """Test that a raw_fov_captured event is issued from the scanner"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "raw_fov_captured"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    # @pytest.mark.asyncio
    # async def test_reconstruct_pbs_fov_issued_command_issued(self, setup_pbs_job):
    #     """Test that a reconstruct_pbs_fov_issued command is issued to the reconstructor"""
    #
    #     test_instance = setup_pbs_job
    #
    #     count = 0
    #     # Wait for a message
    #     await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)
    #
    #     # Retrieve and verify message
    #     last_message = await test_instance.received_messages.get()
    #     last_message_data = json.loads(last_message.data.decode())
    #     for _ in range(1, 1628):
    #
    #         if  last_message_data["type"] == "reconstruct_pbs_fov":
    #             count += 1
    #
    #     assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id
    #     assert last_message_data["type"] == "reconstruct_pbs_fov"
    #     assert count > 0, f"Expected 50 'update_uwl_workitem_state' messages, but received {count}"

    @pytest.mark.asyncio
    async def test_reconstruct_pbs_fov_issued_command_issued(self,setup_pbs_job):
        """Test that a reconstruct_pbs_fov_issued command is issued to the reconstructor"""

        test_instance = setup_pbs_job
        count = 0
        last_message_data =None

        from collections import defaultdict

        messages_test = defaultdict(int)

        for _ in range(1, 1629):
            # Wait for a message
            await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

            # Retrieve and verify messages
            last_message = await test_instance.received_messages.get()
            last_message_data = json.loads(last_message.data.decode())
            messages_test[last_message_data["type"]] += 1
            if last_message_data["type"] == "reconstruct_pbs_fov":
                count += 1
            else:
                print(last_message_data["type"])
                
        print(messages_test)
        print(f"Received {count}/{1628} 'reconstruct_pbs_fov' messages")
        print(f"Processing message: {last_message_data.get('type')}")
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id
        assert count == 1628, f"Expected 1628 'reconstruct_pbs_fov' messages, but received {count}"

    @pytest.mark.asyncio
    async def test_update_uwl_workitem_state_commandsss_issued(self, setup_pbs_job):
        """Test that a update_uwl_workitem_state command is issued to the dicom bridge"""
        test_instance = setup_pbs_job
        count = 0

        for _ in range(1, 19):
            await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)
            last_message = await test_instance.received_messages.get()
            last_message_data = json.loads(last_message.data.decode())

            if last_message_data["type"] == "update_uwl_workitem_state":
                count += 1

        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id
        assert count > 0, f"Expected 18 'update_uwl_workitem_state' messages, but received {count}"

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_pbs_aoi_scan_completed_event_issued(self, setup_pbs_job):
        """Test that a pbs_aoi_scan_completed event is issued from the scanner"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "pbs_aoi_scan_completed"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    # @pytest.mark.asyncio
    # async def test_update_uwl_workitem_state_commandss_issued(self, setup_pbs_job):
    #     """Test that a update_uwl_workitem_state command is issued to the dicom bridge"""
    #     test_instance = setup_pbs_job
    #     count = 0
    #     await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)
    #     last_message = await test_instance.received_messages.get()
    #     last_message_data = json.loads(last_message.data.decode())
    #     for _ in range(1, 19):
    #
    #         if last_message_data["type"] == "update_uwl_workitem_state":
    #             count += 1
    #
    #     assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id
    #     assert count > 0, f"Expected 18 'update_uwl_workitem_state' messages, but received {count}"

    @pytest.mark.asyncio
    async def test_complete_pbs_reconstruction_command_issued(self, setup_pbs_job):
        """Test that a complete_pbs_reconstruction command is issued to the reconstructor command"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "complete_pbs_reconstruction"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_pbs_slot_scan_status_command(self, setup_pbs_job):
        """Test that a show_pbs_slot_scan_status command is issued to the console"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "show_pbs_slot_scan_status"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_pbs_main_scan_summary_info_command(self, setup_pbs_job):
        """Test that a show_pbs_main_scan_summary_info command is issued to the console"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "show_pbs_main_scan_summary_info"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_eject_tray_commands_command(self, setup_pbs_job):
        """Test that a show_pbs_main_scan_summary_info command is issued to the console"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "eject_tray"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_show_tray_ejected_message_command(self, setup_pbs_job):
        """Test that a show_tray_ejected_message command is issued to the console"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "show_tray_ejected"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_pbs_aoi_reconstruction_completed_event_issued(self, setup_pbs_job):
        """Test that a pbs_aoi_reconstruction event is issued from the reconstructor"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "pbs_aoi_reconstruction"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_start_pbs_slot_analysis_command_issued(self, setup_pbs_job):
        """Test that a start_pbs_slot_analysis command is issued to the Analyser"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "start_pbs_slot_analysis"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_create_dicom_files_command_issued(self, setup_pbs_job):
        """Test that a create_dicom_files command is issued to the dicom convertor"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "create_dicom_files"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_dicom_files_created_event_issued(self, setup_pbs_job):
        """Test that a dicom_files created event is issued from the dicom convertor"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "dicom_files_created"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_upload_dicom_object_command_issued(self, setup_pbs_job):
        """Test that a upload_dicom_object command is issued to the dicom bridge"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "upload_dicom_object"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_dicom_object_uploaded_event_issued(self, setup_pbs_job):
        """Test that a dicom_object_uploaded event is issued from the dicom bridge"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "dicom_object_uploaded"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_pbs_slot_analysis_completed_event_issued(self, setup_pbs_job):
        """Test that a pbs_slot_analysis event is issued from the analyser"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "pbs_slot_analysis"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_create_patch_annotation_dicom_files_command_issued(self, setup_pbs_job):
        """Test that a create_patch_annotation_dicom_files command is issued to the dicom convertor"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "create_patch_annotation_dicom_files"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_report_initialized_command_issued(self, setup_pbs_job):
        """Test that a report_initialized command is issued to the cloud bridge"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "report_initialized"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_patch_annotation_dicom_files_created_event_issued(self, setup_pbs_job):
        """Test that a patch_annotation_dicom_files_created event is issued from the dicom convertor"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "patch_annotation_dicom_files_created"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_create_dicom_files_for_initialize_and_analysis_data_command_issued(self, setup_pbs_job):
        """Test that a create_dicom_files command is issued to the dicom convertor"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "create_dicom_files_for_initialize_and_analysis_data"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_dicom_files_for_initialize_and_analysis_data_created_event_issued(self, setup_pbs_job):
        """Test that a dicom_files_for_initialize_and_analysis_data_created event issued from the dicom convertor"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "dicom_files_for_initialize_and_analysis_data_created "
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_get_study_size_command_issued(self, setup_pbs_job):
        """Test that a get_study_size command is issued to the dicom convertor"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "get_study_size "
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.skip
    @pytest.mark.asyncio
    async def test_study_size_calculated_event_issued(self, setup_pbs_job):
        """Test that a study_size calculated event is issued from the dicom convertor"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "study_size_calculated "
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

    @pytest.mark.asyncio
    async def test_ready_report_command_issued(self, setup_pbs_job):
        """Test that a ready_report command is issued to the cloud bridge"""

        test_instance = setup_pbs_job

        # Wait for a message
        await asyncio.wait_for(test_instance.message_received.wait(), timeout=5)

        # Retrieve and verify message
        last_message = await test_instance.received_messages.get()
        last_message_data = json.loads(last_message.data.decode())

        assert last_message_data["type"] == "ready_report"
        assert last_message_data["metadata"]["trace_id"] == test_instance._trace_id

















































































