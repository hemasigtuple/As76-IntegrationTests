import asyncio
import json
import uuid
import pytest
from services.config import generate_orchestrator_command_start_pbs_job


@pytest.mark.asyncio
@pytest.mark.order(1)
async def test_eject_tray_command_issued(nats_client):
    received_messages = []
    trace_id = str(uuid.uuid4())

    async def handle_scanner_commands(msg):
        received_command = json.loads(msg.data.decode())
        received_messages.append(received_command)

    # Subscribe to NATS topic
    sub = await nats_client.subscribe("nila.garuda.scanner.command", cb=handle_scanner_commands)
    trigger_start_pbs_command=generate_orchestrator_command_start_pbs_job(correlation_id = None, trace_id=trace_id)

    # clear Messages
    received_messages.clear()


    await asyncio.sleep(10)

    print("Message", nats_client)
    assert len(received_messages) > 0, "No messages received by the scanner"
    assert received_messages[0]["type"] == "eject_tray"
    assert received_messages[0]["metadata"]["trace_id"] == trace_id, \
        f"Expected trace ID to be '{trace_id}', but got {received_messages[0]['metadata'].get('trace_id')}"
    assert trigger_start_pbs_command["id"] == received_messages[0]["metadata"]["correlation_id"]

    await sub.unsubscribe()

@pytest.mark.asyncio
@pytest.mark.order(2)
async def test_show_tray_ejecting_dialog_command_issued(nats_client):
    received_messages = []

    async def handle_console_commands(msg):
        received_command = json.loads(msg.data.decode())
        received_messages.append(received_command)
        print(received_messages)

    trace_id = str(uuid.uuid4())

    # Subscribe to NATS topic
    sub = await nats_client.subscribe("nila.console.commands", cb=handle_console_commands)

    await asyncio.sleep(20)
    assert len(received_messages) > 0, "No messages received by the scanner"
    assert received_messages[0]["type"] == "show_tray_ejecting_dialog"
    assert received_messages[0]["metadata"]["trace_id"] == trace_id, \
        f"Expected trace ID to be '{trace_id}', but got {received_messages[0]['metadata'].get('trace_id')}"

    await sub.unsubscribe()

@pytest.mark.skip
@pytest.mark.asyncio
@pytest.mark.order(3)
async def test_tray_ejected_scanner_event_issued(nats_client, shared_correlation_id):
    received_messages = []

    async def handle_scanner_events(msg):
        received_command = json.loads(msg.data.decode())
        received_messages.append(received_command)
        print(received_messages)

    trace_id = str(uuid.uuid4())
    correlation_id = shared_correlation_id["correlation_id"]  # Get correlation_id from previous test

    assert correlation_id is not None, "Correlation ID should be set from previous test"

    # Subscribe to NATS topic
    sub = await nats_client.subscribe("nila.garuda.scanner.event.>", cb=handle_scanner_events)

    #clear messages
    received_messages.clear()


    # Publish command with correlation_id
    trigger_start_pbs_command = generate_orchestrator_command_start_pbs_job(correlation_id, trace_id=trace_id)
    await nats_client.publish(
        "nila.device-job-orchestrator.commands",
        json.dumps(trigger_start_pbs_command).encode()
    )

    await asyncio.sleep(20)
    assert len(received_messages) > 0, "No messages received by the scanner"
    assert received_messages[0]["type"] == "tray_ejected"
    assert received_messages[0]["metadata"]["trace_id"] == trace_id, \
        f"Expected trace ID to be '{trace_id}', but got {received_messages[0]['metadata'].get('trace_id')}"
    # assert received_messages[0]["metadata"]["correlation_id"] == correlation_id, \
    #     f"Expected correlation ID to be '{correlation_id}', but got {received_messages[0]['metadata'].get('correlation_id')}"
    # Store the correlation_id from the received message for the next test
    shared_correlation_id["correlation_id"] = received_messages[0]["metadata"]["correlation_id"]

    await sub.unsubscribe()



@pytest.mark.asyncio
@pytest.mark.order(4)
async def test_show_place_tray_dialog_command_issued(nats_client, shared_correlation_id):
    received_messages = []
    trace_id = str(uuid.uuid4())

    async def handle_console_commands(msg):
        received_command = json.loads(msg.data.decode())
        received_messages.append(received_command)
        print("Received:", received_command)

    # Get correlation ID from previous test
    correlation_id = shared_correlation_id["correlation_id"]
    assert correlation_id is not None, "Correlation ID not set by previous tests"

    # Subscribe to console commands
    sub = await nats_client.subscribe("nila.console.commands", cb=handle_console_commands)
    received_messages.clear()

    # Publish command with correlation_id
    trigger_start_pbs_command = generate_orchestrator_command_start_pbs_job(correlation_id, trace_id=trace_id)
    await nats_client.publish(
        "nila.device-job-orchestrator.commands",
        json.dumps(trigger_start_pbs_command).encode()
    )
    # Wait for message with timeout
    start_time = asyncio.get_event_loop().time()
    timeout = 20  # seconds
    while True:
        if any(msg.get("type") == "show_place_tray_dialog" for msg in received_messages):
            break
        if (asyncio.get_event_loop().time() - start_time) > timeout:
            assert False, "Timed out waiting for place tray dialog"
        await asyncio.sleep(0.5)

    # Find the target message
    place_tray_msg = next(msg for msg in received_messages if msg.get("type") == "show_place_tray_dialog")

    # Validate message
    print(correlation_id)
    assert received_messages[0]["metadata"]["trace_id"] == trace_id, f"Expected trace ID to be '{trace_id}', but got {received_messages[0]['metadata'].get('trace_id')}"
    #assert place_tray_msg["metadata"]["correlation_id"] == correlation_id

    # Store the correlation_id from the received message for the next test
    assert trigger_start_pbs_command["id"]==received_messages[0]["metadata"]["correlation_id"]

    await sub.unsubscribe()