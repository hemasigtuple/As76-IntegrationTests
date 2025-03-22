import uuid
from datetime import datetime


def generate_orchestrator_command():
    """Generate a new ORCHESTRATOR_COMMAND_START_PBS_JOB with a runtime UUID"""
    trace_id = str(uuid.uuid4())  # Generate trace ID
    command_id = str(uuid.uuid4())  # Generate unique command ID

    return {
        "id": command_id,  # Unique ID for the command
        "type": "start_pbs_job",
        "version": 1,
        "data": {
            "tray_type": "all_pbs",
            "scanned_by": "",
            "workflow_mode": "manual",
            "timezone": ""
        },
        "metadata": {
            "correlation_id": "",
            "timestamp": datetime.utcnow().isoformat() + "Z",  # Current UTC timestamp
            "trace_id": trace_id  # Attach generated trace ID
        }
    }


# Example usage:
command = generate_orchestrator_command()
print(command)
