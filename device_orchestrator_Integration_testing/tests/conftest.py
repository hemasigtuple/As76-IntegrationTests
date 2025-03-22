import asyncio
import os
import uuid

import pytest
from nats.aio.client import Client as NATS
import logging
import json
import pytest_asyncio
from services.nats_connection import NATSConnection
from utils.logger_config import get_logger
from services.config import generate_orchestrator_command_start_pbs_job, correlation_id

logger = get_logger("device-orchestrator-job-integration-testing")

@pytest_asyncio.fixture(scope="module")
def event_loop():
    """Create a new event loop for async tests."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)  # Ensure the loop is set for asyncio tasks
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def nats_client():
    """Fixture to create and close a NATS connection."""
    nc = NATS()
    await nc.connect("nats://192.168.0.58:4222")  # Change if needed
    yield nc
    await nc.close()

@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    """Set logger dynamically with test case name."""
    module_name = item.module.__name__ if item.module else "unknown_module"
    test_name = item.nodeid.split("::")[-1]  # Extract test function name

    global logger
    logger = get_logger(f"{module_name}.{test_name}")  # Module.TestName


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logstart(nodeid):
    """Logs test start"""
    logger.info(f"🔹 STARTING TEST: {nodeid}")

@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logreport(report):
    """Logs test results"""
    if report.when == "call":  # Log final outcome
        if report.passed:

            logger.info(f"✅ PASSED: {report.nodeid}\n")

        elif report.failed:

            logger.error(f"❌ FAILED: {report.nodeid}\n{report.longrepr}\n")
            logger.info(f"============================================================================================\n")

@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logfinish(nodeid):
    """Logs test completion"""
    logger.info(f"🔸 FINISHED TEST: {nodeid}\n")
    logger.info(f"============================================================================================\n")






