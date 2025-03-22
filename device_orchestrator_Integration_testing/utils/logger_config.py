import logging
import os
from datetime import datetime

# Always create logs at the project root level
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))  # Get current file directory (utils)
PROJECT_DIR = os.path.dirname(ROOT_DIR)  # Go one level up (project root)
LOG_DIR = os.path.join(PROJECT_DIR, "logs")

# Create logs directory if not exists
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Log file with timestamp
log_file = os.path.join(LOG_DIR, f"test_log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode="w"),  # Save logs to file
        #logging.StreamHandler()  # Show logs on console
    ],
)

def get_logger(name):
    """Return a module-specific logger."""
    return logging.getLogger(name)
