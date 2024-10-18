import logging
import os

# Ensure the Logs directory exists
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Logs")
os.makedirs(log_dir, exist_ok=True)

# Function to setup logging
def setup_logging():
    # Create loggers
    info_logger = logging.getLogger("info_logger")
    error_logger = logging.getLogger("error_logger")
    
    # Ensure logs are not propagated to the root logger
    info_logger.propagate = False
    error_logger.propagate = False

    # Set logging levels
    info_logger.setLevel(logging.INFO)
    error_logger.setLevel(logging.WARNING)

    # Create handlers
    info_handler = logging.FileHandler(os.path.join(log_dir, 'info.log'), mode='w')
    error_handler = logging.FileHandler(os.path.join(log_dir, 'error.log'), mode='w')

    # Define formatters
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Assign formatter to handlers
    info_handler.setFormatter(formatter)
    error_handler.setFormatter(formatter)

    # Set handler levels
    info_handler.setLevel(logging.DEBUG)  # Capture DEBUG and INFO levels
    error_handler.setLevel(logging.WARNING)  # Capture WARNING and above levels

    # Add handlers to respective loggers
    info_logger.addHandler(info_handler)
    error_logger.addHandler(error_handler)

    return info_logger, error_logger
