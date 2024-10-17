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

    # Create handlers
    info_handler = logging.FileHandler(os.path.join(log_dir, 'info.log'), mode='w')
    error_handler = logging.FileHandler(os.path.join(log_dir, 'error.log'), mode='w')

    # Set logging levels
    info_handler.setLevel(logging.INFO)
    error_handler.setLevel(logging.ERROR)

    # Define formatters
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Assign formatter to handlers
    info_handler.setFormatter(formatter)
    error_handler.setFormatter(formatter)

    # Add handlers to respective loggers
    info_logger.addHandler(info_handler)
    error_logger.addHandler(error_handler)

    # Set levels for the loggers themselves
    info_logger.setLevel(logging.INFO)
    error_logger.setLevel(logging.ERROR)

    return info_logger, error_logger
