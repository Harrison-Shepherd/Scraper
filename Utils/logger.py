import logging
import os

# Ensure the Logs directory exists
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Logs")
os.makedirs(log_dir, exist_ok=True)

# Function to configure logging
def setup_logging(log_filename):
    log_path = os.path.join(log_dir, log_filename)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='w'  # Overwrites the log file each time
    )
    return log_path
