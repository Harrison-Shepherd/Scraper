import re

def sanitize_filename(filename):
    """
    Remove unwanted characters from the filename and format it consistently.
    """
    # Replace problematic characters and collapse multiple spaces
    filename = re.sub(r'[^\w\s]', '', filename)  # Remove non-alphanumeric characters except whitespace
    filename = re.sub(r'\s+', ' ', filename)     # Replace multiple spaces with a single space
    filename = filename.strip()                  # Remove leading/trailing spaces
    return filename
