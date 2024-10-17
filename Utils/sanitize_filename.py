import re

def sanitize_filename(filename):
    """
    Remove unwanted characters from the filename, format it consistently,
    and preserve key terms like 'NZ', 'Mens', 'Womens', etc.
    """
    # Replace problematic characters and collapse multiple spaces
    filename = re.sub(r'[^\w\s]', '', filename)  # Remove non-alphanumeric characters except whitespace
    filename = re.sub(r'\s+', ' ', filename)     # Replace multiple spaces with a single space
    filename = filename.strip()                  # Remove leading/trailing spaces

    # Preserve 'NZ', 'Mens', 'Womens' capitalizations
    filename = re.sub(r'\bNz\b', 'NZ', filename)
    filename = re.sub(r'\bMens\b', 'Mens', filename)
    filename = re.sub(r'\bWomens\b', 'Womens', filename)

    return filename
