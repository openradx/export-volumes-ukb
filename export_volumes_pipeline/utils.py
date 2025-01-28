import re


def is_falsy(value: str) -> bool:
    return value.lower() in ["false", "0", "n", "no", "off"]


def is_truthy(value: str) -> bool:
    return value.lower() in ["true", "1", "y", "yes", "on"]


def sanitize_filename(name):
    # Define a regular expression pattern for illegal characters
    pattern = r'[<>:"/\\|?*\x00-\x1F]'
    # Replace illegal characters with an underscore
    sanitized_name = re.sub(pattern, "_", name)
    # Trim leading and trailing whitespace
    sanitized_name = sanitized_name.strip()
    # Ensure the name is not empty
    if not sanitized_name:
        sanitized_name = "default_name"
    return sanitized_name
