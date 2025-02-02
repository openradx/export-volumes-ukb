import hashlib
import re
import string
from typing import Any


def is_falsy(value: str) -> bool:
    return value.lower() in ["false", "0", "n", "no", "off"]


def is_truthy(value: str) -> bool:
    return value.lower() in ["true", "1", "y", "yes", "on"]


def parse_int(value: Any) -> int:
    try:
        return int(value)
    except (ValueError, TypeError):
        return -1


def sanitize_filename(name):
    # Define a regular expression pattern for illegal characters
    pattern = r'[<>:"/\\|?*\x00-\x1F]'
    # Replace illegal characters with an underscore
    sanitized_name = re.sub(pattern, "_", name)
    # Trim leading and trailing whitespaces
    sanitized_name = sanitized_name.strip()
    # Trim trailing dots
    sanitized_name = sanitized_name.rstrip(".")
    # Replace any remaining whitespace with a dash
    sanitized_name = re.sub(r"\s+", "-", sanitized_name)
    # Ensure the name is not empty or a reserved name
    reserved_names = {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "COM1",
        "COM2",
        "COM3",
        "COM4",
        "COM5",
        "COM6",
        "COM7",
        "COM8",
        "COM9",
        "LPT1",
        "LPT2",
        "LPT3",
        "LPT4",
        "LPT5",
        "LPT6",
        "LPT7",
        "LPT8",
        "LPT9",
    }
    if not sanitized_name or sanitized_name.upper() in reserved_names:
        sanitized_name = "default_name"
    return sanitized_name


BASE62_ALPHABET = string.digits + string.ascii_letters  # 0-9, A-Z, a-z


def hash_date_base62(date_str: str, length: int = 10) -> str:
    hash_obj = hashlib.sha256(date_str.encode()).digest()
    num = int.from_bytes(hash_obj, "big")
    base62 = []
    while num > 0:
        num, rem = divmod(num, 62)
        base62.append(BASE62_ALPHABET[rem])
    return "".join(base62)[:length]
