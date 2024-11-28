def is_falsy(value: str) -> bool:
    return value.lower() in ["false", "0", "n", "no", "off"]


def is_truthy(value: str) -> bool:
    return value.lower() in ["true", "1", "y", "yes", "on"]
