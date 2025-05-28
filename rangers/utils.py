__all__ = [
    "bytes_xor",
    "bytes_to_int",
    "bytes_to_uint",
    "int_to_bytes",
    "uint_to_bytes",
    "num_leading",
    "parse_index",
]


def bytes_xor(a: bytes, b: bytes) -> bytes:
    """
    Performs a byte-wise XOR operation between two byte sequences.

    Args:
        a (bytes): First byte sequence.
        b (bytes): Second byte sequence.

    Returns:
        bytes: Resulting byte sequence after XORing corresponding bytes.
    """
    return bytes(tuple(_a ^ _b for _a, _b in zip(a, b)))


def bytes_to_int(a: bytes) -> int:
    """
    Converts a byte sequence to a signed integer using little-endian byte order.

    Args:
        a (bytes): Byte sequence to convert.

    Returns:
        int: The corresponding signed integer.
    """
    return int.from_bytes(a, "little", signed=True)


def bytes_to_uint(a: bytes) -> int:
    """
    Converts a byte sequence to an unsigned integer using little-endian byte order.

    Args:
        a (bytes): Byte sequence to convert.

    Returns:
        int: The corresponding unsigned integer.
    """
    return int.from_bytes(a, "little", signed=False)


def int_to_bytes(a: int) -> bytes:
    """
    Converts a signed integer to a 4-byte little-endian byte sequence.

    Args:
        a (int): Signed integer to convert.

    Returns:
        bytes: The resulting 4-byte sequence.
    """
    return a.to_bytes(4, "little", signed=True)


def uint_to_bytes(a: int) -> bytes:
    """
    Converts an unsigned integer to a 4-byte little-endian byte sequence.

    Args:
        a (int): Unsigned integer to convert.

    Returns:
        bytes: The resulting 4-byte sequence.
    """
    return a.to_bytes(4, "little", signed=False)


def num_leading(s: str, c: str) -> int:
    """
    Counts the number of leading characters in a string that match a given character.

    Args:
        s (str): Input string.
        c (str): Character to count at the beginning of the string.

    Returns:
        int: Number of leading occurrences of the specified character.
    """
    result = 0
    for _c in s:
        if _c != c:
            break
        result += 1
    return result


def parse_index(s: str, strict: bool = False) -> tuple[str, int]:
    """
    Parses a string containing a name and an index in the format 'name[index]'.

    Args:
        s (str): The input string to parse.
        strict (bool): If True, raises ValueError on invalid index. If False, returns 0 on error.

    Returns:
        tuple[str, int]: A tuple containing the name and the parsed index.
    """
    name, index = s.strip().rstrip("]").split("[", 1)
    try:
        return name, int(index)
    except ValueError:
        if strict:
            raise
        return name, 0
