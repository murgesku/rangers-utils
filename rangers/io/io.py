__all__ = [
    "write_bool",
    "write_byte",
    "write_word",
    "write_int",
    "write_uint",
    "write_single",
    "write_double",
    "write_widestr",
    "write_struct",
    "read_bool",
    "read_byte",
    "read_word",
    "read_int",
    "read_uint",
    "read_single",
    "read_double",
    "read_widestr",
    "read_struct",
    "cipher_xor",
    "zl_compress",
    "zl_decompress",
    "TypedStruct",
]

import math
import zlib
from collections import namedtuple
from io import SEEK_CUR, BytesIO
from struct import pack, unpack
from typing import (
    Any,
    BinaryIO,
    Callable,
    Generator,
    Literal,
    NamedTuple,
    TypeAlias,
)

ZLFormats: TypeAlias = Literal["ZL01", "ZL02", "ZL03"]


def write_bool(dst: BinaryIO, val: bool) -> None:
    """Write a boolean value as a single byte (0 or 1) to a binary stream."""
    dst.write(pack("<B", int(val)))


def write_byte(dst: BinaryIO, val: int) -> None:
    """Write an unsigned 8-bit integer to a binary stream."""
    assert 0 <= val < 256
    dst.write(pack("<B", val))


def write_word(dst: BinaryIO, val: int) -> None:
    """Write an unsigned 16-bit integer to a binary stream."""
    assert 0 <= val < 65536
    dst.write(pack("<H", val))


def write_int(dst: BinaryIO, val: int) -> None:
    """Write a signed 32-bit integer to a binary stream."""
    assert -2147483648 <= val < 2147483648
    dst.write(pack("<i", val))


def write_uint(dst: BinaryIO, val: int) -> None:
    """Write an unsigned 32-bit integer to a binary stream."""
    assert 0 <= val < 4294967296
    dst.write(pack("<I", val))


def write_single(dst: BinaryIO, val: float) -> None:
    """Write a 32-bit floating point number to a binary stream."""
    assert 1.175494351e-38 <= val <= 3.402823466e38
    dst.write(pack("<f", val))


def write_double(dst: BinaryIO, val: float) -> None:
    """Write a 64-bit floating point number to a binary stream."""
    assert 2.2250738585072014e-308 <= val <= 1.7976931348623158e308
    dst.write(pack("<d", val))


def write_widestr(dst: BinaryIO, val: str) -> None:
    """Write a UTF-16LE encoded null-terminated string to a binary stream."""
    dst.write(val.encode("utf-16le"))
    dst.write(b"\x00\x00")


def write_struct(dst: BinaryIO, val: NamedTuple) -> None:
    """Write a custom typed struct to a binary stream using the cached schema."""
    structname = val.__class__.__name__
    if structname in TypedStruct.structs_cache:
        TypedStruct.structs_cache[structname].write(dst, val)
        return
    return NotImplemented  # type: ignore


def read_bool(src: BinaryIO) -> bool:
    """Read a boolean value (1 byte) from a binary stream."""
    return unpack("<B", src.read(1))[0] > 0


def read_byte(src: BinaryIO) -> int:
    """Read an unsigned 8-bit integer from a binary stream."""
    return unpack("<B", src.read(1))[0]


def read_word(src: BinaryIO) -> int:
    """Read an unsigned 16-bit integer from a binary stream."""
    return unpack("<H", src.read(2))[0]


def read_int(src: BinaryIO) -> int:
    """Read a signed 32-bit integer from a binary stream."""
    return unpack("<i", src.read(4))[0]


def read_uint(src: BinaryIO) -> int:
    """Read an unsigned 32-bit integer from a binary stream."""
    return unpack("<I", src.read(4))[0]


def read_single(src: BinaryIO) -> float:
    """Read a 32-bit floating point number from a binary stream."""
    return unpack("<f", src.read(4))[0]


def read_double(src: BinaryIO) -> float:
    """Read a 64-bit floating point number from a binary stream."""
    return unpack("<d", src.read(8))[0]


def read_widestr(src: BinaryIO) -> str:
    """Read a UTF-16LE encoded null-terminated string from a binary stream."""
    start = src.tell()
    size = 0
    while True:
        c = src.read(2)
        if len(c) < 2:
            return ""
        elif c == b"\x00\x00":
            size = src.tell() - start - 2
            break
        else:
            continue
    src.seek(start)
    s = src.read(size).decode("utf-16le")
    src.seek(2, SEEK_CUR)
    return s


def read_struct(src: BinaryIO, t: "TypedStruct") -> NamedTuple:
    """Read a custom typed struct from a binary stream using the given TypedStruct schema."""
    return t.read(src)


def zl_compress(data: bytes | bytearray | memoryview, fmt: ZLFormats) -> bytes:
    """
    Compress data using a custom 'ZL' format.

    Supported formats:
    - ZL01: single compressed block.
    - ZL02: multiple compressed 64KB chunks.
    - ZL03: similar to ZL02 but with chunk count.

    Args:
        data: Raw input bytes.
        fmt: Compression format identifier.

    Returns:
        Compressed data as bytes.
    """
    size = len(data)
    wrapper = BytesIO(data)

    result = BytesIO()
    match fmt:
        case "ZL01":
            result.write(b"ZL01")
            write_uint(result, size + 8)
            result.write(zlib.compress(data, level=9))
        case "ZL02":
            while wrapper.tell() < size:
                content = wrapper.read(65536)
                chunk = zlib.compress(content, level=9)
                write_uint(result, len(chunk) + 8)
                result.write(b"ZL02")
                write_uint(result, len(content))
                result.write(chunk)
        case "ZL03":
            numchunks = math.floor(size / 65000)
            result.write(b"ZL03")
            write_int(result, numchunks)
            for _ in range(numchunks):
                chunk = zlib.compress(wrapper.read(65000), level=9)
                write_uint(result, len(chunk))
                result.write(chunk)
        case _:
            raise ValueError("Unknown compress algorithm")

    return result.getvalue()


def zl_decompress(data: bytes | bytearray | memoryview, fmt: ZLFormats) -> bytes:
    """
    Decompress data using a custom 'ZL' format.

    Args:
        data: Compressed input bytes.
        fmt: Compression format identifier.

    Returns:
        Decompressed data as bytearray.
    """
    size = len(data)
    wrapper = BytesIO(data)

    result = BytesIO()
    match fmt:
        case "ZL01":
            if (magic := wrapper.read(4)) != b"ZL01":
                raise ValueError(f"Invalid magic bytes for ZL01: {magic}")
            bufsize = read_uint(wrapper)
            result.write(zlib.decompress(wrapper.read(), bufsize))
        case "ZL02":
            while wrapper.tell() < size:
                chunksize = read_uint(wrapper)
                if (magic := wrapper.read(4)) != b"ZL02":
                    raise ValueError(f"Invalid magic bytes for ZL02: {magic}")
                bufsize = read_uint(wrapper)
                result.write(zlib.decompress(wrapper.read(chunksize - 8), bufsize))
        case "ZL03":
            if (magic := wrapper.read(4)) != b"ZL03":
                raise ValueError(f"Invalid magic bytes for ZL03: {magic}")
            numchunks = read_int(wrapper)
            for _ in range(numchunks):
                chunksize = read_uint(wrapper)
                result.write(zlib.decompress(wrapper.read(chunksize), 65000))
        case _:
            raise ValueError("Unknown compress algorithm")

    return result.getvalue()


def _rand31pm(seed: int) -> Generator[int, None, None]:
    """Generate an infinite pseudo-random sequence using Park-Miller 31-bit PRNG."""
    while True:
        hi, lo = divmod(seed, 0x1F31D)
        seed = lo * 0x41A7 - hi * 0xB14
        if seed < 1:
            seed += 0x7FFFFFFF
        yield seed - 1


def cipher_xor(data: bytearray | memoryview, key: int):
    """
    Apply XOR-based stream cipher to data using a Park-Miller pseudo-random generator.

    Modifies the input data in place.
    """
    gen = _rand31pm(key)
    for i in range(len(data)):
        data[i] = data[i] ^ (next(gen) & 255)


class TypedStruct:
    """
    A schema for reading and writing structured binary data using named fields.

    Attributes:
        structs_cache: Cache of defined TypedStructs by name.
        io_map: Mapping of primitive types to their read/write functions.
    """

    structs_cache: dict[str, "TypedStruct"] = {}
    io_map: dict[
        str, tuple[Callable[[BinaryIO], Any], Callable[[BinaryIO, Any], None]]
    ] = {
        "bool": (read_bool, write_bool),
        "byte": (read_byte, write_byte),
        "word": (read_word, write_word),
        "int": (read_int, write_int),
        "uint": (read_uint, write_uint),
        "single": (read_single, write_single),
        "double": (read_double, write_double),
        "widestr": (read_widestr, write_widestr),
    }

    def __init__(self, name: str, fields: tuple[str, str]):
        """
        Initialize a new TypedStruct schema.

        Args:
            name: Name of the struct.
            fields: Sequence of (field_name, field_type) pairs.
        """
        self._cls = namedtuple(name, tuple(f[0] for f in fields))  # type: ignore
        self._types = tuple(f[1].strip() for f in fields)
        TypedStruct.structs_cache[name] = self

    def read(self, src: BinaryIO) -> NamedTuple:
        """
        Read a struct instance from a binary stream according to the schema.

        Returns:
            An instance of the NamedTuple matching this TypedStruct.
        """
        result: list[Any] = []
        for t in self._types:
            field_type, num_repeat = self._parse_type(t)

            get_call: Callable[[BinaryIO], Any]
            if field_type in TypedStruct.io_map:
                get_call = TypedStruct.io_map[field_type][0]
            elif field_type in TypedStruct.structs_cache:
                get_call = TypedStruct.structs_cache[field_type].read
            else:
                src.close()
                raise TypeError("Unknown type")
            if num_repeat > 1:
                subresult: list[Any] = []
                for _ in range(num_repeat):
                    subresult.append(get_call(src))
                result.append(tuple(subresult))
            else:
                for _ in range(num_repeat):
                    result.append(get_call(src))

        return self._cls._make(result)

    def write(self, dst: BinaryIO, value: NamedTuple):
        """
        Write a struct instance to a binary stream according to the schema.

        Args:
            dst: Output binary stream.
            value: NamedTuple matching this TypedStruct.
        """
        for v, t in zip(value, self._types):
            field_type, num_repeat = self._parse_type(t)

            add_call: Callable[[BinaryIO, Any], None]
            if field_type in TypedStruct.io_map:
                add_call = TypedStruct.io_map[field_type][1]
            elif field_type in TypedStruct.structs_cache:
                add_call = TypedStruct.structs_cache[field_type].write
            else:
                dst.close()
                raise TypeError("Unknown type")
            if num_repeat > 1:
                for i in range(num_repeat):
                    add_call(dst, v[i])
            else:
                add_call(dst, v)

    @staticmethod
    def _parse_type(t: str) -> tuple[str, int]:
        """
        Parse a type string (e.g., 'word:3') into its base type and repeat count.

        Returns:
            Tuple of (base_type, repeat_count).
        """
        if ":" in t:
            field_type, num_repeat = t.split(":", 1)
            num_repeat = int(num_repeat)
        else:
            field_type = t
            num_repeat = 1
        return field_type, num_repeat
