__all__ = [
    "AbstractIO",
    "Stream",
    "Buffer",
    "SEEK_END", "SEEK_SET", "SEEK_CUR",
]

import zlib
from io import BytesIO, SEEK_CUR, SEEK_SET, SEEK_END
from struct import pack, unpack
from abc import abstractmethod
from collections import namedtuple
from typing import Union, BinaryIO, NamedTuple


class AbstractIO:

    def __init__(self, io=None):
        self._io = io

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self._io.close()

    def close(self):
        self._io.close()

    def seek(self, n, flag=SEEK_SET):
        self._io.seek(n, flag)

    def pos(self):
        return self._io.tell()

    def size(self):
        old_pos = self._io.tell()
        self._io.seek(0, SEEK_END)
        size = self._io.tell()
        self._io.seek(old_pos, SEEK_SET)
        return size

    def add(self, v: Union[bytes, bytearray]):
        self._io.write(v)

    def add_bool(self, v: bool):
        self._io.write(pack('<B', int(v)))

    def add_byte(self, v: int):
        assert 0 <= v < 256
        self._io.write(pack('<B', v))

    def add_word(self, v: int):
        assert 0 <= v < 65536
        self._io.write(pack('<H', v))

    def add_int(self, v: int):
        self._io.write(pack('<i', v))

    def add_uint(self, v: int):
        self._io.write(pack('<I', v))

    def add_single(self, v: float):
        self._io.write(pack('<f', v))

    def add_double(self, v: float):
        self._io.write(pack('<d', v))

    def add_widestr(self, v: str):
        self._io.write(v.encode('utf-16le'))
        self._io.write(b'\x00\x00')

    def add_struct(self, v: NamedTuple):
        if v.__class__.__name__ in TypeStruct._structs_cache:
            TypeStruct._structs_cache[v.__class__.__name__]._add(self, v)

    def get(self, size: int) -> bytes:
        return self._io.read(size)

    def get_bool(self) -> bool:
        return unpack('<B', self._io.read(1))[0] == 1

    def get_byte(self) -> int:
        return unpack('<B', self._io.read(1))[0]

    def get_word(self) -> int:
        return unpack('<H', self._io.read(2))[0]

    def get_int(self) -> int:
        return unpack('<i', self._io.read(4))[0]

    def get_uint(self) -> int:
        return unpack('<I', self._io.read(4))[0]

    def get_single(self) -> float:
        return unpack('<f', self._io.read(4))[0]

    def get_double(self) -> float:
        return unpack('<d', self._io.read(8))[0]

    def get_widestr(self) -> str:
        start = self._io.tell()
        size = 0
        while True:
            c = self._io.read(2)
            if c is None or len(c) < 2:
                return ''
            elif c == b'\x00\x00':
                size = self._io.tell() - start - 2
                break
            else:
                continue
        self._io.seek(start)
        s = self._io.read(size).decode('utf-16le')
        self._io.seek(2, SEEK_CUR)
        return s

    def get_struct(self, t: 'TypeStruct') -> NamedTuple:
        return t._get(self)

    @abstractmethod
    def _decompress(self, start: int, size: int, bufsize: int) -> bytes:
        """Abstract interface for zlib.decompress"""
        pass

    def decompress(self, size: int = -1) -> bytes:
        if size == -1:
            size = self.size() - self.pos()

        result = b''
        magic = self.get(4)
        if magic == b'ZL01':
            bufsize = self.get_uint()
            start = self.pos()
            result = self._decompress(start, size - 8, bufsize)
        elif magic == b'ZL02':
            # return
            self.close()
            raise ValueError("AbstractIO.decompress: unknown format")
        elif magic == b'ZL03':
            for i in range(self.get_int()):
                chunksize = self.get_uint()
                start = self.pos()
                result += self._decompress(start, chunksize, 65000)
        else:
            self.close()
            raise ValueError("AbstractIO.decompress: unknown format")
        return result

    def _compress(self):
        pass

    def compress(self, fmt: str, size: int = -1) -> bytes:
        """
        Supported formats: 'ZLO1', 'ZL02', 'ZL03'
        """
        pass
        # if size == -1:
        #     size = self.size() - self.pos()

        # result = b''
        # if fmt == 'ZL01':
        #     result += b'ZL01'
        #     result += uint_to_bytes(sz)
        #     result += zlib.compress(self.get(size),
        #                             level=9)
        # elif fmt == 'ZL02':
        #     pass
        # elif fmt == 'ZL03':
        #     pass
        # return result


class Stream(AbstractIO):

    def open(self, io: BinaryIO):
        if self._io is not None:
            self._io.close()
        self._io = io

    def _decompress(self, start: int, size: int, bufsize: int) -> bytes:
        return zlib.decompress(self.get(size), bufsize=bufsize)

    @classmethod
    def from_file(cls, file: Union[str, bytes, int],
                  mode: str = 'rb') -> 'Stream':
        f = open(file, mode)
        return cls(f)

    @classmethod
    def from_bytes(cls, buf: Union[bytes, bytearray, None] = None) -> 'Stream':
        if buf is None:
            return cls(BytesIO())
        else:
            return cls(BytesIO(buf))

    @classmethod
    def from_io(cls, io: Union[BinaryIO, AbstractIO]) -> 'Stream':
        if isinstance(io, cls):
            return io
        else:
            return cls(io)


class Buffer(AbstractIO):

    def set(self, io: BinaryIO):
        if self._io is not None:
            self._io.close()
        self._io = BytesIO(io.read())

    def add_buf(self, buf: 'Buffer', size: int):
        self._io.write(buf.get(size))

    def get_buf(self, size: int):
        return Buffer(BytesIO(self.get(size)))

    def save(self, file: Union[str, bytes, int]):
        with open(file, mode='wb') as f:
            f.write(self._io.getvalue())

    @staticmethod
    def _rand31pm(seed: int) -> int:
        while True:
            hi, lo = divmod(seed, 0x1f31d)
            seed = lo * 0x41a7 - hi * 0xb14
            if seed < 1:
                seed += 0x7fffffff
            yield seed - 1

    def cipher(self, key: int, size: int = -1):
        if size == -1:
            size = self.size() - self.pos()
        gen = self._rand31pm(key)
        begin = self.pos()
        end = begin + size
        view = self._io.getbuffer()
        for i in range(begin, end):
            view[i] = view[i] ^ (next(gen) & 255)
        self._io.seek(size, SEEK_CUR)

    def decipher(self, key: int, size: int = -1):
        if size == -1:
            size = self.size() - self.pos()
        gen = self._rand31pm(key)
        begin = self.pos()
        end = begin + size
        view = self._io.getbuffer()
        for i in range(begin, end):
            view[i] = view[i] ^ (next(gen) & 255)
        self._io.seek(begin, SEEK_SET)

    def _decompress(self, start: int, size: int, bufsize: int) -> bytes:
        end = start + size
        result = zlib.decompress(self._io.getbuffer()[start:end],
                                 bufsize=bufsize)
        self._io.seek(size, SEEK_CUR)
        return result

    def calc_hash(self, size: int = -1) -> int:
        if size == -1:
            size = self.size() - self.pos()
        begin = self.pos()
        end = begin + size
        return zlib.crc32(self._io.getbuffer()[begin:end])

    @classmethod
    def from_file(cls, file: Union[str, bytes, int],
                  mode: str = 'rb') -> 'Buffer':
        f = open(file, mode)
        return cls(BytesIO(f.read()))

    @classmethod
    def from_bytes(cls, b: Union[bytes, bytearray]) -> 'Buffer':
        return cls(BytesIO(b))


class TypeStruct:
    _structs_cache = {}
    _io_map = {
        'bool': ('get_bool', 'add_bool'),
        'byte': ('get_byte', 'add_byte'),
        'word': ('get_word', 'add_word'),
        'int': ('get_int', 'add_int'),
        'uint': ('get_uint', 'add_uint'),
        'single': ('get_single', 'add_single'),
        'double': ('get_double', 'add_double'),
        'widestr': ('get_widestr', 'add_widestr')
    }

    def __init__(self, name: str, fields: tuple):
        self.ntuple_cls = namedtuple(name, tuple(f[0] for f in fields))
        self.types = tuple(f[1].strip() for f in fields)
        TypeStruct._structs_cache[name] = self

    def _get(self, absio: AbstractIO) -> NamedTuple:
        result = []
        for t in self.types:
            if ':' in t:
                field_type, num_repeat = t.split(':', 1)
                num_repeat = int(num_repeat)
            else:
                field_type = t
                num_repeat = 1

            if field_type in TypeStruct._io_map:
                get_call = getattr(absio, TypeStruct._io_map[field_type][0])
                if num_repeat > 1:
                    subresult = []
                    for i in range(num_repeat):
                        subresult.append(get_call())
                    result.append(tuple(subresult))
                else:
                    for i in range(num_repeat):
                        result.append(get_call())
            elif field_type in TypeStruct._structs_cache:
                get_call = TypeStruct._structs_cache[field_type]._get
                if num_repeat > 1:
                    subresult = []
                    for i in range(num_repeat):
                        subresult.append(get_call())
                    result.append(tuple(subresult))
                else:
                    for i in range(num_repeat):
                        result.append(get_call())
            else:
                absio.close()
                raise TypeError("TypeStruct.get: unknown type")

        return self.ntuple_cls._make(result)

    def _add(self, absio: AbstractIO, value: NamedTuple):
        for v, t in zip(value, self.types):
            if ':' in t:
                field_type, num_repeat = t.split(':', 1)
                num_repeat = int(num_repeat)
            else:
                field_type = t
                num_repeat = 1

            if field_type in TypeStruct._io_map:
                add_call = getattr(absio, TypeStruct._io_map[field_type][1])
                if num_repeat > 1:
                    for i in range(num_repeat):
                        add_call(v[i])
                else:
                    add_call(v)
            elif field_type in TypeStruct._structs_cache:
                add_call = TypeStruct._structs_cache[field_type]._add
                if num_repeat > 1:
                    for i in range(num_repeat):
                        add_call(v[i])
                else:
                    add_call(v)
            else:
                absio.close()
                raise TypeError("TypeStruct.add: unknown type")
