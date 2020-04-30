import zlib
from io import BytesIO, SEEK_CUR, SEEK_END, SEEK_SET
from struct import pack, unpack
from abc import abstractmethod

from .common import uint_to_bytes


class AbstractIO:

    def __init__(self, io=None):
        self._io = io

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

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

    def add(self, v):
        """
        :type v: bytes
        """
        self._io.write(v)

    def add_bool(self, v):
        """
        :type v: bool
        """
        self._io.write(pack('<B', int(v)))

    def add_byte(self, v):
        """
        :type v: int
        """
        assert 0 <= v < 256
        self._io.write(pack('<B', v))

    def add_word(self, v):
        """
        :type v: int
        """
        assert 0 <= v < 65536
        self._io.write(pack('<H', v))

    def add_int(self, v):
        """
        :type v: int
        """
        self._io.write(pack('<i', v))

    def add_uint(self, v):
        """
        :type v: int
        """
        self._io.write(pack('<I', v))

    def add_single(self, v):
        """
        :type v: float
        """
        self._io.write(pack('<f', v))

    def add_double(self, v):
        """
        :type v: float
        """
        self._io.write(pack('<d', v))

    def add_widestr(self, v):
        """
        :type v: str
        """
        self._io.write(v.encode('utf-16le'))
        self._io.write(b'\x00\x00')

    def get(self, sz):
        """
        :type sz: int
        :rtype : bytes
        """
        return self._io.read(sz)

    def get_bool(self):
        """
        :rtype : bool
        """
        return unpack('<B', self._io.read(1))[0] == 1

    def get_byte(self):
        """
        :rtype : int
        """
        return unpack('<B', self._io.read(1))[0]

    def get_word(self):
        """
        :rtype : int
        """
        return unpack('<H', self._io.read(2))[0]

    def get_int(self):
        """
        :rtype : int
        """
        return unpack('<i', self._io.read(4))[0]

    def get_uint(self):
        """
        :rtype : int
        """
        return unpack('<I', self._io.read(4))[0]

    def get_single(self):
        """
        :rtype : float
        """
        return unpack('<f', self._io.read(4))[0]

    def get_double(self):
        """
        :rtype : float
        """
        return unpack('<d', self._io.read(8))[0]

    def get_widestr(self):
        """
        :rtype : str
        """
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


class Stream(AbstractIO):
    
    def open(self, io):
        if self._io is not None:
            self._io.close()
        self._io = io

    def compress(self, fmt, sz=-1):
        """
        :type fmt: int
        :type sz: int
        :rtype: bytes
        """
        if size == -1:
            size = self.size() - self.pos()
        
        result = b''
        if fmt == 1:
            result += b'ZL01'
            result += uint_to_bytes(sz)
            result += zlib.compress(self.get(sz),
                                    level=9)
        elif fmt == 2:
            pass
        elif fmt == 3:
            pass
        return result

    def decompress(self, sz=-1):
        """
        :type sz: int
        :rtype: bytes
        """
        if sz == -1:
            sz = self.size() - self.pos()

        result = b''
        magic = self.get(4)
        if magic == b'ZL01':
            bufsize = self.get_uint()
            result = zlib.decompress(self.get(sz-8),
                                     bufsize=bufsize)
        elif magic == b'ZL02':
            return
        elif magic == b'ZL03':
            for i in range(self.get_int()):
                bufsize = self.get_uint()
                result += zlib.decompress(self.get(bufsize),
                                          bufsize=65000)
        else:
            self.close()
            raise Exception("Buffer.unpack: wrong format")
        return result
    
    @classmethod
    def from_file(cls, file, mode='rb'):
        f = open(file, mode)
        return cls(f)

    @classmethod
    def from_bytes(cls, buf=None):
        if buf is None:
            return cls(BytesIO())
        else:
            return cls(BytesIO(buf))

    @classmethod
    def from_io(cls, io):
        if isinstance(io, cls):
            return io
        else:
            return cls(io)


class Buffer(AbstractIO):

    def set(self, io):
        if self._io is not None:
            self._io.close()
        self._io = BytesIO(io.read())

    def add_buf(self, buf, sz):
        self._io.write(buf.get(sz))

    def get_buf(self, sz):
        return Buffer(BytesIO(self.get(sz)))

    def save(self, path):
        with open(path, mode='wb') as f:
            f.write(self._io.getvalue())

    @staticmethod
    def _rand31pm(seed):
        """
        :type seed: int
        :rtype : int
        """
        while True:
            hi, lo = divmod(seed, 0x1f31d)
            seed = lo * 0x41a7 - hi * 0xb14
            if seed < 1:
                seed += 0x7fffffff
            yield seed - 1

    def cipher(self, key, sz=-1):
        """
        :type key: int
        :type sz: int
        """
        if sz == -1:
            sz = self.size() - self.pos()
        gen = self._rand31pm(key)
        begin = self.pos()
        end = begin + sz
        view = self._io.getbuffer()
        for i in range(begin, end):
            view[i] = view[i] ^ (next(gen) & 255)
        self._io.seek(sz, SEEK_CUR)

    def decipher(self, key, sz=-1):
        """
        :type key: int
        :type sz: int
        """
        if sz == -1:
            sz = self.size() - self.pos()
        gen = self._rand31pm(key)
        begin = self.pos()
        end = begin + sz
        view = self._io.getbuffer()
        for i in range(begin, end):
            view[i] = view[i] ^ (next(gen) & 255)
        self._io.seek(begin, SEEK_SET)

    def compress(self, fmt, sz=-1):
        """
        :type fmt: int
        :type sz: int
        :rtype: bytes
        """
        pass

    def decompress(self, sz=-1):
        """
        :type sz: int
        :rtype: bytes
        """
        if sz == -1:
            sz = self.size() - self.pos()

        result = b''
        magic = self.get(4)
        if magic == b'ZL01':
            bufsize = self.get_uint()
            begin = self.pos()
            end = begin + sz-8
            result = zlib.decompress(self._io.getbuffer()[begin:end],
                                     bufsize=bufsize)
            self._io.seek(sz-8, SEEK_CUR)
        elif magic == b'ZL02':
            return
        elif magic == b'ZL03':
            for i in range(self.get_int()):
                size = self.get_uint()
                begin = self.pos()
                end = begin + size
                result += zlib.decompress(self._io.getbuffer()[begin:end],
                                          bufsize=65000)
                self._io.seek(size, SEEK_CUR)
        else:
            self.close()
            raise Exception("Buffer.unpack: wrong format")
        return result

    def calc_hash(self, sz=-1):
        """
        :type sz: int
        :rtype : int
        """
        if sz == -1:
            sz = self.size() - self.pos()
        begin = self.pos()
        end = begin + sz
        return zlib.crc32(self._io.getbuffer()[begin:end])

    @classmethod
    def from_file(cls, path, mode='rb'):
        """
        :type path: str
        :type mode: str
        :rtype : Buffer
        """
        f = open(path, mode)
        return cls(BytesIO(f.read()))

    @classmethod
    def from_bytes(cls, buf):
        """
        :type buf: bytearray | bytes
        :rtype : Buffer
        """
        return cls(BytesIO(buf))
