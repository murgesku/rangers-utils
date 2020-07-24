__all__ = [
    "CacheData",
]

from enum import IntEnum
from typing import Union, TextIO
import warnings

from rangers._blockpar_helper import *
from rangers.io import AbstractIO, Buffer
from rangers.common import bytes_xor, bytes_to_int


class ElementKind(IntEnum):
    UNDEF = 0
    PARAM = 1
    BLOCK = 2


class CacheDataElement:

    def __init__(self,
                 name: str = "",
                 content: Union[str, 'CacheData', None] = None,
                 comment: str = ""):
        self.name = name
        if isinstance(content, str):
            self.kind = ElementKind.PARAM
        elif isinstance(content, CacheData):
            self.kind = ElementKind.BLOCK
        else:
            self.kind = ElementKind.UNDEF
        self.content = content
        self.comment = comment

    def __repr__(self):
        return f"<\"{self.name}\">"


class CacheData:

    def __init__(self):
        self._search_map = RedBlackTree()

    def __setitem__(self, key: str, value: Union[str, 'CacheData']):
        warnings.warn("Mapping interface is deprecated, "
                      "use object.set instead",
                      DeprecationWarning)
        self.set(key, value)

    def __getitem__(self, key: str) -> Union[str, 'CacheData']:
        warnings.warn("Mapping interface is deprecated, "
                      "use object.get or object.getone instead",
                      DeprecationWarning)
        return self.getone(key)

    def __delitem__(self, key: str):
        raise NotImplementedError

    def __contains__(self, key: str) -> bool:
        return self._search_map.__contains__(key)

    def __len__(self):
        return self._search_map.count

    def __iter__(self) -> Union[str, 'CacheData']:
        src = self._search_map.__iter__()
        for i in range(len(self)):
            node = next(src)
            yield node.content.content

    def clear(self):
        pass

    def add(self, key: str, value: Union[str, 'CacheData']):
        elem = CacheDataElement(key, value)
        self._search_map.append(elem)

    def set(self, key: str, value: Union[str, 'CacheData']):
        self._search_map.remove_all(key)
        elem = CacheDataElement(key, value)
        self._search_map.append(elem)

    def get(self, key: str) -> Union[str, 'CacheData']:
        return self.getone(key)

    def getone(self, key: str) -> Union[str, 'CacheData']:
        node = self._search_map.find(key)
        if node is None:
            raise KeyError
        return node.content.content

    def save(self, s: AbstractIO):
        s.add_uint(len(self))

        curblock = self._search_map.__iter__()

        left = len(self)
        index = 0

        level = 0
        stack = list()

        while level > -1:
            if left > 0:
                node = next(curblock)
                el = node.content

                if el.kind == ElementKind.PARAM:
                    s.add_byte(int(ElementKind.PARAM))
                    s.add_widestr(el.name)
                    s.add_widestr(el.content)

                    left -= 1

                elif el.kind == ElementKind.BLOCK:
                    s.add_byte(int(ElementKind.BLOCK))
                    s.add_widestr(el.name)

                    stack.append((curblock, left, index))
                    curblock = el.content._search_map.__iter__()
                    left = len(el.content)

                    s.add_uint(left)

                    level += 1

            else:
                if level > 0:
                    curblock, left, index = stack.pop()
                    left -= 1
                level -= 1

    def load(self, s: AbstractIO):
        self.clear()

        curblock = self

        left = s.get_uint()

        level = 0
        stack = list()

        while level > -1:
            if left > 0:
                type = s.get_byte()
                name = s.get_widestr()

                if type == ElementKind.PARAM:
                    curblock.add(name, s.get_widestr())
                    left -= 1

                elif type == ElementKind.BLOCK:
                    stack.append((curblock, left))

                    prevblock = curblock
                    curblock = CacheData()
                    prevblock.add(name, curblock)

                    left = s.get_uint()
                    level += 1
                    continue

            else:
                if level > 0:
                    curblock, left = stack.pop()
                    left -= 1
                level -= 1

    def load_txt(self, f: TextIO):
        self.clear()

        curblock = self

        level = 0
        stack = list()

        line_no = 0

        while True:
            line = f.readline()
            line_no += 1
            if line == '':  # EOF
                break

            line = line.strip('\x09\x0a\x0d\x20')  # \t\n\r\s

            comment = ''
            if '//' in line:
                line, comment = line.split('//', 1)
                line = line.rstrip('\x09\x20')  # \t\s

            if '{' in line:
                stack.append(curblock)

                head = line.split('{', 1)[0]
                head = head.rstrip('\x09\x20')  # \t\s

                path = ''
                if '=' in head:
                    name, path = line.split('=', 1)
                    name = name.rstrip('\x09\x20')  # \t\s
                    path = path.lstrip('\x09\x20')  # \t\s
                else:
                    name = head

                if path != '':
                    curblock[name] = CacheData.from_txt(path)
                else:
                    prevblock = curblock
                    curblock = CacheData()
                    prevblock.add(name, curblock)

                    level += 1

            elif '}' in line:
                if level > 0:
                    curblock = stack.pop()
                level -= 1

            elif '=' in line:
                name, value = line.split('=', 1)
                name = name.rstrip('\x09\x20')  # \t\s
                value = value.lstrip('\x09\x20')  # \t\s

                curblock.add(name, value)

            else:
                continue

    def save_txt(self, f: TextIO):
        curblock = self._search_map.__iter__()
        left = len(self)

        level = 0
        stack = list()

        while level > -1:
            if left > 0:
                node = next(curblock)
                el = node.content
                f.write(4 * '\x20' * level)

                if el.kind == ElementKind.PARAM:
                    f.write(el.name)
                    f.write('=')
                    f.write(el.content)
                    f.write('\x0d\x0a')
                    left -= 1

                elif el.kind == ElementKind.BLOCK:
                    stack.append((curblock, left))

                    curblock = el.content._search_map.__iter__()
                    left = len(el.content)

                    f.write(el.name)
                    f.write('\x20')  # space
                    f.write('{')
                    f.write('\x0d\x0a')  # \r\n
                    level += 1
                    continue

                else:
                    f.write('\x0d\x0a')
            else:
                level -= 1
                if level > -1:
                    f.write(4 * '\x20' * level)  # 4 spaces level padding
                    f.write('}')
                    f.write('\x0d\x0a')  # \r\n
                    curblock, left = stack.pop()
                    left -= 1

    def get_par(self, path: str) -> str:
        path = path.strip().split('.')

        curblock = self
        for part in path:
            el = curblock._search_map.find(part)
            if el is None:
                raise Exception("CacheData.get_par: path not exists")
            if part != path[-1]:
                if el.content.kind is not ElementKind.BLOCK:
                    raise Exception("CacheData.get_par: path not exists")
                curblock = el.content.content
            else:
                if el.content.kind is not ElementKind.PARAM:
                    raise Exception("CacheData.get_par: not a parameter")
                return el.content.content

    def get_block(self, path: str) -> 'CacheData':
        path = path.strip().split('.')

        curblock = self
        for part in path:
            el = curblock._search_map.find(part)
            if el is None:
                raise Exception("CacheData.get_par: path not exists")
            if part != path[-1]:
                if el.content.kind is not ElementKind.BLOCK:
                    raise Exception("CacheData.get_par: path not exists")
                curblock = el.content.content
            else:
                if el.content.kind is not ElementKind.BLOCK:
                    raise Exception("CacheData.get_par: not a block")
                return el.content.content

    def to_txt(self, path: str, encoding: str = 'cp1251'):
        with open(path, 'wt', encoding=encoding, newline='') as txt:
            self.save_txt(txt)

    @classmethod
    def from_txt(cls, path: str, encoding: str = 'cp1251') -> 'CacheData':
        cachedata = cls()
        with open(path, 'rt', encoding=encoding, newline='') as txt:
            cachedata.load_txt(txt)
        return cachedata

    @classmethod
    def from_dat(cls, path: str) -> 'CacheData':
        cachedata = None
        seed_key = b'\x37\x3f\x8f\xea'

        b = Buffer.from_file(path)

        content_hash = b.get_uint()

        seed = bytes_xor(b.get(4), seed_key)
        seed = bytes_to_int(seed)

        size = b.size() - b.pos()

        b.decipher(seed, size)
        calc_hash = b.calc_hash(size)

        if calc_hash == content_hash:
            unpacked = Buffer.from_bytes(b.decompress(size))
            b.close()
            cachedata = cls()
            cachedata.load(unpacked)
            unpacked.close()
        else:
            b.close()
            raise Exception("CacheData.from_dat: wrong content hash")

        return cachedata

