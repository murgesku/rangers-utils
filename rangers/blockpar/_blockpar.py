__all__ = [
    "BlockPar",
]

from enum import IntEnum
from typing import Union, List, TextIO
import warnings

from rangers._blockpar_helper import *
from rangers.io import AbstractIO, Buffer
from rangers.common import bytes_xor, bytes_to_int


class ElementKind(IntEnum):
    UNDEF = 0
    PARAM = 1
    BLOCK = 2


class BlockParElement:

    def __init__(self,
                 name: str = "",
                 content: Union[str, 'BlockPar', None] = None,
                 comment: str = ""):
        self.name = name
        if isinstance(content, str):
            self.kind = ElementKind.PARAM
        elif isinstance(content, BlockPar):
            self.kind = ElementKind.BLOCK
        else:
            self.kind = ElementKind.UNDEF
        self.content = content
        self.comment = comment

    def __repr__(self):
        return f"<\"{self.name}\">"


class BlockPar:

    def __init__(self, sort: bool = True):
        self._order_map = LinkedList()
        self._search_map = RedBlackTree()
        self.sorted = sort

    def __setitem__(self, key: str, value: Union[str, 'BlockPar']):
        warnings.warn("Mapping interface is deprecated, "
                      "use object.set instead",
                      DeprecationWarning)
        self.set(key, value)

    def __getitem__(self, key: str) -> Union[str, 'BlockPar']:
        warnings.warn("Mapping interface is deprecated, "
                      "use object.get or object.getone instead",
                      DeprecationWarning)
        return self.getone(key)

    def __delitem__(self, key: str):
        raise NotImplementedError

    def __contains__(self, key: str) -> bool:
        return self._search_map.__contains__(key)

    def __len__(self):
        return self._order_map.count

    def __iter__(self) -> Union[str, 'BlockPar']:
        if self.sorted:
            src = self._search_map.__iter__()
        else:
            src = self._order_map.__iter__()
        for i in range(len(self)):
            node = next(src)
            yield node.content.content

    def clear(self):
        pass

    def add(self, key: str, value: Union[str, 'BlockPar']):
        elem = BlockParElement(key, value)
        self._order_map.append(elem)
        self._search_map.append(elem)

    def set(self, key: str, value: Union[str, 'BlockPar']):
        self._order_map.remove_all(key)
        self._search_map.remove_all(key)
        elem = BlockParElement(key, value)
        self._order_map.append(elem)
        self._search_map.append(elem)

    def get(self, key: str) -> Union[str, 'BlockPar']:
        return self.getone(key)

    def getone(self, key: str) -> Union[str, 'BlockPar']:
        node = self._search_map.find(key)
        if node is None:
            raise KeyError
        return node.content.content

    def getall(self, key: str) -> List[Union[str, 'BlockPar']]:
        node = self._search_map.find(key)
        if node is None:
            raise KeyError
        result = [None for i in range(node.count)]
        for i in range(node.count):
            result[i] = node.content.content
            node = node.next
        return result

    def save(self, s: AbstractIO, *, new_format: bool = False):
        s.add_bool(self.sorted)
        s.add_uint(len(self))

        is_sort = self.sorted
        if is_sort:
            curblock = self._search_map.__iter__()
        else:
            curblock = self._order_map.__iter__()
        left = len(self)
        count = 1
        index = 0

        level = 0
        stack = list()

        while level > -1:
            if left > 0:
                node = next(curblock)
                el = node.content

                if new_format and is_sort:
                    if node.count > 1:
                        count = node.count
                        index = 0
                    s.add_uint(index)
                    if index == 0:
                        s.add_uint(count)
                    else:
                        s.add_uint(0)

                if el.kind == ElementKind.PARAM:
                    s.add_byte(int(ElementKind.PARAM))
                    s.add_widestr(el.name)
                    s.add_widestr(el.content)

                    left -= 1

                    index += 1
                    if index >= count:
                        count = 1
                        index = 0

                elif el.kind == ElementKind.BLOCK:
                    s.add_byte(int(ElementKind.BLOCK))
                    s.add_widestr(el.name)

                    stack.append((curblock, left, is_sort, count, index))
                    is_sort = el.content.sorted
                    if is_sort:
                        curblock = el.content._search_map.__iter__()
                    else:
                        curblock = el.content._order_map.__iter__()
                    left = len(el.content)
                    count = 1
                    index = 0

                    s.add_bool(is_sort)
                    s.add_uint(left)

                    level += 1

            else:
                if level > 0:
                    curblock, left, is_sort, count, index = stack.pop()
                    left -= 1
                    index += 1
                    if index >= count:
                        count = 1
                        index = 0
                level -= 1

    def load(self, s: AbstractIO, *, new_format: bool = False):
        self.clear()

        curblock = self
        curblock.sorted = s.get_bool()

        left = s.get_uint()

        level = 0
        stack = list()

        while level > -1:
            if left > 0:
                if new_format and curblock.sorted:
                    s.get(8)

                type = s.get_byte()
                name = s.get_widestr()

                if type == ElementKind.PARAM:
                    curblock.add(name, s.get_widestr())
                    left -= 1

                elif type == ElementKind.BLOCK:
                    stack.append((curblock, left))

                    prevblock = curblock
                    curblock = BlockPar()
                    prevblock.add(name, curblock)

                    curblock.sorted = s.get_bool()
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

                if head.endswith(('^', '~')):
                    curblock.sorted = head.endswith('^')
                    head = head[:-1]
                    head = head.rstrip('\x09\x20')  # \t\s
                else:
                    curblock.sorted = True

                path = ''
                if '=' in head:
                    name, path = line.split('=', 1)
                    name = name.rstrip('\x09\x20')  # \t\s
                    path = path.lstrip('\x09\x20')  # \t\s
                else:
                    name = head

                if path != '':
                    curblock[name] = BlockPar.from_txt(path)
                else:
                    prevblock = curblock
                    curblock = BlockPar()
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

                # multiline parameters - heredoc
                if value.startswith('<<<'):
                    value = ''
                    spacenum = 0
                    while True:
                        line = f.readline()
                        line_no += 1
                        if line == '':  # EOF
                            raise Exception("BlockPar.load_txt: "
                                            "heredoc end marker not found")

                        if line.strip('\x09\x0a\x0d\x20') == '':
                            continue

                        if value == '':
                            spacenum = len(line) - len(line.lstrip('\x20'))
                            if spacenum > (4 * level):
                                spacenum = 4 * level

                        if line.lstrip('\x09\x20').startswith('>>>'):
                            value = value.rstrip('\x0a\x0d')
                            break

                        value += line[spacenum:]

                curblock.add(name, value)

            else:
                continue

    def save_txt(self, f: TextIO):
        is_sort = self.sorted
        if is_sort:
            curblock = self._search_map.__iter__()
        else:
            curblock = self._order_map.__iter__()
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
                    if '\x0d' in el.content or '\x0a' in el.content:
                        f.write('<<<')
                        f.write('\x0d\x0a')
                        content = el.content
                        for s in content.splitlines(keepends=True):
                            f.write(4 * '\x20' * level)
                            f.write(s)
                        f.write('\x0d\x0a')
                        f.write(4 * '\x20' * level)
                        f.write('>>>')
                    else:
                        f.write(el.content)
                    f.write('\x0d\x0a')
                    left -= 1

                elif el.kind == ElementKind.BLOCK:
                    stack.append((curblock, left))

                    is_sort = el.content.sorted
                    if is_sort:
                        curblock = el.content._search_map.__iter__()
                    else:
                        curblock = el.content._order_map.__iter__()
                    left = len(el.content)

                    f.write(el.name)
                    f.write('\x20')  # space
                    if el.content.sorted:
                        f.write('^')
                    else:
                        f.write('~')
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
                raise Exception("BlockPar.get_par: path not exists")
            if part != path[-1]:
                if el.content.kind is not ElementKind.BLOCK:
                    raise Exception("BlockPar.get_par: path not exists")
                curblock = el.content.content
            else:
                if el.content.kind is not ElementKind.PARAM:
                    raise Exception("BlockPar.get_par: not a parameter")
                return el.content.content

    def get_block(self, path: str) -> 'BlockPar':
        path = path.strip().split('.')

        curblock = self
        for part in path:
            el = curblock._search_map.find(part)
            if el is None:
                raise Exception("BlockPar.get_par: path not exists")
            if part != path[-1]:
                if el.content.kind is not ElementKind.BLOCK:
                    raise Exception("BlockPar.get_par: path not exists")
                curblock = el.content.content
            else:
                if el.content.kind is not ElementKind.BLOCK:
                    raise Exception("BlockPar.get_par: not a block")
                return el.content.content

    def to_txt(self, path: str, encoding: str = 'cp1251'):
        with open(path, 'wt', encoding=encoding, newline='') as txt:
            self.save_txt(txt)

    @classmethod
    def from_txt(cls, path: str, encoding: str = 'cp1251') -> 'BlockPar':
        blockpar = cls()
        with open(path, 'rt', encoding=encoding, newline='') as txt:
            blockpar.load_txt(txt)
        return blockpar

    @classmethod
    def from_dat(cls, path: str) -> 'BlockPar':
        blockpar = None
        seed_key = b'\x89\xc6\xe8\xb1'

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
            blockpar = cls()
            blockpar.load(unpacked, new_format=True)
            unpacked.close()
        else:
            b.close()
            raise Exception("BlockPar.from_dat: wrong content hash")

        return blockpar

