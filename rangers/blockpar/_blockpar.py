__all__ = [
    "BlockPar",
]

from multidict import MultiDict
from enum import IntEnum
from typing import Union, List, Tuple, TextIO, Optional
import warnings

from rangers.io import AbstractIO, Buffer
from rangers.common import bytes_xor, bytes_to_int


class ElementKind(IntEnum):
    EMPTY = 0
    PARAM = 1
    BLOCK = 2


class BlockParNode:
    __slots__ = ("kind", "parent", "content", "comment", "count")

    def __init__(self,
                 parent: Optional['BlockPar'] = None,
                 content: Optional[Union[str, 'BlockPar']] = None,
                 comment: str = ""):
        self.parent = parent
        if isinstance(content, str):
            self.kind = ElementKind.PARAM
        elif isinstance(content, BlockPar):
            self.kind = ElementKind.BLOCK
        else:
            self.kind = ElementKind.UNDEF
        self.content = content
        self.comment = comment
        self.count = 0

    def clear(self):
        self.kind = ElementKind.EMPTY
        self.parent = None
        self.content = None
        self.comment = ""
        self.count = 0

    def set_par(self, content: str):
        self.kind = ElementKind.PARAM
        self.content = content

    def set_block(self, content: 'BlockPar'):
        self.kind = ElementKind.BLOCK
        self.content = content

    def add_comment(self, comment: str):
        self.comment = comment


class BlockPar:

    def __init__(self, sort: bool = True):
        self._map: MultiDict[BlockParNode] = MultiDict()
        self.sorted = sort

    def __setitem__(self, name: str, content: Union[str, 'BlockPar']):
        warnings.warn("Mapping interface is deprecated, "
                      "use object.set instead",
                      DeprecationWarning)
        self.set(name, content)

    def __getitem__(self, name: str) -> Union[str, 'BlockPar']:
        warnings.warn("Mapping interface is deprecated, "
                      "use object.get or object.getone instead",
                      DeprecationWarning)
        return self.getone(name)

    def __delitem__(self, name: str):
        self._map.__delitem__(name)

    def __contains__(self, name: str) -> bool:
        return name in self._map

    def __len__(self):
        return len(self._map)

    def __iter__(self) -> Tuple[str, Union[str, 'BlockPar', None]]:
        if self.sorted:
            source = sorted(self._map.items(), key=lambda k: k[0])
        else:
            source = self._map.items()
        for name, content in source:
            yield name, content.content

    def clear(self):
        self._map.clear()
        self.sorted = True

    def add(self, name: str, content: Union[str, 'BlockPar'], *, comment: str = ""):
        self._map.add(name, BlockParNode(self, content, comment))
        if self.sorted:
            first = self._map.getone(name)
            first.count += 1

    def set(self, name: str, content: Union[str, 'BlockPar'], *, comment: str = ""):
        self._map.__setitem__(name, BlockParNode(self, content, comment))

    def get(self, name: str) -> Union[str, 'BlockPar', None]:
        return self._map.getone(name).content

    def getone(self, name: str) -> Union[str, 'BlockPar', None]:
        return self._map.getone(name).content

    def getall(self, name: str) -> List[Union[str, 'BlockPar', None]]:
        return list(map(lambda node: node.content, self._map.getall(name)))

    def add_empty(self, name: str = "") -> BlockParNode:
        node = BlockParNode(self)
        self._map.add(name, node)
        if self.sorted:
            first = self._map.getone(name)
            first.count += 1
        return node

    def add_block(self, name: str, sort: bool = True) -> 'BlockPar':
        bp = BlockPar(sort=sort)
        self._map.add(name, BlockParNode(self, bp))
        if self.sorted:
            first = self._map.getone(name)
            first.count += 1
        return bp

    def add_par(self, name: str, content: str):
        self._map.add(name, BlockParNode(self, content))
        if self.sorted:
            first = self._map.getone(name)
            first.count += 1

    def get_par(self, name: str, index: int = 0) -> str:
        node = self._map.getone(name)
        if index > 0:
            if node.count == 1 or index >= node.count:
                raise IndexError("BlockPar.get_par_path: index out of range")
            node = self._map.getall(name)[index]
        if node.kind is not ElementKind.PARAM:
            raise TypeError("BlockPar.get_par_path: not a parameter")
        return node.content

    def get_par_path(self, path: str) -> str:
        path = path.strip()
        path_index = path.rsplit(":", 1)
        index = 0
        if len(path_index) > 1:
            index = int(path_index[1])
        path = path_index[0]
        path = path.split('.')

        curblock = self
        for part in path:
            if part not in curblock._map:
                raise KeyError("BlockPar.get_par_path: path not exists")
            if part != path[-1]:
                node = curblock._map.getone(part)
                if node.kind is not ElementKind.BLOCK:
                    raise TypeError("BlockPar.get_par_path: path not exists")
                curblock = node.content
            else:
                node = curblock._map.getone(part)
                if index > 0:
                    if node.count == 1 or index >= node.count:
                        raise IndexError("BlockPar.get_par_path: index out of range")
                    node = curblock._map.getall(part)[index]
                if node.kind is not ElementKind.PARAM:
                    raise TypeError("BlockPar.get_par_path: not a parameter")
                return node.content

    def get_block(self, name: str, index: int = 0) -> 'BlockPar':
        node = self._map.getone(name)
        if index > 0:
            if node.count == 1 or index >= node.count:
                raise IndexError("BlockPar.get_block: index out of range")
            node = self._map.getall(name)[index]
        if node.kind is not ElementKind.BLOCK:
            raise TypeError("BlockPar.get_block: not a block")
        return node.content

    def get_block_path(self, path: str) -> 'BlockPar':
        path = path.strip()
        path_index = path.rsplit(":", 1)
        index = 0
        if len(path_index) > 1:
            index = int(path_index[1])
        path = path_index[0]
        path = path.split('.')

        curblock = self
        for part in path:
            if part not in curblock._map:
                raise KeyError("BlockPar.get_block_path: path not exists")
            if part != path[-1]:
                node = curblock._map.getone(part)
                if node.kind is not ElementKind.BLOCK:
                    raise TypeError("BlockPar.get_block_path: path not exists")
                curblock = node.content
            else:
                node = curblock._map.getone(part)
                if index > 0:
                    if node.count == 1 or index >= node.count:
                        raise IndexError("BlockPar.get_block_path: index out of range")
                    node = curblock._map.getall(part)[index]
                if node.kind is not ElementKind.BLOCK:
                    raise TypeError("BlockPar.get_block_path: not a block")
                return node.content

    def save(self, s: AbstractIO):
        s.add_bool(self.sorted)
        s.add_int(len(self._map))

        is_sort = self.sorted
        if is_sort:
            for name in sorted(self._map.keys()):
                for i, node in enumerate(self._map.getall(name)):
                    s.add_int(i)
                    s.add_int(node.count)
                    if node.kind == ElementKind.PARAM:
                        s.add_byte(int(node.kind))
                        s.add_widestr(name)
                        s.add_widestr(node.content)
                    elif node.kind == ElementKind.BLOCK:
                        s.add_byte(int(node.kind))
                        s.add_widestr(name)
                        node.content.save(s)
        else:
            for name, node in self._map.items():
                if node.kind == ElementKind.PARAM:
                    s.add_byte(int(node.kind))
                    s.add_widestr(name)
                    s.add_widestr(node.content)
                elif node.kind == ElementKind.BLOCK:
                    s.add_byte(int(node.kind))
                    s.add_widestr(name)
                    node.content.save(s)

    def load(self, s: AbstractIO):
        self.sorted = s.get_bool()
        length = s.get_int()

        for i in range(length):
            if self.sorted:
                s.get_int()
                s.get_int()
            kind = s.get_byte()
            if kind == ElementKind.PARAM:
                name = s.get_widestr()
                self.add_par(name, s.get_widestr())
            elif kind == ElementKind.BLOCK:
                name = s.get_widestr()
                bp = self.add_block(name)
                bp.load(s)

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
                    is_sort = head.endswith('^')
                    head = head[:-1]
                    head = head.rstrip('\x09\x20')  # \t\s
                else:
                    is_sort = True

                path = ''
                if '=' in head:
                    name, path = line.split('=', 1)
                    name = name.rstrip('\x09\x20')  # \t\s
                    path = path.lstrip('\x09\x20')  # \t\s
                else:
                    name = head

                if path != '':
                    curblock.set(name, BlockPar.from_txt(path))
                else:
                    prevblock = curblock
                    curblock = BlockPar(is_sort)
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

                curblock.add(name, value, comment=comment)

            else:
                continue

    def save_txt(self, f: TextIO):
        is_sort = self.sorted
        if is_sort:
            curblock = iter(sorted(self._map.items(), key=lambda k: k[0]))
        else:
            curblock = iter(self._map.items())
        left = len(self._map)

        level = 0
        stack = list()

        while level > -1:
            if left > 0:
                name, node = next(curblock)
                f.write(4 * '\x20' * level)

                if node.kind == ElementKind.PARAM:
                    f.write(name)
                    f.write('=')
                    if '\x0d' in node.content or '\x0a' in node.content:
                        f.write('<<<')
                        f.write('\x0d\x0a')
                        content = node.content
                        for s in content.splitlines(keepends=True):
                            f.write(4 * '\x20' * level)
                            f.write(s)
                        f.write('\x0d\x0a')
                        f.write(4 * '\x20' * level)
                        f.write('>>>')
                    else:
                        f.write(node.content)
                    f.write('\x0d\x0a')
                    left -= 1

                elif node.kind == ElementKind.BLOCK:
                    stack.append((curblock, left))

                    is_sort = node.content.sorted
                    if is_sort:
                        curblock = iter(sorted(node.content._map.items(), key=lambda k: k[0]))
                    else:
                        curblock = iter(node.content._map.items())
                    left = len(node.content._map)

                    f.write(name)
                    f.write('\x20')  # space
                    if node.content.sorted:
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
            blockpar.load(unpacked)
            unpacked.close()
        else:
            b.close()
            raise Exception("BlockPar.from_dat: wrong content hash")

        return blockpar

