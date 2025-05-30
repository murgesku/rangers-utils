__all__ = [
    "BlockPar",
]

import os.path
from bisect import bisect_left, bisect_right, insort_right
from collections import Counter
from enum import IntEnum, Enum
from functools import total_ordering
from typing import Generator, TextIO, TypeAlias, Union, cast, overload

from rangers.io import AbstractIO, Buffer
from rangers.utils import bytes_to_int, bytes_xor, num_leading, parse_index

Content: TypeAlias = Union[str, "BlockPar"]


@total_ordering
class _Node:
    class Kind(IntEnum):
        UNDEF = 0
        PARAM = 1
        BLOCK = 2

    __slots__ = ("kind", "name", "content", "comment")

    kind: Kind
    name: str
    content: Content | None
    comment: str

    def __init__(
        self, name: str = "", content: Content | None = None, comment: str = ""
    ):
        self.name = name
        if isinstance(content, str):
            self.kind = _Node.Kind.PARAM
        elif isinstance(content, BlockPar):
            self.kind = _Node.Kind.BLOCK
        else:
            self.kind = _Node.Kind.UNDEF
        self.content = content
        self.comment = comment

    def __repr__(self):
        return f'<{self.kind.name}: "{self.name}">'

    def __lt__(self, other: object) -> bool:
        if isinstance(other, _Node):
            return self.name < other.name
        elif isinstance(other, str):
            return self.name < other
        return NotImplemented

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _Node):
            return self.name == other.name
        elif isinstance(other, str):
            return self.name == other
        return NotImplemented


# constant guard to terminate iteration
_TERMINAL = _Node(comment="!!!TERMINAL!!!")

# sentinel type for default values
_SENTINEL = Enum("_SENTINEL", "sentinel")
sentinel = _SENTINEL.sentinel


class BlockPar:
    __content: list[_Node]
    __keys: Counter[str]

    def __init__(self, sort: bool = True) -> None:
        self.__content = list()
        self.__keys = Counter()
        self.sorted = sort

    def __delitem__(self, key: str) -> None:
        self.delete(key)

    def __contains__(self, key: str) -> bool:
        return key in self.__keys

    def __len__(self) -> int:
        return len(self.__content)

    def __iter__(self) -> Generator[tuple[str, Content], None, None]:
        src = iter(self.__content)
        while (node := next(src, _TERMINAL)) is not _TERMINAL:
            if node.content is None:
                continue
            yield (node.name, node.content)
    
    def _clamp(self, key: str, index: int) -> int:
        count = self.__keys[key]
        return count - (~index) if index < 0 else index
    
    def _in_bounds(self, key: str, index: int) -> bool:
        count = self.__keys[key]
        if 0 <= index < count:
            return True
        return False

    def clear(self) -> None:
        self.__content.clear()
        self.__keys.clear()
    
    @overload
    def delete(self, key: str) -> None: ...
    @overload
    def delete(self, key: str, index: int) -> None: ...
    def delete(self, key: str, index: int | _SENTINEL = sentinel) -> None:
        if key not in self.__keys:
            return

        is_indexed = index is not sentinel
        if is_indexed:
            index = self._clamp(key, cast(int, index))
            if not self._in_bounds(key, index):
                raise IndexError(f"Index {index} out of range for key '{key}'")

        if self.sorted:
            begin = bisect_left(self.__content, key)
            end = bisect_right(self.__content, key)
            if is_indexed:
                index = cast(int, index)
                del self.__content[begin + index]
            else:
                del self.__content[begin:end]
                del self.__keys[key]
        else:
            if is_indexed:
                index = cast(int, index)
                for i, node in enumerate(self.__content):
                    if node.name == key:
                        if index < 0:
                            del self.__content[i]
                            break
                        index -= 1
            else:
                self.__content = [node for node in self.__content if node.name != key]
                del self.__keys[key]

    def add(self, key: str, value: Content) -> None:
        node = _Node(key, value)
        if self.sorted:
            insort_right(self.__content, node)
        else:
            self.__content.append(node)
        self.__keys[key] += 1

    def set(self, key: str, value: Content) -> None:
        del self[key]
        self.add(key, value)

    @overload
    def get(self, key: str) -> Content | None: ...
    @overload
    def get(self, key: str, index: int) -> Content | None: ...
    @overload
    def get(self, key: str, *, default: Content | None) -> Content | None: ...
    @overload
    def get(
        self, key: str, index: int, *, default: Content | None
    ) -> Content | None: ...
    def get(
        self, key: str, index: int = 0, *, default: Content | None = None
    ) -> Content | None:
        return self.getone(key, index, default=default)

    @overload
    def getone(self, key: str) -> Content: ...
    @overload
    def getone(self, key: str, index: int) -> Content: ...
    @overload
    def getone(self, key: str, *, default: Content | None) -> Content | None: ...
    @overload
    def getone(
        self, key: str, index: int, *, default: Content | None
    ) -> Content | None: ...
    def getone(
        self,
        key: str,
        index: int = 0,
        *,
        default: Content | None | _SENTINEL = sentinel,
    ) -> Content | None:
        if key not in self.__keys:
            if default is sentinel:
                raise KeyError(key)
            return cast(Content | None, default)

        index = self._clamp(key, index)
        if not self._in_bounds(key, index):
            if default is sentinel:
                raise IndexError(f"Index {index} out of range for key '{key}'")
            return cast(Content | None, default)

        return self._getone(key, index).content

    @overload
    def getall(self, key: str) -> list[Content]: ...
    @overload
    def getall(
        self, key: str, *, default: list[Content] | None
    ) -> list[Content] | None: ...
    def getall(
        self, key: str, *, default: list[Content] | None | _SENTINEL = sentinel
    ) -> list[Content] | None:
        if key not in self.__keys:
            if default is sentinel:
                raise KeyError(key)
            return cast(list[Content] | None, default)

        return [node.content for node in self._getall(key) if node.content is not None]

    def _getone(self, key: str, index: int = 0) -> _Node:
        if self.sorted:
            first = bisect_left(self.__content, key)
            return self.__content[first + index]
        else:
            for node in self.__content:
                if node.name == key:
                    index -= 1
                    if index < 0:
                        return node
            assert False, "unreachable"

    def _getall(self, key: str) -> list[_Node]:
        if self.sorted:
            begin = bisect_left(self.__content, key)
            end = bisect_right(self.__content, key)
            return self.__content[begin:end]
        else:
            return [node for node in self.__content if node.name == key]

    def save(self, s: AbstractIO, *, new_format: bool = False):
        s.add_bool(self.sorted)
        s.add_uint(len(self))

        prev_name = None
        index = 0

        for node in self.__content:
            if new_format and self.sorted:
                if node.name != prev_name:
                    prev_name = node.name
                    index = 0
                    count = self.__keys[node.name]
                else:
                    count = 0

                s.add_uint(index)
                s.add_uint(count)
                index += 1

            if node.kind != _Node.Kind.UNDEF:
                s.add_byte(node.kind)
                s.add_widestr(node.name)

            match node.kind:
                case _Node.Kind.PARAM:
                    s.add_widestr(cast(str, node.content))

                case _Node.Kind.BLOCK:
                    cast(BlockPar, node.content).save(s, new_format=new_format)

                case _:
                    continue

    def load(self, s: AbstractIO, *, new_format: bool = False):
        self.clear()

        self.sorted = s.get_bool()
        remain = s.get_uint()

        while remain > 0:
            count = 0
            if new_format and self.sorted:
                s.get_uint()  # index
                count = s.get_uint()

            kind = _Node.Kind(s.get_byte())
            name = s.get_widestr()

            if count > 0:
                self.__keys[name] = count

            match kind:
                case _Node.Kind.PARAM:
                    content = s.get_widestr()
                    self.add(name, content)

                case _Node.Kind.BLOCK:
                    content = BlockPar()
                    content.load(s, new_format=new_format)
                    self.add(name, content)

                case _:
                    pass

            remain -= 1

    def save_txt(self, f: TextIO, *, level: int = 0):
        for node in self.__content:
            f.write(4 * "\x20" * level)

            match node.kind:
                case _Node.Kind.PARAM:
                    content = cast(str, node.content)

                    f.write(node.name)
                    f.write("=")

                    if "\x0d" in content or "\x0a" in content:
                        f.write("<<<")
                        f.write("\x0d\x0a")

                        for s in content.splitlines(keepends=True):
                            f.write(4 * "\x20" * level)
                            f.write(s)
                        f.write("\x0d\x0a")

                        f.write(4 * "\x20" * level)
                        f.write(">>>")

                    else:
                        f.write(content)

                    f.write("\x0d\x0a")

                case _Node.Kind.BLOCK:
                    content = cast(BlockPar, node.content)
                    f.write(node.name)
                    f.write(" ")
                    f.write("^" if content.sorted else "~")
                    f.write("{")
                    f.write("\x0d\x0a")  # \r\n

                    content.save_txt(f, level=level + 1)

                    f.write("}")
                    f.write("\x0d\x0a")

                case _:
                    continue

    def load_txt(self, f: TextIO, *, level: int = 0):
        self.clear()
        self.sorted = level == 0

        while line := f.readline():
            line = line.strip("\x09\x0a\x0d\x20")  # \t\n\r\s

            if "//" in line:
                line, _ = line.split("//", 1)
                line = line.rstrip("\x09\x20")  # \t\s

            if "=" in line:
                name, value = line.split("=", 1)
                name = name.rstrip("\x09\x20")
                value = value.lstrip("\x09\x20")

                # multiline parameters - heredoc
                if value.startswith("<<<"):
                    value = ""
                    while line := f.readline():
                        if line.strip("\x09\x0a\x0d\x20") == "":
                            continue

                        spacenum = 4 * (level + 1)
                        if value == "":
                            spacenum = min(spacenum, num_leading(line, "\x20"))

                        if line.lstrip("\x09\x20").startswith(">>>"):
                            value = value.rstrip("\x0a\x0d")
                            break

                        value += line[spacenum:]
                    else:
                        raise Exception(
                            "BlockPar.load_txt: heredoc end marker not found"
                        )

                self.add(name, value)

            elif "{" in line:
                head = line.split("{", 1)[0]
                head = head.rstrip("\x09\x20")

                sorted = True
                if head.endswith(("^", "~")):
                    sorted = head.endswith("^")
                    head = head[:-1]
                    head = head.rstrip("\x09\x20")

                path = ""
                if "=" in head:
                    name, path = line.split("=", 1)
                    name = name.rstrip("\x09\x20")
                    path = path.lstrip("\x09\x20")
                else:
                    name = head

                if path != "":
                    if not os.path.exists(path):
                        raise Exception(
                            "BlockPar.load_txt: invalid path to load blockpar"
                        )
                    self.add(name, BlockPar.from_txt(path))
                else:
                    block = BlockPar(sort=sorted)
                    block.load_txt(f, level=level + 1)
                    self.add(name, block)

            elif "}" in line:
                if level > 0:
                    break
                else:
                    raise Exception("BlockPar.load_txt: unexpected end of blockpar")

            else:
                continue
        else:
            if level > 1:
                raise Exception(
                    "BlockPar.load_txt: end of file reached in nested block"
                )

    def get_par(self, path: str) -> str:
        parts = path.strip().split(".")
        block = self

        for part in parts:
            name, index = parse_index(part)

            if name not in self.__keys:
                raise Exception("BlockPar.get_par: path not exists")
            index = self._clamp(name, index)
            if not self._in_bounds(name, index):
                raise Exception(f"BlockPar.get_par: index out of range in '{name}'")
            
            node = block._getone(name, index)

            if part != path[-1]:
                if node.kind is not _Node.Kind.BLOCK:
                    raise Exception("BlockPar.get_par: path not exists")
                block = cast(BlockPar, node.content)

            else:
                if node.kind is not _Node.Kind.PARAM:
                    raise Exception("BlockPar.get_par: not a parameter")
                return cast(str, node.content)
            
        assert False, "unreachable"

    def get_block(self, path: str) -> "BlockPar":
        parts = path.strip().split(".")
        block = self

        for part in parts:
            name, index = parse_index(part)

            if name not in self.__keys:
                raise Exception("BlockPar.get_par: path not exists")
            index = self._clamp(name, index)
            if not self._in_bounds(name, index):
                raise Exception(f"BlockPar.get_par: index out of range in '{name}'")
            
            node = block._getone(name, index)

            if part != path[-1]:
                if node.kind is not _Node.Kind.BLOCK:
                    raise Exception("BlockPar.get_par: path not exists")
                block = cast(BlockPar, node.content)

            else:
                if node.kind is not _Node.Kind.BLOCK:
                    raise Exception("BlockPar.get_par: not a block")
                return cast(BlockPar, node.content)
            
        assert False, "unreachable"

    def to_txt(self, path: str, encoding: str = "cp1251"):
        with open(path, "wt", encoding=encoding, newline="") as txt:
            self.save_txt(txt)

    @classmethod
    def from_txt(cls, path: str, encoding: str = "cp1251") -> "BlockPar":
        blockpar = cls()
        with open(path, "rt", encoding=encoding, newline="") as txt:
            blockpar.load_txt(txt)
        return blockpar

    @classmethod
    def from_dat(cls, path: str) -> "BlockPar":
        blockpar = None
        seed_key = b"\x89\xc6\xe8\xb1"

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
