__all__ = [
    "Storage",
]

from enum import IntEnum
from collections import namedtuple
from typing import List

from rangers.io import AbstractIO, Stream, Buffer
from rangers.blockpar import BlockPar


_header = namedtuple('DataBufHeader',
                     ('alloc_table_offset',
                      'arrays_number',
                      'element_type_size'))

_table_entry = namedtuple('DataBufAllocTableEntry',
                          ('offset',
                           'number',
                           'allocated_number'))


class StorageKind(IntEnum):
    INT32 = 0
    DWORD = 1
    BYTE = 2
    FLOAT = 3
    DOUBLE = 4
    WCHAR = 5

def get_kind(e):
    return StorageKind(e & ((1 << 31)-1)), (e & (1 << 31)) > 0

def set_kind(e, compressed: bool):
    if compressed:
        return (e & ((1 << 31)-1)) | (1 << 31)
    else:
        return e & ((1 << 31) - 1)

def get_size_by_kind(kind):
    if kind is StorageKind.DOUBLE:
        return 8
    elif kind is StorageKind.BYTE:
        return 1
    elif kind is StorageKind.WCHAR:
        return 2
    else:
        return 4


class DataTable:

    def __init__(self, el_size: int = 1):
        self._el_size = el_size
        self.entries: List[bytes] = []

    def load(self, s: AbstractIO, size: int):
        initpos = s.pos()
        header = _header(
            s.get_uint(),
            s.get_int(),
            s.get_int()
        )
        for i in range(header.arrays_number):
            header_size = 4*3  # uint, uint, int
            offset = initpos + header.alloc_table_offset + i*header_size
            s.seek(offset)
            entry = _table_entry(
                s.get_uint(),
                s.get_int(),
                s.get_int(),
            )
            start = initpos + entry.offset
            length = header.element_type_size*entry.number
            s.seek(start)
            self.entries.append(s.get(length))
        endpos = initpos + size
        s.seek(endpos)

    def save(self, s: AbstractIO):
        initpos = s.pos()
        s.add_uint(0)
        s.add_int(len(self.entries))
        s.add_int(self._el_size)
        datapos = s.pos()
        for entry in self.entries:
            s.add(entry)
        tablepos = s.pos()
        s.seek(initpos)
        s.add_uint(tablepos)
        s.seek(tablepos)
        for i in range(len(self.entries)):
            offset = 0 if i == 0 else len(self.entries[i-1])
            s.add_uint(datapos + offset)
            number = len(self.entries[i]) // self._el_size
            s.add_int(number)
            s.add_int(number)

    def get_buf(self, i: int) -> Buffer:
        return Buffer.from_bytes(self.entries[i])

    def get_widestr(self, i: int) -> str:
        return self.entries[i].decode('utf-16le')


class StorageItem:

    def __init__(self, name='', kind=StorageKind.BYTE, datatable=None):
        self.name: str = name
        self.kind, compressed = get_kind(kind)
        self.datatable: DataTable = datatable

    def get(self) -> DataTable:
        return self.datatable

    def load(self, s: AbstractIO):
        self.name = s.get_widestr()
        self.kind, compressed = get_kind(s.get_uint())
        size = s.get_uint()

        if compressed:
            self.datatable = DataTable(get_size_by_kind(self.kind))
            tempbuf = Buffer.from_bytes(s.decompress(size))
            self.datatable.load(tempbuf, tempbuf.size())
            tempbuf.close()
            del tempbuf
        else:
            self.datatable = DataTable(get_size_by_kind(self.kind))
            self.datatable.load(s, size)

    def save(self, s: AbstractIO, compressed: bool = True):
        pass


class StorageRecord:

    def __init__(self, name='', items=None):
        self.name = name
        self.items = items if items is not None else []

    def add(self, item: StorageItem):
        self.items.append(item)

    def get(self, column: str) -> DataTable:
        if self.items is not None:
            for item in self.items:
                if item.name == column:
                    return item.get()

    def load(self, s: AbstractIO):
        self.name = s.get_widestr()
        for i in range(s.get_uint()):
            item = StorageItem()
            item.load(s)
            self.items.append(item)


class Storage:

    def __init__(self, records=None):
        self.records = records if records is not None else []

    def add(self, record: StorageRecord):
        self.records.append(record)

    def get(self, table: str, column: str) -> DataTable:
        if self.records is not None:
            for record in self.records:
                if record.name == table:
                    return record.get(column)

    def load(self, s: AbstractIO):
        magic = s.get(4)
        if magic != b'STRG':
            s.close()
            raise Exception("Storage.load: wrong magic")

        version = s.get_uint()
        if version > 1:
            s.close()
            raise Exception("Storage.load: wrong version")

        if version == 1:
            tempbuf = Buffer.from_bytes(s.decompress())
            s.close()
            s = tempbuf

        for i in range(s.get_uint()):
            record = StorageRecord()
            record.load(s)
            self.records.append(record)

    def restore_blockpar(self, root: str) -> BlockPar:
        bp = BlockPar()

        keys = self.get(root, '0')
        values = self.get(root, '1')
        for i in range(len(keys.entries)):
            bp.add(keys.get_widestr(i), values.get_widestr(i))

        keys = self.get(root, '2')
        values = self.get(root, '3')
        for i in range(len(keys.entries)):
            bp.add(keys.get_widestr(i),
                   self.restore_blockpar(values.get_widestr(i)))

        return bp

    @classmethod
    def from_file(cls, path: str) -> 'Storage':
        storage = cls()
        with Stream.from_file(path, 'rb') as s:
            storage.load(s)
        return storage
