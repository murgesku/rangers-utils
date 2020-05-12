from struct import unpack
from enum import IntEnum
from collections import namedtuple

from .iolib import Stream, Buffer
from .blockpar import BlockPar


_header = namedtuple('DataBufHeader',
                     ('alloc_table_offset',
                      'arrays_number',
                      'element_type_size'))

_table_entry = namedtuple('DataBufAllocTableEntry',
                          ('offset',
                           'number',
                           'allocated_number'))


DataTableEntry = namedtuple('DataTableEntry',
                            ('count',
                             'array'))


class StorageKind(IntEnum):
    INT32 = 0
    DWORD = 1
    BYTE = 2
    FLOAT = 3
    DOUBLE = 4
    WCHAR = 5

def get_kind(e):
    return StorageKind(e & ((1 << 31)-1)), (e & (1 << 31)) > 0

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

    def __init__(self, kind=StorageKind(0)):
        self.header = _header(4*3,  # header size: uint, uint, int; 32 bit
                              0,
                              get_size_by_kind(kind))
        self.entries = []

    def load(self, s, sz):
        """
        :type s: AbstractIO
        :type sz: int
        """
        initpos = s.pos()
        self.header = _header(
            s.get_uint(),
            s.get_uint(),
            s.get_int()
        )
        for i in range(self.header.arrays_number):
            offset = initpos + self.header.alloc_table_offset + i*4*3  # header size: uint, uint, uint; 32 bit
            s.seek(offset)
            entry = _table_entry(
                s.get_uint(),
                s.get_uint(),
                s.get_uint(),
            )
            start = initpos + entry.offset
            length = self.header.element_type_size*entry.number
            s.seek(start)
            self.entries.append(DataTableEntry(entry.number, s.get(length)))
        endpos = initpos + sz
        s.seek(endpos)

    def get_widestr(self, i):
        """
        :type i: int
        :rtype: str
        """
        return self.entries[i].array.decode('utf-16le')


class StorageItem:

    def __init__(self, name='', kind=0, datatable=None):
        self.name = name
        self.kind, self.compressed = get_kind(kind)
        self.datatable = datatable

    def get(self):
        """
        :rtype: DataTable
        """
        return self.datatable

    def load(self, s):
        """
        :type s: AbstractIO
        """
        self.name = s.get_widestr()
        self.kind, self.compressed = get_kind(s.get_uint())
        size = s.get_uint()

        if self.compressed:
            self.datatable = DataTable()
            tempbuf = Buffer.from_bytes(s.decompress(size))
            self.datatable.load(tempbuf, tempbuf.size())
            tempbuf.close()
            del tempbuf
        else:
            self.datatable = DataTable()
            self.datatable.load(s, size)


class StorageRecord:

    def __init__(self, name='', items=None):
        self.name = name
        self.items = items if items is not None else []

    def add(self, item):
        """
        :type item: StorageItem
        """
        self.items.append(item)

    def get(self, column):
        """
        :type column: str
        :rtype: DataTable
        """
        if self.items is not None:
            for item in self.items:
                if item.name == column:
                    return item.get()

    def load(self, s):
        """
        :type s: AbstractIO
        """
        self.name = s.get_widestr()
        for i in range(s.get_uint()):
            item = StorageItem()
            item.load(s)
            self.items.append(item)


class Storage:

    def __init__(self, records=None):
        self.records = records if records is not None else []

    def add(self, record):
        """
        :type record: StorageRecord
        """
        self.records.append(record)

    def get(self, table, column):
        """
        :type table: str
        :type column: str
        :rtype: DataTable
        """
        if self.records is not None:
            for record in self.records:
                if record.name == table:
                    return record.get(column)

    def load(self, s):
        """
        :type s: AbstractIO
        """
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

    def restore_blockpar(self, root):
        """
        :type root: str
        :rtype: BlockPar
        """
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