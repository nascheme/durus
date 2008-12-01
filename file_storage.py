"""
$URL$
$Id$
"""
from datetime import datetime
from durus.file import File
from durus.logger import log, is_logging
from durus.serialize import unpack_record, split_oids
from durus.shelf import Shelf
from durus.storage import Storage
from durus.utils import int8_to_str, str_to_int8, IntSet
from durus.utils import read, write, read_int8_str, write_int8_str
from durus.utils import read_int8, write_int8, ShortRead, iteritems
from durus.utils import write_int4, read_int4_str, write_int4_str
from durus.utils import dumps, loads, as_bytes, byte_string
from zlib import compress, decompress
import durus.connection


class FileStorage (Storage):

    def __new__(klass, filename=None, readonly=False, repair=False):
        if klass != FileStorage:
            return object.__new__(klass)
        # The caller did not provide a specific concrete class.
        # We will choose the concrete class from the available implementations,
        # and by examining the prefix string in the file itself.
        # The first class in the "implementations" list is the default.
        implementations = [ShelfStorage, FileStorage2]
        file = File(filename, readonly=readonly)
        storage_class = implementations[0]
        for implementation in implementations:
            if implementation.has_format(file):
                storage_class = implementation
                break
        file.close()
        return storage_class.__new__(storage_class)

    def __str__(self):
        return '%s(%r)' % (self.__class__.__name__, self.get_filename())


def TempFileStorage():
    """
    This is just a more explicit way of opening a storage that uses a temporary
    file for the data.  This type of storage can be useful for testing.
    """
    return FileStorage()


class ShelfStorage (FileStorage):
    """
    A ShelfStorage us a FileStorage that uses the Shelf format for the data.
    The offset index of a shelf is stored in a format that makes it usable
    directly from the disk.  This offers some advantage in startup time
    and memory usage.

    This FileStorage implementation is experimental in Durus 3.7,
    and standard in Durus 3.8.

    Instance attributes:
      shelf : Shelf
        Contains the stored records.  This wraps a file.
        See Shelf docs for details of the file format.
      allocated_unused_oids : set([string])
        Contains the oids that have been allocated but not yet used.
      pending_records : { oid:str : record:str }
        Object records are accumulated here during a commit.
      pack_extra : [oid:str] | None
        oids of objects that have been committed after the pack began.  It is
        None if a pack is not in progress.
      invalid : set([oid:str])
        set of oids removed by packs since the last call to sync().
    """
    def __init__(self, filename=None, readonly=False, repair=False):
        self.shelf = Shelf(filename, readonly=readonly, repair=repair)
        self.pending_records = {}
        self.allocated_unused_oids = set()
        self.pack_extra = None
        self.invalid = set()

    @classmethod
    def has_format(klass, file):
        """(File) -> bool
        Does the given file contain the expected header string?
        """
        return Shelf.has_format(file)

    def get_filename(self):
        """() -> str
        Returns the full path name of the file that contains the data.
        """
        return self.shelf.get_file().get_name()

    def load(self, oid):
        """(str) -> str"""
        result = self.shelf.get_value(oid)
        if result is None:
            raise KeyError(oid)
        return result

    def begin(self):
        self.pending_records.clear()

    def store(self, oid, record):
        """(str, str)"""
        self.pending_records[oid] = record
        if (oid not in self.allocated_unused_oids and
            oid not in self.shelf and
            oid != int8_to_str(0)):
            self.begin()
            raise ValueError("oid %r is a surprise" % oid)

    def end(self, handle_invalidations=None):
        self.shelf.store(iteritems(self.pending_records))
        if is_logging(20):
            shelf_file = self.shelf.get_file()
            shelf_file.seek_end()
            pos = shelf_file.tell()
            log(20, "Transaction at [%s] end=%s" % (datetime.now(), pos))
        if self.pack_extra is not None:
            self.pack_extra.update(self.pending_records)
        self.allocated_unused_oids -= set(self.pending_records)
        self.begin()

    def sync(self):
        """() -> [str]
        """
        result = list(self.invalid)
        self.invalid.clear()
        return result

    def gen_oid_record(self, start_oid=None, **other):
        if start_oid is None:
            for item in iteritems(self.shelf):
                yield item
        else:
            todo = [start_oid]
            seen = IntSet() # This eventually contains them all.
            while todo:
                oid = todo.pop()
                if str_to_int8(oid) in seen:
                    continue
                seen.add(str_to_int8(oid))
                record = self.load(oid)
                record_oid, data, refdata = unpack_record(record)
                assert oid == record_oid
                todo.extend(split_oids(refdata))
                yield oid, record

    def new_oid(self):
        while True:
            name = self.shelf.next_name()
            if name not in self.allocated_unused_oids:
                self.allocated_unused_oids.add(name)
                return name

    def get_packer(self):
        assert not self.shelf.get_file().is_temporary()
        assert not self.shelf.get_file().is_readonly()
        if self.pending_records or self.pack_extra is not None:
            return () # Don't pack.
        self.pack_extra = set()
        file_path = self.shelf.get_file().get_name()
        file = File(file_path + '.pack')
        file.truncate() # obtains lock and clears.
        assert file.tell() == 0
        def packer():
            yield "started %s" % datetime.now()
            items = self.gen_oid_record(start_oid=int8_to_str(0))
            for step in Shelf.generate_shelf(file, items):
                yield step
            file.flush()
            file.fsync()
            shelf = Shelf(file)
            yield "base written %s" % datetime.now()
            for j, oid in enumerate(self.shelf):
                yield j
                if shelf.get_position(oid) is None:
                    self.invalid.add(oid)
            yield "invalidations identified %s" % datetime.now()
            n = len(self.pack_extra)
            while self.pack_extra:
                n -= 1
                first = []
                while len(self.pack_extra) > n:
                    first.append(self.pack_extra.pop())
                shelf.store(
                    (name, self.shelf.get_value(name)) for name in first)
                file.flush()
                file.fsync()
                yield len(self.pack_extra)
            yield "extra written %s" % datetime.now()
            # Now we are caught up.  Close the deal.
            file.flush()
            file.fsync()
            yield "file completed %s" % datetime.now()
            if not self.shelf.get_file().is_temporary():
                self.shelf.get_file().rename(file_path + '.prepack')
                self.shelf.get_file().close()
            shelf.get_file().rename(file_path)
            self.shelf = shelf
            self.pack_extra = None
            yield "finished %s" % datetime.now()
        return packer()


    def pack(self):
        for iteration in self.get_packer():
            pass

    def close(self):
        self.shelf.close()


class FileStorage2 (FileStorage):
    """
    This is the standard FileStorage implementation in Durus 3.7,
    and deprecated in Durus 3.8.

    Instance attributes:
      fp : file
      index : { oid:string : offset:int }
        Gives the offset of the current version of each oid.
      pending_records : { oid:str : record:str }
        Object records are accumulated here during a commit.
      pack_extra : [oid:str] | None
        oids of objects that have been committed after the pack began.  It is
        None if a pack is not in progress.

     Abbreviations:

         int4: a 4 byte unsigned big-endian int8
         int8: a 8 byte unsigned big-endian int8
         oid: object identifier (str_to_int8)

     The file format is as follows:

       1) a 6-byte distinguishing "magic" string
       2) the offset to the start of the index record (int8)
       3) zero or more transaction records
       4) the index record
       5) zero or more transaction records

     The index record consists of:

       1) the number of bytes in rest of the record (int8)
       2) a zlib compressed pickle of a dictionary.  The dictionary maps
          oids to file offsets for all objects records that preceed the
          index in the file.

     A transaction record consists of:

       1) zero of more object records
       2) int4 zero (i.e. 4 null bytes)

     An object record consists of:

       1) the number of bytes in rest of the record (int4)
       2) an oid (int8)
       3) the number of bytes in the following field.
       4) the pickle of the object's class followed by the zlib compressed
          pickle of the object's state.  These pickles are produced in
          sequence using the same pickler, with pickle protocol 2.
       5) a sequence of oids of persistent objects referenced in the pickled
          object state.  It is possible to collect these by unpickling the
          object state, but they are included directly here for faster access.
    """

    MAGIC = as_bytes("DFS20\0")

    _PACK_INCREMENT = 20 # number of records to pack before yielding

    def __init__(self, filename=None, readonly=False, repair=False):
        """(filename:str=None, readonly:bool=False, repair:bool=False)
        If filename is empty (or None), a temporary file will be used.
        """
        self.oid = -1
        self.fp = File(filename, readonly=readonly)
        self.pending_records = {}
        self.pack_extra = None

        self.fp.seek(0, 2)
        if self.fp.tell() != 0:
            assert self.has_format(self.fp)
        else:
            # Write header for new file.
            self.fp.seek(len(self.MAGIC))
            self._write_header(self.fp)
            self._write_index(self.fp, {})

        self.index = {}
        self._build_index(repair)
        max_oid = -1
        for oid in self.index:
            max_oid = max(max_oid, str_to_int8(oid))
        self.oid = max_oid
        self.invalid = set()

    @classmethod
    def has_format(klass, file):
        file.seek(0)
        try:
            if klass.MAGIC == read(file, len(klass.MAGIC)):
                return True
        except ShortRead:
            pass
        return False

    def _write_header(self, fp):
        fp.seek(0, 2)
        assert fp.tell() == 0
        write(fp, self.MAGIC)
        write_int8(fp, 0) # index offset

    def new_oid(self):
        self.oid += 1
        return int8_to_str(self.oid)

    def load(self, oid):
        offset = self.index[oid]
        self.fp.seek(offset)
        return self._read_block()

    def begin(self):
        pass

    def store(self, oid, record):
        """Add a record during a commit."""
        self.pending_records[oid] = record

    def _generate_pending_records(self):
        for oid, record in iteritems(self.pending_records):
            yield oid, record

    def end(self, handle_invalidations=None):
        """Complete a commit.
        """
        index = {}
        for z in self._write_transaction(
            self.fp, self._generate_pending_records(), index):
            pass
        self.fp.flush()
        self.fp.fsync()
        self.index.update(index)
        if self.pack_extra is not None:
            self.pack_extra.extend(index)
        self.pending_records.clear()

    def sync(self):
        """() -> [str]
        """
        result = list(self.invalid)
        self.invalid.clear()
        return result

    def get_filename(self):
        """() -> str
        The name of the file.
        If a tempfile is being used, the name will change when it is packed.
        """
        return self.fp.get_name()

    def _write_transaction(self, fp, records, index):
        fp.seek(0, 2)
        for i, (oid, record) in enumerate(records):
            full_record = self._disk_format(record)
            index[oid] = fp.tell()
            write_int4_str(fp, full_record)
            if i % self._PACK_INCREMENT == 0:
                yield None
        write_int4(fp, 0) # terminator

    def _disk_format(self, record):
        return record

    def gen_oid_record(self, start_oid=None, batch_size=100):
        if start_oid is None:
            for oid, offset in iteritems(self.index):
                yield oid, self.load(oid)
        else:
            for item in Storage.gen_oid_record(
                self, start_oid=start_oid, batch_size=batch_size):
                yield item

    def _packer(self):
        name = self.fp.get_name()
        prepack_name = name + '.prepack'
        pack_name = name + '.pack'
        packed = File(pack_name)
        if len(packed) > 0:
            # packed contains data left from an incomplete pack attempt.
            packed.seek(0)
            packed.truncate()
        self._write_header(packed)
        def gen_reachable_records():
            ROOT_OID = durus.connection.ROOT_OID
            for oid, record in self.gen_oid_record(start_oid=ROOT_OID):
                yield oid, record
            while self.pack_extra:
                oid = self.pack_extra.pop()
                yield oid, self.load(oid)
        index = {}
        for z in self._write_transaction(
            packed, gen_reachable_records(), index):
            yield None
        self._write_index(packed, index)
        packed.flush()
        packed.fsync()
        if self.fp.is_temporary():
            self.fp.close()
        else:
            self.fp.rename(prepack_name)
        packed.rename(name)
        self.fp = packed
        for oid in self.index:
            if oid not in index:
                self.invalid.add(oid)
        self.index = index
        self.pack_extra = None

    def get_packer(self):
        """Return an incremental packer (a generator).  Each time next() is
        called, up to _PACK_INCREMENT records will be packed.  Note that the
        generator must be exhausted before calling get_packer() again.
        """
        assert not self.fp.is_temporary()
        assert not self.fp.is_readonly()
        if self.pack_extra is not None or self.pending_records:
            return ()
        else:
            self.pack_extra = []
            return self._packer()

    def pack(self):
        for z in self.get_packer():
            pass

    def close(self):
        self.fp.close()

    def _read_block(self):
        return read_int4_str(self.fp)

    def _build_index(self, repair):
        self.fp.seek(0)
        if read(self.fp, len(self.MAGIC)) != self.MAGIC:
            raise IOError("invalid storage (missing magic in %r)" % self.fp)
        index_offset = read_int8(self.fp)
        assert index_offset > 0
        self.fp.seek(index_offset)
        tmp_index = loads(decompress(read_int8_str(self.fp)))
        self.index = {}
        def oid_as_bytes(oid):
            if isinstance(oid, byte_string):
                return oid
            else:
                return oid.encode('latin1')
        for tmp_oid in tmp_index:
            self.index[oid_as_bytes(tmp_oid)] = tmp_index[tmp_oid]
        del tmp_index
        while 1:
            # Read one transaction each time here.
            oids = {}
            transaction_offset = self.fp.tell()
            try:
                while 1:
                    object_record_offset = self.fp.tell()
                    record = self._read_block()
                    if len(record) == 0:
                        break # normal termination
                    if len(record) < 12:
                        raise ShortRead("Bad record size")
                    oid = record[0:8]
                    oids[oid] = object_record_offset
                # We've reached the normal end of a transaction.
                self.index.update(oids)
                oids.clear()
            except (ValueError, IOError):
                if self.fp.tell() > transaction_offset:
                    if not repair:
                        raise
                    # The transaction was malformed. Attempt repair.
                    self.fp.seek(transaction_offset)
                    self.fp.truncate()
                break

    def _write_index(self, fp, index):
        index_offset = fp.tell()
        compressed = compress(dumps(index))
        write_int8_str(fp, compressed)
        fp.seek(len(self.MAGIC))
        write_int8(fp, index_offset)
        assert fp.tell() == len(self.MAGIC) + 8

