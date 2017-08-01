"""
$URL: svn+ssh://svn.mems-exchange.org/repos/trunk/durus/file_storage.py $
$Id: file_storage.py 31299 2008-11-19 19:52:31Z dbinger $
"""
from datetime import datetime
import heapq
from durus.file import File
from durus.logger import log, is_logging
from durus.serialize import unpack_record, split_oids
from durus.storage import Storage
from durus.utils import int8_to_str, str_to_int8, IntSet
from durus.utils import read, write, read_int8_str, write_int8_str
from durus.utils import read_int8, write_int8, ShortRead, iteritems
from durus.utils import write_int4, read_int4_str, write_int4_str
from durus.utils import dumps, loads, as_bytes, byte_string
from zlib import compress, decompress
import durus.connection


class FileStorage2(Storage):
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

    def __init__(self, file=None, readonly=False, repair=False):
        """(file:str=None, readonly:bool=False, repair:bool=False)
        If file is empty (or None), a temporary file will be used.
        """
        self.oid = -1
        if file is None:
            file = File()
            assert not readonly
            assert not repair
        elif not hasattr(file, 'seek'):
            file = File(file, readonly=readonly)
        if not readonly:
            file.obtain_lock()
        self.fp = file
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
        if is_logging(20):
            log(20, "Transaction at [%s] end=%s" % (datetime.now(),
                                                    self.fp.tell()))
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
        # find all reachable objects.  Note that when we yield, new
        # commits may happen and pack_extra will contain new or modified
        # OIDs.
        index = {}
        def gen_reachable_records():
            # we order the todo queue by file offset. The hope is that the
            # packed file will be mostly the same as the old file in order
            # to speed up the rsync delta process.
            default_rank = 2**64
            pack_todo = [(0, durus.connection.ROOT_OID)]
            while pack_todo or self.pack_extra:
                if self.pack_extra:
                    oid = self.pack_extra.pop()
                    # note we don't check 'index' because it could be an
                    # object that got updated since the pack began and in
                    # that case we have to write the new record to the pack
                    # file
                else:
                    rank, oid = heapq.heappop(pack_todo)
                    if oid in index:
                        # we already wrote this object record
                        continue
                record = self.load(oid)
                oid2, data, refdata = unpack_record(record)
                assert oid == oid2
                # ensure we have records for objects referenced
                for ref_oid in split_oids(refdata):
                    item = (self.index.get(ref_oid, default_rank), ref_oid)
                    heapq.heappush(pack_todo, item)
                yield (oid, record)
        for z in self._write_transaction(
            packed, gen_reachable_records(), index):
            yield None # incremental pack, allow clients to be served
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

    def create_from_records(self, oid_records):
        assert not self.fp.is_readonly()
        self.fp.seek(0)
        self.fp.truncate()
        self._write_header(self.fp)
        index = {}
        for z in self._write_transaction(self.fp, oid_records, index):
            pass
        self._write_index(self.fp, index)
        self.fp.flush()
        self.fp.fsync()
