"""
$URL$
$Id$
"""
from cPickle import dumps, loads
from durus.connection import ROOT_OID
from durus.serialize import split_oids, unpack_record
from durus.storage import Storage
from durus.utils import p32, u32, p64, u64
from tempfile import NamedTemporaryFile
from zlib import compress, decompress
import os

if os.name == 'posix':
    import fcntl
    def lock_file(fp):
        fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    def unlock_file(fp):
        pass
    RENAME_OPEN_FILE = True
elif os.name == 'nt':
    import win32con, win32file, pywintypes # http://sf.net/projects/pywin32/
    def lock_file(fp):
        win32file.LockFileEx(win32file._get_osfhandle(fp.fileno()),
                             (win32con.LOCKFILE_EXCLUSIVE_LOCK |
                              win32con.LOCKFILE_FAIL_IMMEDIATELY),
                             0, -65536, pywintypes.OVERLAPPED())
    def unlock_file(fp):
        win32file.UnlockFileEx(win32file._get_osfhandle(fp.fileno()),
                               0, -65536, pywintypes.OVERLAPPED())
    RENAME_OPEN_FILE = False
else:
    def lock_file(fp):
        raise RuntimeError("Sorry, don't know how to lock files on your OS")
    def unlock_file(fp):
        pass
    RENAME_OPEN_FILE = False

if hasattr(os, 'fsync'):
    fsync = os.fsync
else:
    def fsync(fd):
        pass


class FileStorage(Storage):
    """
    Instance attributes:
      fp : file
      index : { oid:string : offset:int }
        Gives the offset of the current version of each oid.
      pending_records : { oid:str : record:str }
        Object records are accumulated here during a commit.
      pack_extra : [oid:str] | None
        oids of objects that have been committed after the pack began.  It is
        None if a pack is not in progress.
    """

    _PACK_INCREMENT = 20 # number of records to pack before yielding

    def __init__(self, filename=None, readonly=False, repair=False):
        """(filename:str=None, readonly:bool=False, repair:bool=False)
        If filename is empty (or None), a temporary file will be used.
        """
        self.oid = 0
        self.filename = filename
        if readonly:
            if not filename:
                raise ValueError(
                    "A filename is required for a readonly storage.")
            if repair:
                raise ValueError("A readonly storage can't be repaired.")
            self.fp = open(self.filename, 'rb')
        else:
            if not filename:
                self.fp = NamedTemporaryFile(suffix=".durus", mode="w+b")
            elif (os.path.exists(self.filename) and
                  os.stat(self.filename).st_size > 0):
                self.fp = open(self.filename, 'a+b')
            else:
                self.fp = open(self.filename, 'w+b')
            try:
                lock_file(self.fp)
            except IOError:
                self.fp.close()
                raise RuntimeError(
                    "\n  %s is locked."
                    "\n  There is probably a Durus storage server (or a client)"
                    "\n  using it.\n" % self.get_filename())
        self.pending_records = {}
        self.pack_extra = None
        self.repair = repair
        self._set_concrete_class_for_magic()
        self.index = {}
        self._build_index()
        max_oid = 0
        for oid in self.index:
            max_oid = max(max_oid, u64(oid))
        self.oid = max_oid

    def _set_concrete_class_for_magic(self):
        """
        FileStorage is an abstract class.
        The constructor calls this to set self.__class__ to a subclass
        that matches the format of the underlying file.
        If the underlying file is empty, this writes the magic
        string into the file.
        """
        for format in (FileStorage1, FileStorage2):
            self.fp.seek(0)
            self.__class__ = format
            if format.MAGIC == self.fp.read(len(format.MAGIC)):
                return
        # Write header for new FileStorage2 file.
        self.fp.seek(0, 2)
        if self.fp.tell() != 0:
             raise IOError, "%r has no FileStorage magic" % self.fp
        self._write_header(self.fp)
        self._write_index(self.fp, {})

    def _write_header(self, fp):
        fp.seek(0, 2)
        assert fp.tell() == 0
        fp.write(self.MAGIC)

    def _write_index(self, fp, index):
        pass

    def get_size(self):
        return len(self.index)

    def new_oid(self):
        self.oid += 1
        return p64(self.oid)

    def load(self, oid):
        if self.fp is None:
            raise IOError, 'storage is closed'
        offset = self.index[oid]
        self.fp.seek(offset)
        return self._read_block()

    def begin(self):
        pass

    def store(self, oid, record):
        """Add a record during a commit."""
        self.pending_records[oid] = record

    def _generate_pending_records(self):
        for oid, record in self.pending_records.iteritems():
            yield oid, record

    def end(self, handle_invalidations=None):
        """Complete a commit.
        """
        if self.fp is None:
            raise IOError, 'storage is closed'
        index = {}
        for z in self._write_transaction(
            self.fp, self._generate_pending_records(), index):
            pass
        self.fp.flush()
        fsync(self.fp)
        self.index.update(index)
        if self.pack_extra is not None:
            self.pack_extra.extend(index)
        self.pending_records.clear()

    def sync(self):
        """
        A FileStorage is the storage of one StorageServer or one
        Connection, so there can never be any invalidations to transfer.
        """
        return []

    def get_filename(self):
        """() -> str
        The name of the file.
        If a tempfile is being used, the name will change when it is packed.
        """
        return self.filename or self.fp.name

    def _write_transaction(self, fp, records, index):
        fp.seek(0, 2)
        for i, (oid, record) in enumerate(records):
            full_record = self._disk_format(record)
            index[oid] = fp.tell()
            fp.write(p32(len(full_record)))
            fp.write(full_record)
            if i % self._PACK_INCREMENT == 0:
                yield None
        fp.write(p32(0)) # terminator

    def _disk_format(self, record):
        return record

    def _packer(self):
        if self.filename:
            prepack_name = self.filename + '.prepack'
            pack_name = self.filename + '.pack'
            packed = open(pack_name, 'w+b')
        else:
            packed = NamedTemporaryFile(suffix=".durus",
                                        mode="w+b")
        lock_file(packed)
        self._write_header(packed)
        def gen_reachable_records():
            todo = [ROOT_OID]
            seen = set()
            while todo:
                oid = todo.pop()
                if oid in seen:
                    continue
                seen.add(oid)
                record = self.load(oid)
                record_oid, data, refdata = unpack_record(record)
                assert oid == record_oid
                todo.extend(split_oids(refdata))
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
        fsync(packed)
        if self.filename:
            if not RENAME_OPEN_FILE:
                unlock_file(packed)
                packed.close()
            unlock_file(self.fp)
            self.fp.close()
            if os.path.exists(prepack_name): # for Win32
                os.unlink(prepack_name)
            os.rename(self.filename, prepack_name)
            os.rename(pack_name, self.filename)
            if RENAME_OPEN_FILE:
                self.fp = packed
            else:
                self.fp = open(self.filename, 'r+b')
                lock_file(self.fp)
        else: # tempfile
            unlock_file(self.fp)
            self.fp.close()
            self.fp = packed
        self.index = index
        self.pack_extra = None

    def get_packer(self):
        """Return an incremental packer (a generator).  Each time next() is
        called, up to _PACK_INCREMENT records will be packed.  Note that the
        generator must be exhausted before calling get_packer() again.
        """
        if self.fp is None:
            raise IOError, 'storage is closed'
        if self.fp.mode == 'rb':
            raise IOError, "read-only storage"
        assert not self.pending_records
        assert self.pack_extra is None
        self.pack_extra = []
        return self._packer()

    def pack(self):
        for z in self.get_packer():
            pass

    def gen_oid_record(self):
        """() -> sequence([(oid:str, record:str)])
        Generate oid, record pairs, for all oids in the database.
        Note that this may include oids that are not reachable from
        the root object.
        """
        for oid in self.index:
            yield oid, self.load(oid)

    def close(self):
        if self.fp is not None:
            unlock_file(self.fp)
            self.fp.close()
            self.fp = None

    def _read_block(self):
        size_str = self.fp.read(4)
        if len(size_str) == 0:
            raise IOError, "eof"
        size = u32(size_str)
        if size == 0:
            return ''
        result = self.fp.read(size)
        if len(result) != size:
            raise IOError, "short read"
        return result


class FileStorage1(FileStorage):
    """
    The file consists of a 6-byte distinguishing "magic" string followed
    by a sequence of transaction records.  Each transaction record is a
    sequence of object records followed by a 4-byte zero terminator.
    Object records have the following structure:
      1) The number of bytes in the next four components of the object
         record (4 bytes, unsigned, big-endian).
      2) The transaction identifier (8 bytes, unsigned, big-endian).
         This will not necessarily be the same for every object record in
         the transaction record (due to packing).
      3) The object identifier (8 bytes, unsigned, big-endian).
      4) The number of bytes in the pickled object state (4 bytes, unsigned,
         big-endian).
      5) The pickled object state.  This is normally a the pickle of
         the object's __dict__, pickled using protocol 2, with a
         customized pickler that stores oid and class for each
         reference to a persistent object.
      6) A sequence of object identifiers (each 8 bytes, unsigned, big-endian)
         of objects with references in the pickled object state.
         These referenced object identifiers can be collected directly
         while unpickling pickled object state, but they are included
         directly here for faster access.

    Instance attributes:
      tid: str
        8 byte transaction identifier.
        """

    MAGIC = "DFS10\0"

    def __init__(self, *args, **kwargs):
        FileStorage.__init__(self, *args, **kwargs)
        self.tid = 0

    def _build_index(self):
        self.index = {}
        self.fp.seek(0)
        if self.fp.read(len(self.MAGIC)) != self.MAGIC:
            raise IOError, "invalid storage (missing magic in %r)" % self.fp
        max_tid = 0
        while 1:
            # Read one transaction each time here.
            transaction_offset = self.fp.tell()
            oids = {}
            try:
                while 1:
                    object_record_offset = self.fp.tell()
                    trecord = self._read_block()
                    if len(trecord) == 0:
                        break # normal termination
                    if len(trecord) < 16:
                        raise ValueError("Bad record size")
                    tid = trecord[0:8]
                    oid = trecord[8:16]
                    max_tid = max(max_tid, u64(tid))
                    oids[oid] = object_record_offset
                # We've reached the normal end of a transaction.
                self.index.update(oids)
                oids.clear()
            except (ValueError, IOError), exc:
                if self.fp.tell() > transaction_offset:
                    if not self.repair:
                        raise
                    # The transaction was malformed. Attempt repair.
                    if self.fp.mode == 'r':
                        raise RuntimeError(
                            "Can't repair readonly file.\n%s" % exc)
                    self.fp.seek(transaction_offset)
                    self.fp.truncate()
                break
        self.tid = max_tid

    def _disk_format(self, record):
        return p64(self.tid) + record

    def begin(self):
        """Begin a commit."""
        self.tid += 1

    def load(self, oid):
        return FileStorage.load(self, oid)[8:] # just strip the tid.


class FileStorage2(FileStorage):
    """
     Abbreviations:

         u32: a 4 byte unsigned big-endian integer
         u64: a 8 byte unsigned big-endian integer
         oid: object identifier (u64)

     The file format is as follows:

       1) a 6-byte distinguishing "magic" string
       2) the offset to the start of the index record (u64)
       3) zero or more transaction records
       4) the index record
       5) zero or more transaction records

     The index record consists of:

       1) the number of bytes in rest of the record (u64)
       2) a zlib compressed pickle of a dictionary.  The dictionary maps
          oids to file offsets for all transactions that preceed the
          index in the file.

     A transaction record consists of:

       1) zero of more object records
       2) a u32 zero (i.e. 4 null bytes)

     An object record consists of:

       1) the number of bytes in rest of the record (u32)
       2) an oid (u64)
       3) the number of bytes in the following field.
       4) the pickle of the object's class followed by the zlib compressed
          pickle of the object's state.  These pickles are produced in
          sequence using the same pickler, with pickle protocol 2.
       5) a sequence of oids of persistent objects referenced in the pickled
          object state.  It is possible to collect these by unpickling the
          object state, but they are included directly here for faster access.
    """

    MAGIC = "DFS20\0"

    def _write_header(self, fp):
        FileStorage._write_header(self, fp)
        fp.write(p64(0)) # index offset

    def _build_index(self):
        self.fp.seek(0)
        if self.fp.read(len(self.MAGIC)) != self.MAGIC:
            raise IOError, "invalid storage (missing magic in %r)" % self.fp
        index_offset = u64(self.fp.read(8))
        assert index_offset > 0
        self.fp.seek(index_offset)
        index_size = u64(self.fp.read(8))
        self.index = loads(decompress(self.fp.read(index_size)))
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
                        raise ValueError("Bad record size")
                    oid = record[0:8]
                    oids[oid] = object_record_offset
                # We've reached the normal end of a transaction.
                self.index.update(oids)
                oids.clear()
            except (ValueError, IOError), exc:
                if self.fp.tell() > transaction_offset:
                    if not self.repair:
                        raise
                    # The transaction was malformed. Attempt repair.
                    if self.fp.mode == 'r':
                        raise RuntimeError(
                            "Can't repair readonly file.\n%s" % exc)
                    self.fp.seek(transaction_offset)
                    self.fp.truncate()
                break

    def _write_index(self, fp, index):
        index_offset = fp.tell()
        compressed = compress(dumps(index))
        fp.write(p64(len(compressed)))
        fp.write(compressed)
        fp.seek(len(self.MAGIC))
        fp.write(p64(index_offset))
        assert fp.tell() == len(self.MAGIC) + 8


class TempFileStorage(FileStorage2):

    def __init__(self):
        FileStorage2.__init__(self)


