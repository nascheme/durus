#!/www/python/bin/python
"""$URL$
$Id$
"""
import atexit
import os
from tempfile import mkstemp
from sets import Set
from durus.connection import ROOT_OID
from durus.serialize import split_oids, unpack_record
from durus.storage import Storage
from durus.utils import p32, u32, p64, u64

def readn(fp, n):
    """Read 'n' bytes from a file.  Raises IOError if fewer bytes than 'n'
    bytes are available."""
    s = fp.read(n)
    if len(s) != n:
        raise IOError, 'short read'
    return s

MAGIC = "DFS10\0"

if os.name == 'posix':
    import fcntl
    def lock_file(file):
        fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
elif os.name == 'nt':
    import win32con, win32file, pywintypes # http://sf.net/projects/pywin32/
    def lock_file(file):
        win32file.LockFileEx(win32file._get_osfhandle(file.fileno()),
                             (win32con.LOCKFILE_EXCLUSIVE_LOCK |
                              win32con.LOCKFILE_FAIL_IMMEDIATELY),
                             0, -65536, pywintypes.OVERLAPPED())
else:
    def lock_file(file):
        raise RuntimeError("File locking isn't available on your OS.")


class FileStorage(Storage):
    """

    The file contains a sequence of transaction records.
    Each transaction record is a sequence of object records
    followed by a 4-byte zero terminator.
    Object records have the following structure:
      1) The number of bytes in the next four components of the object
         record (4 bytes, unsigned, big-endian).
      2) The transaction identifier (8 bytes, unsigned, big-endian).
         This will be the same for every object record in the
         transaction record.
      3) The object identifier (8 bytes, unsigned, big-endian).
      4) The pickled object state.  This is normally a the pickle of
         the object's __dict__, pickled using protocol 2, with a
         customized pickler that stores oid and class for each
         reference to a persistent object.
      5) A sequence of object identifiers (each 8 bytes, unsigned, big-endian)
         of objects with references in the pickled object state.
         These referenced object identifiers can be collected directly
         while unpickling pickled object state, but they are included
         directly here for faster access.

    Instance attributes:
      fp : file
      index : { oid:string : offset:int }
        Gives the offset of the current version of each oid.
      pending_records : [str]
        Object records are accumulated here during a commit.
    """

    def __init__(self, filename, readonly=False, repair=False):
        """(filename:str, readonly:bool=False, repair:bool=False)
        """
        self.tid = 0
        self.oid = 0
        if readonly:
            self.fp = open(filename, 'r')
        else:
            self.fp = open(filename, 'a+')
            try:
                lock_file(self.fp)
            except IOError:
                self.fp.close()
                raise RuntimeError(
                    "\n  %s is locked."
                    "\n  There is probably a Durus storage server (or a client)"
                    "\n  using it.\n" % self.fp.name)
            if os.fstat(self.fp.fileno()).st_size == 0:
                self.fp.write(MAGIC)
        self.pending_records = []
        self.repair = repair
        self._build_index()
        #print '  len(index)==%s' % len(self.index)

    def _build_index(self):
        self.index = {}
        self.fp.seek(0)
        if self.fp.read(len(MAGIC)) != MAGIC:
            raise IOError, "invalid storage (missing magic)"
        max_tid = 0
        while 1:
            # Read one transaction each time here.
            transaction_offset = self.fp.tell()
            current_tid = None
            oids = {}
            try:
                while 1:
                    object_record_offset = self.fp.tell()
                    size = u32(readn(self.fp, 4))
                    if size == 0:
                        break # normal termination
                    if 0 < size < 16:
                        raise ValueError("Bad record size")
                    trecord = readn(self.fp, size)
                    tid = trecord[0:8]
                    oid = trecord[8:16]
                    if current_tid is None:
                        current_tid = tid
                    else:
                        if current_tid != tid:
                            raise ValueError("Bad tid")
                    if oid in oids:
                        raise ValueError("Object duplicated")
                    oids[oid] = object_record_offset
                # We've reached the normal end of a transaction.
                if current_tid is not None:
                    # The file was not empty.
                    self.index.update(oids)
                    max_tid = max(max_tid, u64(current_tid))
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
        max_oid = 0
        for oid in self.index:
            max_oid = max(max_oid, u64(oid))
        self.oid = max_oid

    def _get_tid(self):
        return p64(self.tid)

    def get_size(self):
        return len(self.index)

    def new_oid(self):
        self.oid += 1
        return p64(self.oid)

    def load(self, oid):
        offset = self.index[oid]
        self.fp.seek(offset)
        size = u32(self.fp.read(4))
        return self.fp.read(size)

    def begin(self):
        """Begin a commit."""
        self.tid += 1

    def store(self, record):
        """Add a record during a commit."""
        self.pending_records.append(record)

    def end(self, handle_invalidations=None):
        """Complete a commit.
        A FileStorage is the storage of one StorageServer or one
        Connection, so there can never be any invalidations to handle.
        """
        self.fp.seek(0, 2)
        tid, index = self._write_transaction(self.fp, self.pending_records)
        self.fp.flush()
        self.index.update(index)
        del self.pending_records[:]
        return tid

    def _write_transaction(self, file, records):
        tid = self._get_tid()
        index = {}
        for record in records:
            oid = record[:8]
            index[oid] = file.tell()
            trecord = tid + record
            file.write(p32(len(trecord)))
            file.write(trecord)
        file.write(p32(0)) # terminator
        return tid, index

    def sync(self):
        """() -> tid:str, invalidations:[str]
        A FileStorage is the storage of one StorageServer or one
        Connection, so there can never be any invalidations to transfer.
        """
        return self._get_tid(), []

    def _get_pack_names(self):
        """() -> prepack_name:str, pack_name:str
        Return the names of files used during a pack.
        The `prepack_name` is for a copy of the file as it was before
        packing.  The `pack_name` is used to hold the newly packed file.
        """
        return self.fp.name + '.prepack', self.fp.name + '.pack'

    def pack(self):
        """Perform a pack on the storage.
        All current object records reachable from the root object
        are moved into one big transaction record.
        """
        assert not self.pending_records
        if self.fp.mode == 'r':
            raise IOError, "read-only storage"
        prepack_name, pack_name = self._get_pack_names()
        packed = open(pack_name, 'w')
        packed.write(MAGIC)
        def generate_records():
            todo = [ROOT_OID]
            seen = Set()
            while todo:
                oid = todo.pop()
                if oid in seen:
                    continue
                seen.add(oid)
                record = self.load(oid)[8:]
                record_oid, data, refdata = unpack_record(record)
                assert oid == record_oid
                todo.extend(split_oids(refdata))
                yield record
        tid, index = self._write_transaction(packed, generate_records())
        packed.close()
        self.fp.close()
        file_name = self.fp.name
        os.rename(file_name, prepack_name)
        os.rename(packed.name, file_name)
        self.fp = open(file_name, 'a+')
        self.index = index

    def gen_oid_record(self):
        """() -> sequence([(oid:str, record:str)])
        Generate oid, record pairs, for all oids in the database.
        Note that this may include oids that are not reachable from
        the root object.
        """
        for oid in self.index:
            yield oid, self.load(oid)


class TempFileStorage(FileStorage):

    def __init__(self):
        (fd, name) = mkstemp(suffix='.tmp.durus')
        FileStorage.__init__(self, name)
        def remove_file():
            os.unlink(self.fp.name)
        atexit.register(remove_file)
        os.close(fd)

    def pack(self):
        FileStorage.pack(self)
        for name in self._get_pack_names():
            if os.path.exists(name):
                os.unlink(name)


