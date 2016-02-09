"""
$URL$
$Id$
"""
from datetime import datetime
import heapq
from durus.error import DurusKeyError
from durus.file import File
from durus.logger import log, is_logging
from durus.serialize import unpack_record, split_oids
from durus.shelf import Shelf
from durus.storage import Storage
from durus.utils import int8_to_str, str_to_int8, IntSet, iteritems
import durus.connection


class FileStorage (Storage):
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
            raise DurusKeyError(oid)
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

    def gen_oid_record(self, start_oid=None, seen=None, **other):
        if start_oid is None:
            for item in iteritems(self.shelf):
                yield item
        else:
            todo = [start_oid]
            if seen is None:
                seen = IntSet() # This eventually contains them all.
            while todo:
                oid = heapq.heappop(todo)
                if str_to_int8(oid) in seen:
                    continue
                seen.add(str_to_int8(oid))
                record = self.load(oid)
                record_oid, data, refdata = unpack_record(record)
                assert oid == record_oid
                for ref_oid in split_oids(refdata):
                    heapq.heappush(todo, ref_oid)
                yield oid, record

    def new_oid(self):
        while True:
            name = self.shelf.next_name()
            if name in self.allocated_unused_oids:
                continue
            if name in self.invalid:
                continue
            self.allocated_unused_oids.add(name)
            return name

    def get_packer(self):
        if (self.pending_records or
            self.pack_extra is not None or
            self.shelf.get_file().is_temporary() or
            self.shelf.get_file().is_readonly()):
            return (x for x in []) # Don't pack.
        self.pack_extra = set()
        file_path = self.shelf.get_file().get_name()
        file = File(file_path + '.pack')
        file.truncate() # obtains lock and clears.
        assert file.tell() == 0
        def packer():
            yield "started %s" % datetime.now()
            seen = IntSet()
            items = self.gen_oid_record(start_oid=int8_to_str(0), seen=seen)
            for step in Shelf.generate_shelf(file, items):
                yield step
            file.flush()
            file.fsync()
            shelf = Shelf(file)
            yield "base written %s" % datetime.now()
            # Invalidate oids that have been removed.
            for hole in shelf.get_offset_map().gen_holes():
                yield hole
                oid = int8_to_str(hole)
                if self.shelf.get_position(oid) is not None:
                    assert shelf.get_position(oid) is None
                    self.invalid.add(oid)
            yield "invalidations identified %s" % datetime.now()
            for oid in self.pack_extra:
                seen.discard(str_to_int8(oid))
            for oid in self.pack_extra:
                shelf.store(self.gen_oid_record(start_oid=oid, seen=seen))
            file.flush()
            file.fsync()
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

    def __str__(self):
        return '%s(%r)' % (self.__class__.__name__, self.get_filename())


def TempFileStorage():
    """
    This is just a more explicit way of opening a storage that uses a temporary
    file for the data.  This type of storage can be useful for testing.
    """
    return FileStorage()
