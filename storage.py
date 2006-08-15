"""$URL$
$Id$
"""

from durus.serialize import unpack_record, split_oids, extract_class_name
from durus.utils import p64

class Storage(object):
    """
    This is the interface that Connection requires for Storage.
    """
    def __init__(self):
        raise RuntimeError("Storage is abstract")

    def load(self, oid):
        """Return the record for this oid.
        """
        raise NotImplementedError

    def begin(self):
        """
        Begin a commit.  
        """
        raise NotImplementedError

    def store(self, oid, record):
        """Include this record in the commit underway."""
        raise NotImplementedError

    def end(self, handle_invalidations=None):
        """Conclude a commit."""
        raise NotImplementedError

    def sync(self):
        """() -> [oid:str]
        Return a list of oids that should be invalidated.
        """
        raise NotImplementedError

    def gen_oid_record(self):
        """() -> sequence([oid:str, record:str])
        """
        raise NotImplementedError

    def new_oid(self):
        """() -> oid:str
        Return an unused oid.  Used by Connection for serializing new persistent
        instances.
        """
        raise NotImplementedError

    def get_packer(self):
        """
        Return an incremental packer (a generator).
        Used by StorageServer.
        """
        raise NotImplementedError

    def pack(self):
        """Remove obsolete records from the storage."""
        raise NotImplementedError

    def get_size(self):
        """() -> int | None
        Return the number of objects available, or None if the number is not known.
        """
        return None

    def bulk_load(self, oids):
        """(oids:sequence(oid:str)) -> sequence(record:str)
        """
        for oid in oids:
            yield self.load(oid)


def gen_referring_oid_record(storage, referred_oid):
    """(storage:Storage, referred_oid:str) -> sequence([oid:str, record:str])
    Generate oid, record pairs for all objects that include a
    reference to the `referred_oid`.
    """
    for oid, record in storage.gen_oid_record():
        if referred_oid in split_oids(unpack_record(record)[2]):
            yield oid, record

def gen_oid_class(storage, *classes):
    """(storage:Storage, classes:(str)) ->
        sequence([(oid:str, class_name:str)])
    Generate a sequence of oid, class_name pairs.
    If classes are provided, only output pairs for which the
    class_name is in `classes`.
    """
    for oid, record in storage.gen_oid_record():
        class_name = extract_class_name(record)
        if not classes or class_name in classes:
            yield oid, class_name

def get_census(storage):
    """(storage:Storage) -> {class_name:str, instance_count:int}"""
    result = {}
    for oid, class_name in gen_oid_class(storage):
        result[class_name] = result.get(class_name, 0) + 1
    return result

def get_reference_index(storage):
    """(storage:Storage) -> {oid:str : [referring_oid:str]}
    Return a full index giving the referring oids for each oid.
    This might be large.
    """
    result = {}
    for oid, record in storage.gen_oid_record():
        for ref in split_oids(unpack_record(record)[2]):
            result.setdefault(ref, []).append(oid)
    return result


class MemoryStorage (Storage):
    """
    A concrete Storage that keeps everything in memory.
    This may be useful for testing purposes.
    """
    def __init__(self):
        self.records = {}
        self.transaction = None
        self.oid = 0

    def new_oid(self):
        self.oid += 1
        return p64(self.oid)

    def load(self, oid):
        return self.records[oid]

    def begin(self):
        self.transaction = {}

    def store(self, oid, record):
        self.transaction[oid] = record

    def end(self, handle_invalidations=None):
        self.records.update(self.transaction)
        self.transaction = None

    def sync(self):
        return []

    def gen_oid_record(self):
        for oid, record in self.records.iteritems():
            yield oid, record
