"""
$URL$
$Id$
"""
import heapq
from durus.serialize import unpack_record, split_oids, extract_class_name
from durus.utils import int8_to_str
import durus.connection

class Storage (object):
    """
    This is the interface that Connection requires for Storage.
    """
    def __init__(self):
        raise RuntimeError("Storage is abstract")

    def load(self, oid):
        """Return the record for this oid.
        Raises a KeyError if there is no such record.
        May also raise a ReadConflictError.
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
        """Conclude a commit.
        This may raise a ConflictError.
        """
        raise NotImplementedError

    def sync(self):
        """() -> [oid:str]
        Return a list of oids that should be invalidated.
        """
        raise NotImplementedError

    def new_oid(self):
        """() -> oid:str
        Return an unused oid.  Used by Connection for serializing new persistent
        instances.
        """
        raise NotImplementedError

    def close(self):
        """Clean up as needed.
        """

    def get_packer(self):
        """
        Return an incremental packer (a generator), or None if this storage
        does not support incremental packing.
        Used by StorageServer.
        """
        return None

    def pack(self):
        """If this storage supports it, remove obsolete records."""
        return None

    def bulk_load(self, oids):
        """(oids:sequence(oid:str)) -> sequence(record:str)
        """
        for oid in oids:
            yield self.load(oid)

    def gen_oid_record(self, start_oid=None, batch_size=100):
        """(start_oid:str = None, batch_size:int = 100) ->
            sequence((oid:str, record:str))
        Returns a generator for the sequence of (oid, record) pairs.

        If a start_oid is given, the resulting sequence follows a
        breadth-first traversal of the object graph, starting at the given
        start_oid.  This uses the storage's bulk_load() method because that
        is faster in some cases.  The batch_size argument sets the number
        of object records loaded on each call to bulk_load().

        If no start_oid is given, the sequence may include oids and records
        that are not reachable from the root.
        """
        if start_oid is None:
            start_oid = durus.connection.ROOT_OID
        todo = [start_oid]
        seen = set()
        while todo:
            batch = []
            while todo and len(batch) < batch_size:
                oid = heapq.heappop(todo)
                if oid not in seen:
                    batch.append(oid)
                    seen.add(oid)
            for record in self.bulk_load(batch):
                oid, data, refdata = unpack_record(record)
                yield oid, record
                for ref in split_oids(refdata):
                    if ref not in seen:
                        heapq.heappush(todo, ref)


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
        self.oid = -1

    def new_oid(self):
        self.oid += 1
        return int8_to_str(self.oid)

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

