"""$URL$
$Id$
"""
from cPickle import loads
from heapq import heappush, heappop
from durus.error import ConflictError, ReadConflictError, DurusKeyError
from durus.logger import log
from durus.persistent import ConnectionBase
from durus.persistent_dict import PersistentDict
from durus.serialize import ObjectReader, ObjectWriter
from durus.serialize import unpack_record, pack_record
from durus.storage import Storage
from durus.utils import p64
from itertools import islice, chain
from os import getpid
from sets import Set
from time import time
from weakref import ref

ROOT_OID = p64(0)

class Connection(ConnectionBase):
    """
    The Connection manages movement of objects in and out of storage.

    Instance attributes:
      storage: Storage
      cache: Cache
      reader: ObjectReader
      changed: {oid:str : Persistent}
      invalid_oids: Set([str])
         Set of oids of objects known to have obsolete state. 
      sync_count: int
        Number of calls to commit() or abort() since this instance was created.
    """

    def __init__(self, storage, cache_size=100000):
        """(storage:Storage, cache_size:int=100000)
        Make a connection to `storage`.
        Set the target number of non-ghosted persistent objects to keep in
        the cache at `cache_size`.
        """
        assert isinstance(storage, Storage)
        self.storage = storage
        self.reader = ObjectReader(self)
        self.changed = {}
        self.invalid_oids = Set()
        self.sync_count = 0
        try:
            storage.load(ROOT_OID)
        except KeyError:
            self.storage.begin()
            writer = ObjectWriter(self)
            data, refs = writer.get_state(PersistentDict())
            writer.close()
            self.storage.store(ROOT_OID, pack_record(ROOT_OID, data, refs))
            self.storage.end(self._handle_invalidations)
            self.sync_count += 1
        self.new_oid = storage.new_oid # needed by serialize
        self.cache = Cache(cache_size)

    def get_storage(self):
        """() -> Storage"""
        return self.storage

    def get_cache_count(self):
        """() -> int
        Return the number of Persistent instances currently in the cache.
        """
        return self.cache.get_count()

    def get_cache_size(self):
        """() -> cache_size:int
        Return the target size for the cache.
        """
        return self.cache.get_size()

    def set_cache_size(self, size):
        """(size:int)
        Set the target size for the cache.        
        """
        self.cache.set_size(size)

    def get_sync_count(self):
        """() -> int
        Return the number of calls to commit() or abort() on this instance.
        """
        return self.sync_count

    def get_root(self):
        """() -> Persistent
        Returns the root object.
        """
        return self.get(ROOT_OID)

    def get_stored_pickle(self, oid):
        """(oid:str) -> str
        Retrieve the pickle from storage.  Will raise ReadConflictError if
        pickle the pickle is invalid.
        """
        if oid in self.invalid_oids:
            # someone is still trying to read after getting a conflict
            raise ReadConflictError([oid])
        try:
            record = self.storage.load(oid)
        except ReadConflictError:
            invalid_oids = self.storage.sync()
            self._handle_invalidations(invalid_oids, read_oid=oid)
            record = self.storage.load(oid)
        oid2, data, refdata = unpack_record(record)
        assert oid == oid2
        return data

    def get(self, oid):
        """(oid:str|int|long) -> Persistent | None
        Return object for `oid`.

        The object may be a ghost.
        """
        if type(oid) is not str:
            oid = p64(oid)
        obj = self.cache.get(oid)
        if obj is not None:
            return obj
        try:
            pickle = self.get_stored_pickle(oid)
        except KeyError:
            return None
        obj = self.reader.get_ghost(pickle)
        obj._p_oid = oid
        obj._p_connection = self
        obj._p_set_status_ghost()
        self.cache[oid] = obj
        return obj

    __getitem__ = get

    def cache_get(self, oid):
        return self.cache.get(oid)

    def cache_set(self, oid, obj):
        self.cache[oid] = obj

    def load_state(self, obj):
        """(obj:Persistent)
        Load the state for the given ghost object.
        """
        assert self.storage is not None, 'connection is closed'
        assert obj._p_is_ghost()
        oid = obj._p_oid
        setstate = obj.__setstate__
        try:
            pickle = self.get_stored_pickle(oid)
        except DurusKeyError:
            # We have a ghost but cannot find the state for it.  This can
            # happen if the object was removed from the storage as a result
            # of packing.
            raise ReadConflictError([oid])
        state = self.reader.get_state(pickle)
        setstate(state)

    def note_change(self, obj):
        """(obj:Persistent)
        This is done when any persistent object is changed.  Changed objects
        will be stored when the transaction is committed or rolled back, i.e.
        made into ghosts, on abort.
        """
        # assert obj._p_connection is self
        self.changed[obj._p_oid] = obj

    def shrink_cache(self):
        """
        If the number of saved and unsaved objects is more than
        twice the target cache size (and the target cache size is positive),
        try to ghostify enough of the saved objects to achieve
        the target cache size.
        """
        self.cache.shrink()

    def _sync(self):
        """
        Process all invalid_oids so that all non-ghost objects are current.
        """
        invalid_oids = self.storage.sync()
        self.invalid_oids.update(invalid_oids)
        for oid in self.invalid_oids:
            obj = self.cache.get(oid)
            if obj is not None:
                obj._p_set_status_ghost()
        self.invalid_oids.clear()

    def abort(self):
        """
        Abort uncommitted changes, sync, and try to shrink the cache.
        """
        for oid, obj in self.changed.iteritems():
            obj._p_set_status_ghost()
        self.changed.clear()
        self._sync()
        self.shrink_cache()
        self.sync_count += 1

    def commit(self):
        """
        If there are any changes, try to store them, and
        raise ConflictError if there are any invalid oids saved
        or if there are any invalid oids for non-ghost objects.
        """
        if not self.changed:
            self._sync()
        else:
            if self.invalid_oids:
                # someone is trying to commit after a read or write conflict
                raise ConflictError(list(self.invalid_oids))
            self.storage.begin()
            new_objects = {}
            for oid, changed_object in self.changed.iteritems():
                writer = ObjectWriter(self)
                try:
                    for obj in writer.gen_new_objects(changed_object):
                        oid = obj._p_oid
                        if oid in new_objects:
                            continue
                        elif oid not in self.changed:
                            new_objects[oid] = obj
                            self.cache[oid] = obj
                        data, refs = writer.get_state(obj)
                        self.storage.store(oid, pack_record(oid, data, refs))
                        obj._p_set_status_saved()
                finally:
                    writer.close()
            try:
                self.storage.end(self._handle_invalidations)
            except ConflictError, exc:
                for oid, obj in new_objects.iteritems():
                    del self.cache[oid]
                    obj._p_set_status_unsaved()
                    obj._p_oid = None
                    obj._p_connection = None
                raise
            self.changed.clear()
        self.shrink_cache()
        self.sync_count += 1

    def _handle_invalidations(self, oids, read_oid=None):
        """(oids:[str], read_oid:str=None)
        Check if any of the oids are for objects that were accessed during
        this transaction.  If so, raise the appropriate conflict exception.
        """
        conflicts = []
        for oid in oids:
            obj = self.cache.get(oid)
            if obj is None:
                continue
            if not obj._p_is_ghost():
                self.invalid_oids.add(oid)
            if obj._p_touched == self.sync_count:
                conflicts.append(oid)
        if conflicts:
            if read_oid is None:
                raise ConflictError(conflicts)
            else:
                raise ReadConflictError([read_oid])

    def pack(self):
        """Clear any uncommited changes and pack the storage."""
        self.abort()
        self.storage.pack()


class _Ref(ref):

    __slots__ = ['_obj']

    def make_strong(self):
        self._obj = self()

    def make_weak(self):
        self._obj = None


class _HeapItem(object):

    __slots__ = ['object']

    def __init__(self, obj):
        self.object = obj

    def get_object(self):
        return self.object

    def __cmp__(self, other):
        return cmp(self.object._p_touched, other.object._p_touched)


class Cache(object):

    def __init__(self, size):
        self.objects = {}
        self.set_size(size)
        self.finger = 0
        # When the cache target is exceeded, shrink identifies a set of
        # non-ghost instances and converts the oldest "ghost_fraction"
        # of them into ghosts.
        # Higher values make cache size control more aggressive.
        self.ghost_fraction = 0.5
        assert 0 <= self.ghost_fraction <= 1

    def get_size(self):
        """Return the target size of the cache."""
        return self.size

    def get_count(self):
        """Return the number of objects currently in the cache."""
        return len(self.objects)

    def set_size(self, size):
        if size <= 0:
            raise ValueError, 'cache target size must be > 0'
        self.size = size

    def get(self, oid):
        weak_reference = self.objects.get(oid)
        if weak_reference is None:
            return None
        else:
            return weak_reference()

    def __setitem__(self, key, obj):
        self.objects[key] = weak_reference = _Ref(obj)
        # we want a strong reference until we decide to strink the cache
        weak_reference.make_strong()

    def __delitem__(self, key):
        del self.objects[key]

    def _get_heap(self, slice_size):
        """(slice_size:int) -> [_HeapItem]
        Examine slice_size items in self.objects.
        Make every examined reference weak.
        Remove oids of objects that have no other remaining references in memory.
        Return a heap of _HeapItems of all of the remaining 
        objects examined that are not ghosts.
        """
        removed = []
        start = self.finger % len(self.objects)
        stop = start + slice_size
        heap = []
        for oid in islice(chain(self.objects, self.objects), start, stop):
            reference = self.objects[oid]
            reference.make_weak()
            obj = reference()
            if obj is None:
                removed.append(oid)
            elif obj._p_is_saved():
                heappush(heap, _HeapItem(obj))
        # Remove dead references.
        for oid in removed:
            del self.objects[oid]
        self.finger = stop - len(removed)
        return heap

    def shrink(self):
        """
        Try to reduce the size of self.objects.
        """
        current = len(self.objects)
        if current < self.size:
            # No excess.
            log(10, '[%s] cache %s', getpid(), current)
            return
        start_time = time()

        slice_size = max(min(self.size - current, current / 4), current / 64)
        heap = self._get_heap(slice_size)

        num_ghosts = int(self.ghost_fraction * len(heap))

        for j in xrange(num_ghosts):
            obj = heappop(heap).get_object()
            obj._p_set_status_ghost()
        for item in heap:
            self.objects[item.get_object()._p_oid].make_strong()
        log(10, '[%s] shrink %fs removed %s ghosted %s'
            ' size %s', getpid(), time() - start_time,
            current - len(self.objects), num_ghosts, len(self.objects))


def touch_every_reference(connection, *words):
    """(connection:Connection, *words:(str))
    Mark as changed, every object whose pickled class/state contains any
    of the given words.  This is useful when you move or rename a class,
    so that all references can be updated.
    """
    get = connection.get
    reader = ObjectReader(connection)
    for oid, record in connection.get_storage().gen_oid_record():
        record_oid, data, refs = unpack_record(record)
        state = reader.get_state_pickle(data)
        for word in words:
            if word in data or word in state:
                get(oid)._p_note_change()

def gen_every_instance(connection, *classes):
    """(connection:Connection, *classes:(class)) -> sequence [Persistent]
    Generate all Persistent instances that are instances of any of the
    given classes."""
    for oid, record in connection.get_storage().gen_oid_record():
        record_oid, state, refs = unpack_record(record)
        record_class = loads(state)
        if issubclass(record_class, classes):
            yield connection.get(oid)
