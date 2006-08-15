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
from durus.serialize import split_oids, unpack_record, pack_record
from durus.storage import Storage
from durus.utils import p64
from itertools import islice, chain
from os import getpid
from time import time
from weakref import WeakValueDictionary, ref

ROOT_OID = p64(0)

class Connection(ConnectionBase):
    """
    The Connection manages movement of objects in and out of storage.

    Instance attributes:
      storage: Storage
      cache: Cache
      reader: ObjectReader
      changed: {oid:str : Persistent}
      invalid_oids: set([str])
         Set of oids of objects known to have obsolete state.
      transaction_serial: int
        Number of calls to commit() or abort() since this instance was created.
        This is used to maintain consistency, and to implement LRU replacement
        in the cache.
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
        self.invalid_oids = set()
        try:
            storage.load(ROOT_OID)
        except KeyError:
            self.storage.begin()
            writer = ObjectWriter(self)
            data, refs = writer.get_state(PersistentDict())
            writer.close()
            self.storage.store(ROOT_OID, pack_record(ROOT_OID, data, refs))
            self.storage.end(self._handle_invalidations)
            self.transaction_serial += 1
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

    def get_transaction_serial(self):
        """() -> int
        Return the number of calls to commit() or abort() on this instance.
        """
        return self.transaction_serial

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
        klass = loads(pickle)
        obj = self.cache.get_instance(oid, klass, self)
        return obj

    __getitem__ = get

    def get_crawler(self, start_oid=ROOT_OID, batch_size=100):
        """(start_oid:str = ROOT_OID, batch_size:int = 100) ->
            sequence(Persistent)
        Returns a generator for the sequence of objects in a breadth first
        traversal of the object graph, starting at the given start_oid.
        The objects in the sequence have their state loaded at the same time,
        so this can be used to initialize the object cache.
        This uses the storage's bulk_load() method to make it faster.  The
        batch_size argument sets the number of object records loaded on each
        call to bulk_load().
        """
        def get_object_and_refs(object_record):
            oid, data, refdata = unpack_record(object_record)
            obj = self.cache.get(oid)
            if obj is None:
                klass = loads(data)
                obj = self.cache.get_instance(oid, klass, self)
                state = self.reader.get_state(data, load=True)
                obj.__setstate__(state)
                obj._p_set_status_saved()
            elif obj._p_is_ghost():
                state = self.reader.get_state(data, load=True)
                obj.__setstate__(state)
                obj._p_set_status_saved()
            return obj, split_oids(refdata)
        queue = [start_oid]
        seen = set()
        while queue:
            batch = queue[:batch_size]
            queue = queue[batch_size:]
            seen.update(batch)
            for record in self.storage.bulk_load(batch):
                obj, refs = get_object_and_refs(record)
                for ref in refs:
                    if ref not in seen:
                        queue.append(ref)
                yield obj

    def get_cache(self):
        return self.cache

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

    def note_access(self, obj):
        assert obj._p_connection is self
        assert obj._p_oid is not None
        obj._p_serial = self.transaction_serial
        self.cache.recent_objects.add(obj)

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
        self.cache.shrink(self)

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
        self.transaction_serial += 1

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
                    obj._p_oid = None
                    del self.cache[oid]
                    obj._p_set_status_unsaved()
                    obj._p_connection = None
                    obj._p_ref = None
                raise
            self.changed.clear()
        self.shrink_cache()
        self.transaction_serial += 1

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
            if obj._p_serial == self.transaction_serial:
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


class Cache(object):

    def __init__(self, size):
        self.objects = WeakValueDictionary()
        self.recent_objects = set()
        self.set_size(size)
        self.finger = 0

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

    def get_instance(self, oid, klass, connection):
        """
        This returns the existing object with the given oid, or else it makes
        a new one with the given class and connection.

        This method is called when unpickling a reference, which may happen at
        a high frequency, so it needs to be fast.  For the sake of speed, it
        inlines some statements that would normally be executed through calling
        other functions.
        """
        # if self.get(oid) is not None: return self.get(oid)
        objects = self.objects
        obj = objects.get(oid)
        if obj is None:
            # Make a new ghost.
            obj = klass.__new__(klass)
            obj._p_oid = oid
            obj._p_connection = connection
            obj._p_status = -1 # obj._p_set_status_ghost()
            objects[oid] = obj
        return obj

    def get(self, oid):
        return self.objects.get(oid)

    def __setitem__(self, key, obj):
        assert key not in self.objects or self.objects[key] is obj
        self.objects[key] = obj

    def __delitem__(self, key):
        obj = self.objects.get(key)
        if obj is not None:
            self.recent_objects.discard(obj)
            assert obj._p_oid is None
            del self.objects[key]

    def _build_heap(self, transaction_serial):
        """(transaction_serial:int) -> [(serial, oid)]
        """
        all = self.objects
        heap_size_target = (len(all) - self.size) * 2
        start = self.finger % len(all)
        heap = []
        for oid in islice(chain(all, all), start, start + len(all)):
            self.finger += 1
            obj = all[oid]
            if obj._p_serial == transaction_serial:
                continue # obj is current.  Leave it alone.
            heappush(heap, (obj._p_serial, oid))
            if len(heap) >= heap_size_target:
                break
        self.finger = self.finger % len(all)
        return heap

    def shrink(self, connection):
        """(connection:Connection)
        Try to reduce the size of self.objects.
        """
        current = len(self.objects)
        if current <= self.size:
            # No excess.
            log(10, '[%s] cache size %s recent %s',
                getpid(), current, len(self.recent_objects))
            return
        start_time = time()
        heap = self._build_heap(connection.get_transaction_serial())
        num_ghosted = 0
        while heap and len(self.objects) > self.size:
            serial, oid = heappop(heap)
            obj = self.objects.get(oid)
            if obj is None:
                continue
            if obj._p_is_saved():
                obj._p_set_status_ghost()
                num_ghosted += 1
            self.recent_objects.discard(obj)
        log(10, '[%s] shrink %fs removed %s ghosted %s size %s recent %s',
            getpid(), time() - start_time, current - len(self.objects),
            num_ghosted, len(self.objects), len(self.recent_objects))


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
