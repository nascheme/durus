"""
$URL$
$Id$
"""
from durus.error import ConflictError, WriteConflictError, ReadConflictError
from durus.error import DurusKeyError
from durus.logger import log
from durus.persistent import ConnectionBase, GHOST
from durus.persistent_dict import PersistentDict
from durus.serialize import ObjectReader, ObjectWriter
from durus.serialize import unpack_record, pack_record
from durus.utils import int8_to_str, iteritems, loads, byte_string, as_bytes
from heapq import heappush, heappop
from itertools import islice, chain
from os import getpid
from time import time
from weakref import ref, KeyedRef
import durus.storage
try:
    from durus._persistent import _setattribute
except ImportError:
    _setattribute = object.__setattr__

ROOT_OID = int8_to_str(0)

class Connection (ConnectionBase):
    """
    The Connection manages movement of objects in and out of storage.

    Instance attributes:
      storage: Storage
      cache: Cache
      reader: ObjectReader
      changed: {oid:str : PersistentObject}
      invalid_oids: set([str])
         Set of oids of objects known to have obsolete state.
      transaction_serial: int
        Number of calls to commit() or abort() since this instance was created.
        This is used to maintain consistency, and to implement LRU replacement
        in the cache.
    """

    def __init__(self, storage, cache_size=100000, root_class=None):
        """(storage:Storage|str, cache_size:int=100000, 
            root_class:class|None=None)
        Make a connection to `storage`.
        Set the target number of non-ghosted persistent objects to keep in
        the cache at `cache_size`.
        If there is no root object yet, create it as an instance
        of the root_class (or PersistentDict, if root_class is None), 
        calling the constructor with no arguments.
        Also, if the root_class is not None, verify that this really is the 
        class of the root object.  
        """
        if isinstance(storage, str):
            from durus.file_storage import FileStorage
            storage = FileStorage(storage)
        assert isinstance(storage, durus.storage.Storage)
        self.storage = storage
        self.reader = ObjectReader(self)
        self.changed = {}
        self.invalid_oids = set()
        self.new_oid = storage.new_oid # needed by serialize
        self.cache = Cache(cache_size)
        self.root = self.get(ROOT_OID)
        if self.root is None:
            new_oid = self.new_oid()
            assert ROOT_OID == new_oid
            self.root = self.get_cache().get_instance(
                ROOT_OID, root_class or PersistentDict, self)
            self.root._p_set_status_saved()
            self.root.__class__.__init__(self.root)
            self.root._p_note_change()
            self.commit()
        assert root_class in (None, self.root.__class__)

    def get_storage(self):
        """() -> Storage"""
        return self.storage

    def get_cache_count(self):
        """() -> int
        Return the number of PersistentObject instances currently in the cache.
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
        """() -> PersistentObject
        Returns the root object.
        """
        return self.root

    def get_stored_pickle(self, oid):
        """(oid:str) -> str
        Retrieve the pickle from storage.  Will raise ReadConflictError if
        the oid is invalid.
        """
        assert oid not in self.invalid_oids, "still conflicted: missing abort()"
        try:
            record = self.storage.load(oid)
        except ReadConflictError:
            invalid_oids = self.storage.sync()
            self._handle_invalidations(invalid_oids, read_oid=oid)
            record = self.storage.load(oid)
        oid2, data, refdata = unpack_record(record)
        assert as_bytes(oid) == oid2, (oid, oid2)
        return data

    def get(self, oid):
        """(oid:str|int|long) -> PersistentObject | None
        Return object for `oid`.

        The object may be a ghost.
        """
        if not isinstance(oid, byte_string):
            oid = int8_to_str(oid)
        obj = self.cache.get(oid)
        if obj is not None:
            return obj
        try:
            data = self.get_stored_pickle(oid)
        except KeyError:
            return None
        klass = loads(data)
        obj = self.cache.get_instance(oid, klass, self)
        state = self.reader.get_state(data, load=True)
        obj.__setstate__(state)
        obj._p_set_status_saved()
        return obj

    __getitem__ = get

    def get_crawler(self, start_oid=ROOT_OID, batch_size=100):
        """(start_oid:str = ROOT_OID, batch_size:int = 100) ->
            sequence(PersistentObject)
        Returns a generator for the sequence of objects in a breadth first
        traversal of the object graph, starting at the given start_oid.
        The objects in the sequence have their state loaded at the same time,
        so this can be used to initialize the object cache.
        This uses the storage's bulk_load() method to make it faster.  The
        batch_size argument sets the number of object records loaded on each
        call to bulk_load().
        """
        oid_record_sequence = self.storage.gen_oid_record(
            start_oid=start_oid, batch_size=batch_size)
        for oid, record in oid_record_sequence:
            obj = self.cache.get(oid)
            if obj is not None and not obj._p_is_ghost():
                yield obj
            else:
                record_oid, data, refdata = unpack_record(record)
                if obj is None:
                    klass = loads(data)
                    obj = self.cache.get_instance(oid, klass, self)
                state = self.reader.get_state(data, load=True)
                obj.__setstate__(state)
                obj._p_set_status_saved()
                yield obj

    def get_cache(self):
        return self.cache

    def load_state(self, obj):
        """(obj:PersistentObject)
        Load the state for the given ghost object.
        """
        assert self.storage is not None, 'connection is closed'
        assert obj._p_is_ghost()
        oid = obj._p_oid
        try:
            pickle = self.get_stored_pickle(oid)
        except DurusKeyError:
            # We have a ghost but cannot find the state for it.  This can
            # happen if the object was removed from the storage as a result
            # of packing.
            raise ReadConflictError([oid])
        state = self.reader.get_state(pickle)
        obj.__setstate__(state)
        obj._p_set_status_saved()

    def get_load_count(self):
        """() -> int
        Returns the number of times that any object's state has been loaded.
        """
        return self.reader.get_load_count()

    def note_access(self, obj):
        assert obj._p_connection is self
        assert obj._p_oid is not None
        _setattribute(obj, '_p_serial', self.transaction_serial)
        self.cache.recent_objects.add(obj)

    def note_change(self, obj):
        """(obj:PersistentObject)
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
        for oid, obj in iteritems(self.changed):
            obj._p_set_status_ghost()
        self.changed.clear()
        self._sync()
        self.shrink_cache()
        self.transaction_serial += 1

    def commit(self):
        """
        If there are any changes, try to store them, and
        raise WriteConflictError if there are any invalid oids saved
        or if there are any invalid oids for non-ghost objects.
        """
        if not self.changed:
            self._sync()
        else:
            assert not self.invalid_oids, "still conflicted: missing abort()"
            self.storage.begin()
            new_objects = {}
            for oid, changed_object in iteritems(self.changed):
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
            except ConflictError:
                for oid, obj in iteritems(new_objects):
                    obj._p_oid = None
                    del self.cache[oid]
                    obj._p_set_status_unsaved()
                    obj._p_connection = None
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
            if obj._p_serial == self.transaction_serial:
                conflicts.append(oid)
                self.invalid_oids.add(oid)
            elif not obj._p_is_ghost():
                assert oid not in self.changed
                obj._p_set_status_ghost()
        if conflicts:
            if read_oid is None:
                raise WriteConflictError(conflicts)
            else:
                raise ReadConflictError([read_oid])

    def pack(self):
        """Clear any uncommited changes and pack the storage."""
        self.abort()
        self.storage.pack()

class ObjectDictionary (object):
    """
    Like a WeakValueDictionary, except that the actual removal of keys is
    delayed until the next time an iteration is started, when it is assumed
    that other threads are not continuing any iterations.
    """
    def __init__(self):
        self.mapping = {}
        self.dead = set()
        def callback(keyed_ref, selfref=ref(self)):
            self = selfref()
            if self is not None:
                self.dead.add(keyed_ref.key)
        self.callback = callback

    def get(self, key, default=None):
        ref = self.mapping.get(key, None)
        if ref is not None:
            value = ref()
            if value is not None and key not in self.dead:
                return value
        return default

    def __setitem__(self, key, value):
        self.dead.discard(key)
        self.mapping[key] = KeyedRef(value, self.callback, key)

    def __delitem__(self, key):
        self.dead.add(key)

    def __contains__(self, key):
        return self.get(key, None) is not None

    def __len__(self):
        return len(self.mapping) - len(self.dead)

    def clear_dead(self):
        while self.dead:
            self.mapping.pop(self.dead.pop(), None)

    def __iter__(self):
        self.clear_dead()
        for key in self.mapping:
            if key not in self.dead:
                yield key


class ReferenceContainer (object):
    """
    This is used to hold hard references to recently used instances.
    """
    def __init__(self):
        self.map = {}

    def __len__(self):
        return len(self.map)

    def add(self, x):
        self.map[id(x)] = x

    def discard(self, x):
        key = id(x)
        if key in self.map:
            del self.map[key]


class Cache (object):

    def __init__(self, size):
        self.objects = ObjectDictionary()
        self.recent_objects = ReferenceContainer()
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
            raise ValueError('cache target size must be > 0')
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
        if obj is None or obj.__class__ is not klass:
            # Make a new ghost.
            obj = klass.__new__(klass)
            _setattribute(obj, '_p_oid', oid)
            _setattribute(obj, '_p_connection', connection)
            _setattribute(obj, '_p_status', GHOST) # obj._p_set_status_ghost()
            objects[oid] = obj
        return obj

    def get(self, oid):
        return self.objects.get(oid)

    def __setitem__(self, key, obj):
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
            obj = all.get(oid)
            if obj is None:
                continue # The ref is dead.
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

    def __iter__(self):
        get = self.objects.get
        for key in self.objects:
            yield get(key)


def touch_every_reference(connection, *words):
    """(connection:Connection, *words:(str))
    Mark as changed, every object whose pickled class/state contains any
    of the given words.  This is useful when you move or rename a class,
    so that all references can be updated.
    """
    get = connection.get
    reader = ObjectReader(connection)
    words = [as_bytes(w) for w in words]
    for oid, record in connection.get_storage().gen_oid_record():
        record_oid, data, refs = unpack_record(record)
        state = reader.get_state_pickle(data)
        for word in words:
            if word in data or word in state:
                get(oid)._p_note_change()

def gen_every_instance(connection, *classes):
    """(connection:Connection, *classes:(class)) -> sequence [PersistentObject]
    Generate all PersistentObject instances that are instances of any of the
    given classes."""
    for oid, record in connection.get_storage().gen_oid_record():
        record_oid, state, refs = unpack_record(record)
        record_class = loads(state)
        if issubclass(record_class, classes):
            yield connection.get(oid)
