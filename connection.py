"""$URL$
$Id$
"""

from os import getpid
from sets import Set
from time import time
from weakref import ref
from itertools import islice, chain
from durus.error import ConflictError, ReadConflictError
from durus.logger import log
from durus.persistent_dict import PersistentDict
from durus.serialize import ObjectReader, ObjectWriter, split_oids, \
     unpack_record, pack_record
from durus.storage import Storage
from durus.utils import p64

ROOT_OID = p64(0)

class Connection(object):
    """
    The Connection manages movement of objects in and out of storage.

    Instance attributes:
      storage: Storage
      cache: Cache
      reader: ObjectReader
      changed: {oid:str : Persistent}
      invalid_oids: Set([str])
         Set of oids of objects known to have obsolete state. 
      loaded_oids : Set([str])
         Set of oids of objects that were in the SAVED state at some time
         during the current transaction.
      tid: str
         The last tid reported by the storage.
    """

    def __init__(self, storage, cache_size=8000):
        """(storage:Storage, cache_size:int=8000)
        Make a connection to `storage`.
        Set the target number of non-ghosted persistent objects to keep in
        the cache at `cache_size`.
        """
        assert isinstance(storage, Storage)
        self.storage = storage
        self.cache = Cache(cache_size)
        self.reader = ObjectReader(self)
        self.changed = {}
        self.invalid_oids = Set()
        self.loaded_oids = Set()
        self.tid, invalid_oids = self.storage.sync()
        try:
            storage.load(ROOT_OID)
        except KeyError:
            def new_oid(self):
                return ROOT_OID
            self.new_oid = new_oid
            self.storage.begin()
            writer = ObjectWriter(self)
            data, refs = writer.get_state(PersistentDict())
            writer.close()
            self.storage.store(pack_record(ROOT_OID, data, refs))
            self.tid = self.storage.end(self._handle_invalidations)
        self.new_oid = storage.new_oid # needed by serialize

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

    def get_root(self):
        """() -> Persistent
        Returns the root object.
        """
        return self.get(ROOT_OID)

    def get_stored_pickle(self, oid):
        """(oid:str) -> str
        Retrieve the pickle from storage.  Will raise ReadConflictError if
        pickle's tid is newer than self.tid and there are invalid objects
        in the cache.
        """
        if oid in self.invalid_oids:
            # someone is still trying to read after getting a conflict
            raise ReadConflictError([oid])
        record = self.storage.load(oid)
        tid = record[:8]
        record = record[8:]
        if tid > self.tid:
            # might need to generate a read conflict.  If none of the
            # invalid OIDs were loaded by this connection then we are okay.
            self.tid, invalid_oids = self.storage.sync()
            self._handle_invalidations(invalid_oids, read_oid=oid)
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
        pickle = self.get_stored_pickle(oid)
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

    def note_saved(self, obj):
        self.loaded_oids.add(obj._p_oid)

    def shrink_cache(self):
        """
        If the number of saved and unsaved objects is more than
        twice the target cache size (and the target cache size is positive),
        try to ghostify enough of the saved objects to achieve
        the target cache size.
        """
        self.cache.shrink(self.loaded_oids)

    def _sync(self):
        """
        Update self.tid and process all invalid_oids so that all non-ghost
        objects are current up to this tid.
        """
        self.tid, invalid_oids = self.storage.sync()
        self.invalid_oids.update(split_oids(invalid_oids))
        for oid in self.invalid_oids:
            obj = self.cache.get(oid)
            if obj is not None:
                obj._p_set_status_ghost()
                self.loaded_oids.discard(oid)
        self.invalid_oids.clear()

    def abort(self):
        """
        Abort uncommitted changes, sync, and try to shrink the cache.
        """
        for oid, obj in self.changed.iteritems():
            obj._p_set_status_ghost()
            self.loaded_oids.discard(oid)
        self.changed.clear()
        self._sync()
        self.shrink_cache()

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
                        self.storage.store(pack_record(oid, data, refs))
                        obj._p_set_status_saved()
                finally:
                    writer.close()
            try:
                self.tid = self.storage.end(self._handle_invalidations)
            except ConflictError, exc:
                for oid, obj in new_objects.iteritems():
                    del self.cache[oid]
                    self.loaded_oids.discard(oid)
                    obj._p_set_status_unsaved()
                    obj._p_oid = None
                    obj._p_connection = None
                raise
            self.changed.clear()
        self.shrink_cache()

    def _handle_invalidations(self, packed_oids, read_oid=None):
        """(packed_oids:str, read_oid:str=None)
        Check if any of the oids are for objects that were loaded during
        this transaction.  If so, raise the appropriate conflict exception.
        """
        oids = Set(split_oids(packed_oids))
        invalid_oids = self.loaded_oids.intersection(oids)
        if invalid_oids:
            self.invalid_oids.update(invalid_oids)
            if read_oid is None:
                raise ConflictError(list(invalid_oids))
            else:
                raise ReadConflictError([read_oid])

    def pack(self):
        """Clear any uncommited changes and pack the storage."""
        self.abort()
        self.storage.pack()


class Cache(object):

    def __init__(self, size):
        self.objects = {}
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

    def get(self, oid):
        weak_reference = self.objects.get(oid)
        if weak_reference is None:
            return None
        else:
            return weak_reference()

    def __setitem__(self, key, obj):
        self.objects[key] = ref(obj)

    def __delitem__(self, key):
        del self.objects[key]

    def shrink(self, loaded_oids):
        if 0:
            # debugging code, ensure loaded_oids is sane
            for oid, r in self.objects.iteritems():
                obj = r()
                if obj is not None and obj._p_is_saved():
                    # every SAVED object must be in loaded_oids
                    assert oid in loaded_oids, obj._p_format_oid()
            for oid in loaded_oids:
                # every oid in loaded_oids must have an entry in the cache
                assert oid in self.objects

        size = len(self.objects)
        assert len(loaded_oids) <= size
        extra = size - self.size
        if extra < 0:
            log(10, '[%s] cache size %s loaded %s', getpid(), size,
                len(loaded_oids))
            return
        start_time = time()
        aged = 0
        removed = Set()
        ghosts = Set()
        start = self.finger % size
        # Look at no more than 1/4th and no less than 1/64th of objects
        stop = start + max(min(size >> 2, extra), size >> 6)
        for oid in islice(chain(self.objects, self.objects), start, stop):
            weak_reference = self.objects[oid]
            obj = weak_reference()
            if obj is None:
                removed.add(oid)
            elif obj._p_touched:
                obj._p_touched = 0
                aged += 1
            elif obj._p_is_saved():
                obj._p_set_status_ghost()
                ghosts.add(oid)
        for oid in removed:
            del self.objects[oid]
        loaded_oids -= removed
        loaded_oids -= ghosts
        self.finger = stop - len(removed)
        log(10, '[%s] shrink %fs aged %s removed %s ghosted %s'
            ' loaded %s size %s', getpid(), time() - start_time,
            aged, len(removed), len(ghosts), len(loaded_oids),
            len(self.objects))
