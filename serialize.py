"""$URL$
$Id$
"""

import struct
from sets import Set
from cPickle import Pickler, Unpickler
from cStringIO import StringIO
from durus.error import InvalidObjectReference
from durus.persistent import Persistent
from durus.utils import p32, u32

def pack_record(oid, data, refs):
    """(oid:str, data:str, refs:str) -> record:str
    """
    return ''.join([oid, p32(len(data)), data, refs])

def unpack_record(record):
    """(record:str) -> oid:str, data:str, refs:str
    The inverse of pack_record().
    """
    oid = record[:8]
    data_length = u32(record[8:12])
    data_end = 12 + data_length
    data = record[12:data_end]
    refs = record[data_end:]
    return oid, data, refs

def split_oids(s):
    """(s:str) -> [str]
    s is a packed string of oids.  Return a list of oid strings.
    """
    if not s:
        return []
    num, extra = divmod(len(s), 8)
    assert extra == 0, s
    fmt = '8s' * num
    return list(struct.unpack('>' + fmt, s))

def extract_class_name(record):
    class_name = record[20:80].split('\n', 2)[1] # assumes pickle protocol 2    
    return class_name

class ObjectWriter(object):
    """
    Serializes objects for storage in the database.

    The client is responsible for calling the close() method to avoid
    leaking memory.  The ObjectWriter uses a Pickler internally, and
    Pickler objects do not participate in garbage collection.
    """

    def __init__(self, connection):
        self.sio = StringIO()
        self.pickler = Pickler(self.sio, 2)
        self.pickler.persistent_id = self._persistent_id
        self.objects_found = []
        self.refs = Set() # populated by _persistent_id()
        self.connection = connection

    def close(self):
        # see ObjectWriter.__doc__
        # Explicitly break cycle involving pickler
        self.pickler.persistent_id = None
        self.pickler = None

    def _persistent_id(self, obj):
        """
        This function is used by the pickler to test whether an object
        is persistent. If the obj is persistent, it returns the oid and type,
        otherwise it returns None.
        """
        if not isinstance(obj, Persistent):
            return None
        if obj._p_oid is None:
            obj._p_oid = self.connection.new_oid()
            obj._p_connection = self.connection
            self.objects_found.append(obj)
        elif obj._p_connection is not self.connection:
            raise InvalidObjectReference(obj, self.connection)
        self.refs.add(obj._p_oid)
        return obj._p_oid, type(obj)

    def gen_new_objects(self, obj):
        def once(obj):
            raise RuntimeError('gen_new_objects() already called.')
        self.gen_new_objects = once
        yield obj # The modified object is also a "new" object.
        for obj in self.objects_found:
            yield obj

    def get_state(self, obj):
        self.sio.seek(0) # recycle StringIO instance
        self.sio.truncate()
        self.pickler.clear_memo()
        self.pickler.dump(type(obj))
        self.refs.clear()
        self.pickler.dump(obj.__getstate__())
        data = self.sio.getvalue()
        self.refs.discard(obj._p_oid)
        return data, ''.join(self.refs)

class ObjectReader(object):

    def __init__(self, connection):
        self.connection = connection

    def _get_unpickler(self, pickle):
        def persistent_load(oid_klass):
            oid, klass = oid_klass
            obj = self.connection.cache_get(oid)
            if obj is None:
                # Go ahead and make the ghost instance.
                obj = klass.__new__(klass)
                obj._p_oid = oid
                obj._p_connection = self.connection
                obj._p_set_status_ghost()
                self.connection.cache_set(oid, obj)
            return obj
        unpickler = Unpickler(StringIO(pickle))
        unpickler.persistent_load = persistent_load
        return unpickler

    def get_ghost(self, data):
        unpickler = self._get_unpickler(data)
        klass = unpickler.load()
        instance = klass.__new__(klass)
        instance._p_set_status_ghost()
        return instance

    def get_state(self, data):
        unpickler = self._get_unpickler(data)
        klass = unpickler.load()
        state = unpickler.load()
        return state
