"""
$URL: svn+ssh://svn.mems-exchange.org/repos/trunk/durus/serialize.py $
$Id: serialize.py 31144 2008-09-17 13:46:42Z dbinger $
"""
from durus.persistent import call_if_persistent
from durus.utils import int4_to_str, str_to_int4, join_bytes, BytesIO
from durus.utils import Pickler, Unpickler, loads, dumps, as_bytes
from types import MethodType
from zlib import compress, decompress, error as zlib_error
import struct
import sys

WRITE_COMPRESSED_STATE_PICKLES = True
PICKLE_PROTOCOL = 2

def pack_record(oid, data, refs):
    """(oid:str, data:str, refs:str) -> record:str
    """
    return join_bytes([oid, int4_to_str(len(data)), data, refs])

def unpack_record(record):
    """(record:str) -> oid:str, data:str, refs:str
    The inverse of pack_record().
    """
    oid = record[:8]
    data_length = str_to_int4(record[8:12])
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

NEWLINE = as_bytes('\n')

def extract_class_name(record):
    try:
        oid, state, refs = unpack_record(record)
        return state.split(NEWLINE, 2)[1]
    except IndexError:
        return "?"

if sys.version < "3":
    def method(a, b):
        return MethodType(a, b, object)
else:
    def method(a, b):
        return MethodType(a, b)



class ObjectWriter (object):
    """
    Serializes objects for storage in the database.

    The client is responsible for calling the close() method to avoid
    leaking memory.  The ObjectWriter uses a Pickler internally, and
    Pickler objects do not participate in garbage collection.
    """

    def __init__(self, connection):
        self.sio = BytesIO()
        self.pickler = Pickler(self.sio, PICKLE_PROTOCOL)
        self.pickler.persistent_id = method(
            call_if_persistent, self._persistent_id)
        self.objects_found = []
        self.refs = set() # populated by _persistent_id()
        self.connection = connection

    def close(self):
        # see ObjectWriter.__doc__
        # Explicitly break cycle involving pickler
        self.pickler.persistent_id = int
        self.pickler = None

    def _persistent_id(self, obj):
        """(PersistentBase) -> (oid:str, klass:type)
        This is called on PersistentBase instances during pickling.
        """
        if obj._p_oid is None:
            obj._p_oid = self.connection.new_oid()
            obj._p_connection = self.connection
            self.objects_found.append(obj)
        elif obj._p_connection is not self.connection:
            raise ValueError(
                "Reference to %r has a different connection." % obj)
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
        self.sio.seek(0) # recycle BytesIO instance
        self.sio.truncate()
        self.pickler.clear_memo()
        self.pickler.dump(type(obj))
        self.refs.clear()
        position = self.sio.tell()
        self.pickler.dump(obj.__getstate__())
        uncompressed = self.sio.getvalue()
        pickled_type = uncompressed[:position]
        pickled_state = uncompressed[position:]
        if WRITE_COMPRESSED_STATE_PICKLES:
            state = compress(pickled_state)
        else:
            state = pickled_state
        data = pickled_type + state
        self.refs.discard(obj._p_oid)
        return data, join_bytes(self.refs)



COMPRESSED_START_BYTE = compress(dumps({}, 2))[0]

class ObjectReader (object):

    def __init__(self, connection):
        self.connection = connection
        self.load_count = 0

    def _get_unpickler(self, file):
        connection = self.connection
        get_instance = connection.get_cache().get_instance
        def persistent_load(oid_klass):
            oid, klass = oid_klass
            return get_instance(oid, klass, connection)
        unpickler = Unpickler(file)
        unpickler.persistent_load = persistent_load
        return unpickler

    def get_ghost(self, data):
        klass = loads(data)
        instance = klass.__new__(klass)
        instance._p_set_status_ghost()
        return instance

    def get_state(self, data, load=True):
        self.load_count += 1
        s = BytesIO()
        s.write(data)
        s.seek(0)
        unpickler = self._get_unpickler(s)
        klass = unpickler.load()
        position = s.tell()
        if data[s.tell()] == COMPRESSED_START_BYTE:
            # This is almost certainly a compressed pickle.
            try:
                decompressed = decompress(data[position:])
            except zlib_error:
                pass # let the unpickler try anyway.
            else:
                s.write(decompressed)
                s.seek(position)
        if load:
            return unpickler.load()
        else:
            return s.read()

    def get_state_pickle(self, data):
        return self.get_state(data, load=False)

    def get_load_count(self):
        return self.load_count
