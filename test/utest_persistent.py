"""
$URL$
$Id$
"""
from cPickle import dumps, loads
from durus.connection import Connection
from durus.file_storage import TempFileStorage
from durus.persistent import Persistent
from durus.utils import p64
from sancho.utest import UTest


class Test (UTest):

    def check_getstate(self):
        p=Persistent()
        assert p.__getstate__() == {}
        p.a = 1
        assert p.__getstate__() == {'a':1}

    def check_setstate(self):
        p=Persistent()
        p.__setstate__({})
        p.__setstate__({'a':1})
        assert p.a == 1

    def check_change(self):
        p=Persistent()
        p._p_changed == 0
        p._p_note_change()
        assert p._p_changed == True

    def check_accessors(self):
        p=Persistent()
        p._p_oid
        assert p._p_format_oid() == 'None'
        p._p_oid = p64(1)
        assert p._p_format_oid() == '1'
        assert repr(p) == "<Persistent 1>"

    def check_more(self):
        storage = TempFileStorage()
        connection = Connection(storage)
        root=connection.get_root()
        assert root._p_is_ghost()
        root.a = 1
        assert root._p_is_unsaved()
        del root.a
        connection.abort()
        assert root._p_is_ghost()
        try:
            root.a
            assert 0
        except AttributeError: pass
        root._p_set_status_saved()
        assert root._p_is_saved()
        root._p_set_status_unsaved()
        assert root._p_is_unsaved()
        root._p_set_status_ghost()
        assert root._p_is_ghost()
        root._p_set_status_unsaved()

    def pickling(self):
        a = Persistent()
        pickle_a = dumps(a, 2) # Pickle protocol 2 is required.
        b = loads(pickle_a)
        assert isinstance(b, Persistent)

if __name__ == "__main__":
    Test()
