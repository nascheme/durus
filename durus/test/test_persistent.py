"""
$URL$
$Id$
"""
from durus.connection import Connection
from durus.file_storage import TempFileStorage
from durus.logger import direct_output
from durus.persistent import Persistent, PersistentObject
from durus.utils import int8_to_str, dumps, loads
from sancho.utest import UTest, raises
import sys


class TestPersistent (UTest):

    def _pre(self):
        direct_output(sys.stdout)

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

    def check_accessors(self):
        p=Persistent()
        p._p_oid
        assert p._p_format_oid() == 'None'
        p._p_oid = 'aaaaaaaa'
        assert p._p_format_oid() == '7016996765293437281'
        p._p_oid = int8_to_str(1)
        assert p._p_format_oid() == '1'
        assert repr(p) == "<Persistent 1>"

    def check_more(self):
        storage = TempFileStorage()
        connection = Connection(storage)
        root=connection.get_root()
        assert not root._p_is_ghost()
        root['a'] = 1
        assert root._p_is_unsaved()
        del root['a']
        connection.abort()
        assert root._p_is_ghost()
        raises(AttributeError, getattr, root, 'a')
        root._p_set_status_saved()
        assert root._p_is_saved()
        root._p_set_status_unsaved()
        assert root._p_is_unsaved()
        root._p_set_status_ghost()
        assert root._p_is_ghost()
        root._p_set_status_unsaved()

    def pickling(self):
        a = Persistent()
        pickle_a = dumps(a, 2)
        b = loads(pickle_a)
        assert isinstance(b, Persistent)

    def lowlevelops(self):
        from durus.persistent import _getattribute, _setattribute
        from durus.persistent import _delattribute, _hasattribute
        storage = TempFileStorage()
        connection = Connection(storage)
        root = connection.get_root()
        root._p_set_status_ghost()
        assert not _hasattribute(root, 'data')
        root._p_set_status_ghost()
        raises(AttributeError, _getattribute, root, 'data')
        assert root._p_is_ghost()
        _setattribute(root, 'data', 'bogus')
        assert root._p_is_ghost()
        _delattribute(root, 'data')
        assert root._p_is_ghost()

class TestPersistentObject (UTest):

    def check_getstate(self):
        p = PersistentObject()
        assert p.__getstate__() == {}
        raises(AttributeError, setattr, p, 'a', 1)

    def check_setstate(self):
        p = PersistentObject()
        p.__setstate__({})
        raises(AttributeError, p.__setstate__, {'a':1})

    def check_change(self):
        p = PersistentObject()
        p._p_note_change()

    def check_accessors(self):
        p = PersistentObject()
        p._p_oid
        assert p._p_format_oid() == 'None'
        p._p_oid = int8_to_str(1)
        assert p._p_format_oid() == '1'
        assert repr(p) == "<PersistentObject 1>"

    def pickling(self):
        a = PersistentObject()
        pickle_a = dumps(a, 2)
        b = loads(pickle_a)
        assert isinstance(b, PersistentObject)

class SlottedPersistentObject (PersistentObject):

    __slots__ = ['a', 'b']

class SlottedPersistentObjectWithDict (PersistentObject):

    __slots__ = ['a', 'b', '__dict__']

main = sys.modules['__main__']
main.SlottedPersistentObject = SlottedPersistentObject
main.SlottedPersistentObjectWithDict = SlottedPersistentObjectWithDict

class TestSlottedPersistentObject (UTest):

    def a(self):
        p = SlottedPersistentObject()
        assert p.__getstate__() == {}
        p.a = 1
        assert p.__getstate__() == dict(a=1)
        raises(AttributeError, setattr, p, 'c', 2)

    def pickling(self):
        a = SlottedPersistentObject()
        pickle_a = dumps(a, 2)
        b = loads(pickle_a)
        assert isinstance(b, SlottedPersistentObject)

class TestSlottedPersistentObjectWithDict (UTest):

    def a(self):
        p = SlottedPersistentObjectWithDict()
        assert p.__getstate__() == {}
        p.a = 1
        assert p.__getstate__() == dict(a=1)
        p.c = 2
        assert p.__getstate__() == dict(a=1, c=2)
        assert p.__dict__ == dict(c=2)

    def pickling(self):
        a = SlottedPersistentObjectWithDict()
        pickle_a = dumps(a, 2)
        b = loads(pickle_a)
        assert isinstance(b, SlottedPersistentObjectWithDict)


if __name__ == "__main__":
    TestPersistent()
    TestPersistentObject()
    TestSlottedPersistentObject()
    TestSlottedPersistentObjectWithDict()

