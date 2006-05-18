"""
$URL$
$Id$
"""
from durus.connection import Connection
from durus.file_storage import TempFileStorage
from durus.history import HistoryConnection
from durus.persistent import Persistent
from sancho.utest import UTest, raises


class HistoryTest (UTest):

    def a(self):
        connection = Connection(TempFileStorage())
        root = connection.get_root()
        root['a'] = Persistent()
        root['a'].b = 1
        connection.commit()
        root['a'].b = 2
        connection.commit()
        hc = HistoryConnection(connection.get_storage().fp.name)
        a = hc.get_root()['a']
        assert len(hc.get_storage().index.history) == 4
        assert a.b == 2
        hc.previous()
        assert a.b == 1
        hc.next()
        assert a.b == 2
        hc.previous()
        assert a.b == 1
        hc.previous()
        assert a._p_is_ghost()
        assert not hasattr(a, '__dict__')
        assert isinstance(a, Persistent)
        raises(KeyError, getattr, a, 'b')
        assert hc.get(a._p_oid) is a
        hc.next()
        assert a.b == 1

    def b(self):
        connection = Connection(TempFileStorage())
        root = connection.get_root()
        root['a'] = Persistent()
        root['a'].b = 1
        connection.commit()
        root['b'] = Persistent()
        connection.commit()
        root['b'].a = root['a']
        root['a'].b = 2
        connection.commit()
        root['a'].b = 3
        connection.commit()
        hc = HistoryConnection(connection.get_storage().fp.name)
        a = hc.get_root()['a']
        assert len(hc.get_storage().index.history) == 6
        hc.previous_instance(a)
        assert a.b == 2
        hc.previous_instance(a)
        assert a.b == 1
        hc.previous_instance(a)
        assert not hasattr(a, '__dict__')
        assert hc.get_root().keys() == []





if __name__ == "__main__":
    HistoryTest()
