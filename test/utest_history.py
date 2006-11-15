"""
$URL$
$Id$
"""
from durus.connection import Connection
from durus.history import HistoryConnection
from durus.file_storage import TempFileStorage
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
        hc = HistoryConnection(connection.get_storage().get_filename())
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
        raises(KeyError, getattr, a, 'b')

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
        hc = HistoryConnection(connection.get_storage().get_filename())
        a = hc.get_root()['a']
        assert len(hc.get_storage().index.history) == 6
        hc.previous_instance(a)
        assert a.b == 2
        hc.previous_instance(a)
        assert a.b == 1
        result = hc.previous_instance(a)
        assert result is None
        assert hc.get_root().keys() == []


if __name__ == "__main__":
    HistoryTest()
