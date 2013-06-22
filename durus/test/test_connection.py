"""
$URL$
$Id$
"""
from durus import __main__
from durus.client_storage import ClientStorage
from durus.connection import Connection, touch_every_reference
from durus.connection import ObjectDictionary
from durus.error import ConflictError, WriteConflictError
from durus.persistent import Persistent, PersistentBase
from durus.persistent import ConnectionBase
from durus.storage import get_reference_index, get_census, MemoryStorage
from durus.storage import gen_referring_oid_record, Storage
from durus.storage_server import wait_for_server
from durus.utils import int8_to_str, as_bytes, next
from os import unlink, devnull
from os.path import exists
from sancho.utest import UTest, raises
from subprocess import Popen
from tempfile import mktemp
import sys

class TestConnection (UTest):

    def _get_storage(self):
        return MemoryStorage()

    def check_connection(self):
        self.conn=conn=Connection(self._get_storage())
        self.root=root=conn.get_root()
        assert root._p_is_ghost() == False
        assert root is conn.get(int8_to_str(0))
        assert root is conn.get(0)
        assert conn is root._p_connection
        assert conn.get(int8_to_str(1)) == None
        conn.abort()
        conn.commit()
        assert root._p_is_ghost() == False
        root['a'] = Persistent()
        assert root._p_is_unsaved() == True
        assert root['a']._p_is_unsaved() == True
        root['a'].f=2
        assert list(conn.changed.values()) == [root]
        conn.commit()
        assert root._p_is_saved()
        assert list(conn.changed.values()) == []
        root['a'] = Persistent()
        assert list(conn.changed.values()) == [root]
        root['b'] = Persistent()
        root['a'].a = 'a'
        root['b'].b = 'b'
        conn.commit()
        root['a'].a = 'a'
        root['b'].b = 'b'
        conn.abort()
        conn.shrink_cache()
        root['b'].b = 'b'
        del conn

    def check_shrink(self):
        storage = self._get_storage()
        self.conn=conn=Connection(storage, cache_size=3)
        self.root=root=conn.get_root()
        root['a'] = Persistent()
        root['b'] = Persistent()
        root['c'] = Persistent()
        assert self.root._p_is_unsaved()
        conn.commit()
        root['a'].a = 1
        conn.commit()
        root['b'].b = 1
        root['c'].c = 1
        root['d'] = Persistent()
        root['e'] = Persistent()
        root['f'] = Persistent()
        conn.commit()
        root['f'].f = 1
        root['g'] = Persistent()
        conn.commit()
        conn.pack()

    def check_storage_tools(self):
        connection = Connection(self._get_storage())
        root = connection.get_root()
        root['a'] = Persistent()
        root['b'] = Persistent()
        connection.commit()
        index = get_reference_index(connection.get_storage())
        assert index == {
            int8_to_str(1): [int8_to_str(0)], int8_to_str(2): [int8_to_str(0)]}
        census = get_census(connection.get_storage())
        assert census == {as_bytes('PersistentDict'):1, as_bytes('Persistent'):2}
        references = list(gen_referring_oid_record(connection.get_storage(),
                                                   int8_to_str(1)))
        assert references == [
            (int8_to_str(0), connection.get_storage().load(int8_to_str(0)))]
        class Fake(object):
            pass
        s = Fake()
        s.__class__ = Storage
        raises(RuntimeError, s.__init__)
        raises(NotImplementedError, s.load, None)
        raises(NotImplementedError, s.begin)
        raises(NotImplementedError, s.store, None, None)
        raises(NotImplementedError, s.end)
        raises(NotImplementedError, s.sync)
        g = s.gen_oid_record()
        raises(NotImplementedError, next, g)

    def check_touch_every_reference(self):
        connection = Connection(self._get_storage())
        root = connection.get_root()
        root['a'] = Persistent()
        root['b'] = Persistent()
        from durus.persistent_list import PersistentList
        root['b'].c = PersistentList()
        connection.commit()
        touch_every_reference(connection, 'PersistentList')
        assert root['b']._p_is_unsaved()
        assert root['b'].c._p_is_unsaved()
        assert not root._p_is_unsaved()
        assert len(list(connection.get_cache())) == 4

    def check_alternative_root(self):
        connection = Connection(self._get_storage(), root_class=Persistent)
        root = connection.get_root()
        assert isinstance(root, Persistent)
        connection2 = Connection(connection.storage, root_class=None)

class TestConnectionClientStorage (TestConnection):

    address = ("localhost", 9123)

    def _get_storage(self):
        return ClientStorage(port=self.port)

    def _pre(self):
        self.port = 9123
        self.filename = mktemp()
        cmd = [sys.executable, __main__.__file__,
            '-s', '--file=%s' % self.filename]
        cmd.append("--port=%s" % self.address[1])
        output = open(devnull, 'w')
        x = Popen(cmd, stdout=output, stderr=output)
        wait_for_server(address=self.address, sleeptime=1)

    def _post(self):
        __main__.stop_durus(("localhost", self.port))
        if exists(self.filename):
            unlink(self.filename)
        pack_name = self.filename + '.pack'
        if exists(pack_name):
            unlink(pack_name)

    def check_conflict(self):
        b = Connection(self._get_storage())
        c = Connection(self._get_storage())
        rootb = b.get(int8_to_str(0))
        rootb['b'] = Persistent()
        rootc = c.get(int8_to_str(0))
        rootc['c'] = Persistent()
        c.commit()
        raises(ConflictError, b.commit)
        raises(KeyError, rootb.__getitem__, 'c')
        transaction_serial = b.transaction_serial
        b.abort()
        assert b.get_transaction_serial() > transaction_serial
        assert rootb._p_is_ghost()
        rootc['d'] = Persistent()
        c.commit()
        rootb['d']

    def check_fine_conflict(self):
        c1 = Connection(self._get_storage())
        c2 = Connection(self._get_storage())
        c1.get_root()['A'] = Persistent()
        c1.get_root()['A'].a = 1
        c1.get_root()['B'] = Persistent()
        c1.commit()
        c2.abort()
        # c1 has A loaded.
        assert not c1.get_root()['A']._p_is_ghost()
        c1.get_root()['B'].b = 1
        c2.get_root()['A'].a = 2
        c2.commit()
        # Even though A has been changed by c2,
        # c1 has not accessed an attribute of A since
        # the last c1.commit(), so we don't want a ConflictError.
        c1.commit()
        assert c1.get_root()['A']._p_is_ghost()
        c1.get_root()['A'].a # accessed!
        c1.get_root()['B'].b = 1
        c2.get_root()['A'].a = 2
        c2.commit()
        raises(WriteConflictError, c1.commit)

    def _scenario(self):
        c1 = Connection(self._get_storage())
        c2 = Connection(self._get_storage())
        c1.get_root()['A'] = Persistent()
        c1.get_root()['B'] = Persistent()
        c1.get_root()['A'].a = 1
        c1.commit()
        c2.abort()
        c1.cache.recent_objects.discard(c1.get_root()['A'])
        # Imagine c1 has been running for a while, and
        # cache management, for example, has caused the
        # cache reference to be weak.
        return c1, c2

    def conflict_from_invalid_removable_previously_accessed(self):
        c1, c2 = self._scenario()
        A = c1.get_root()['A']
        A.a # access A in c1.  This will lead to conflict.
        A_oid = A._p_oid
        A = None # forget about it
        # Lose the reference to A.
        c1.get_root()._p_set_status_ghost()
        # Commit a new A in c2.
        c2.get_root()['A'].a = 2
        c2.commit()
        c1.get_root()['B'].b = 1 # touch B in c1
        # Conflict because A has been accessed in c1, but it is invalid.
        assert raises(ConflictError, c1.commit)

    def no_conflict_from_invalid_removable_not_previously_accessed(self):
        c1, c2 = self._scenario()
        c1.get_root()._p_set_status_ghost()
        # Commit a new A in c2.
        c2.get_root()['A'].a = 2
        c2.commit()
        c1.get_root()['B'].b = 1 # touch B in c1
        # A was not accessed before the reference was lost,
        # so there is no conflict.
        c1.commit()

    def check_persistentbase_refs(self):
        refs = getattr(sys, 'gettotalrefcount', None)
        if refs is None:
            return
        before = 0
        after = 0
        before = refs()
        PersistentBase()
        after = refs()
        assert after - before == 0, after - before

    def check_connectionbase_refs(self):
        refs = getattr(sys, 'gettotalrefcount', None)
        if refs is None:
            return
        before = 0
        after = 0
        before = refs()
        ConnectionBase()
        after = refs()
        assert after - before == 0, after - before

class TestObjectDictionary (UTest):

    def a(self):
        d = ObjectDictionary()
        assert len(d) == 0
        key = 'ok'
        x = Persistent()
        x.key = key
        d[key] = x
        assert d.get(key) is not None
        assert len(d) == 1
        assert list(d) == [key]
        assert key in d
        x = 1
        assert len(d) == 0
        assert d.get(key) is None
        assert list(d) == []

    def b(self):
        d = ObjectDictionary()
        assert len(d) == 0
        key = 'ok'
        x = Persistent()
        x.key = key
        d[key] = x
        assert d.get(key) is not None
        assert len(d) == 1
        assert list(d) == [key]
        del d[key]
        assert len(d) == 0
        assert d.get(key) is None
        assert list(d) == []

    def call_callback(self):
        d = ObjectDictionary()
        assert len(d) == 0
        key = 'ok'
        x = Persistent()
        x.key = key
        d[key] = x
        d.callback(x)
        assert key in d.dead
        assert key in d.mapping

if __name__ == "__main__":
    TestConnection()
    TestConnectionClientStorage()
    TestObjectDictionary()
