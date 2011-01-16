"""
$URL: svn+ssh://svn.mems-exchange.org/repos/trunk/durus/test/utest_client_storage.py $
$Id: utest_client_storage.py 31518 2009-03-11 20:03:47Z dbinger $
"""
from durus import __main__
from durus.client_storage import ClientStorage
from durus.connection import Connection
from durus.error import ReadConflictError, DurusKeyError, WriteConflictError
from durus.error import ProtocolError
from durus.persistent import Persistent
from durus.persistent_dict import PersistentDict
from durus.serialize import pack_record
from durus.storage_server import STATUS_INVALID, wait_for_server
from durus.utils import int8_to_str, BytesIO, as_bytes, join_bytes
from os import unlink, devnull
from os.path import exists
from sancho.utest import UTest, raises
from subprocess import Popen
from tempfile import mktemp
from time import sleep
import sys


class FakeSocket (object):

    def __init__(self, *args):
        self.io = BytesIO(join_bytes(as_bytes(a) for a in args))

    def recv(self, n):
        sys.stdout.write('recv %s\n' % n)
        return self.io.read(n)

    def sendall(self, s):
        sys.stdout.write('sendall %r\n' % s)

    def write(self, s):
        sys.stdout.write('write %r\n' % s)

class ClientTest (UTest):

    address = ("localhost", 9123)

    def _pre(self):
        self.filename = mktemp()
        cmd = [sys.executable, __main__.__file__, 
            '-s', '--file=%s' % self.filename]
        if isinstance(self.address, tuple):
            cmd.append("--port=%s" % self.address[1])
        else:
            cmd.append("--address=%s" % self.address)
        cmd.append("--logginglevel=1")
        output = open(devnull, 'w')
        #output = sys.__stdout__
        Popen(cmd, stdout=output, stderr=output)
        wait_for_server(address=self.address, sleeptime=1, maxtries=10)

    def _post(self):
        __main__.stop_durus(self.address)
        if exists(self.filename):
            unlink(self.filename)
        prepack = self.filename + '.prepack'
        if exists(prepack):
            unlink(prepack)

    def check_client_storage(self):
        b = ClientStorage(address=self.address)
        c = ClientStorage(address=self.address)
        oid = b.new_oid()
        assert oid == int8_to_str(0), repr(oid)
        oid = b.new_oid()
        assert oid == int8_to_str(1), repr(oid)
        oid = b.new_oid()
        assert oid == int8_to_str(2), repr(oid)
        raises(KeyError, b.load, int8_to_str(0))
        record = pack_record(int8_to_str(0), as_bytes('ok'), as_bytes(''))
        b.begin()
        b.store(int8_to_str(0), record)
        assert b.end() is None
        b.load(int8_to_str(0))
        assert b.sync() == []
        b.begin()
        b.store(
            int8_to_str(1),
            pack_record(int8_to_str(1), as_bytes('no'), as_bytes('')))
        b.end()
        assert len(list(b.gen_oid_record())) == 1
        records = b.bulk_load([int8_to_str(0), int8_to_str(1)])
        assert len(list(records)) == 2
        records = b.bulk_load([int8_to_str(0), int8_to_str(1), int8_to_str(2)])
        raises(DurusKeyError, list, records)
        b.pack()
        assert len(list(b.gen_oid_record())) == 1
        raises(ReadConflictError, c.load, int8_to_str(0))
        raises(ReadConflictError, c.load, int8_to_str(0))
        assert set(c.sync()) == set([int8_to_str(0), int8_to_str(1)])
        assert record == c.load(int8_to_str(0))
        b.close()
        c.close()

    def check_oid_reuse(self):
        # Requires ShelfStorage oid reuse pack semantics
        s1 = ClientStorage(address=self.address)
        s1.oid_pool_size = 1
        c1 = Connection(s1)
        r1 = c1.get_root()
        s2 = ClientStorage(address=self.address)
        s2.oid_pool_size = 1
        c2 = Connection(s2)
        r2 = c2.get_root()
        r1['a'] = PersistentDict()
        r1['b'] = PersistentDict()
        c1.commit()
        c2.abort()
        a_oid = r1['a']._p_oid
        assert 'a' in r1 and 'b' in r1 and len(r1['b']) == 0
        assert 'a' in r2 and 'b' in r2 and len(r2['b']) == 0
        del r2['a'] # remove only reference to a
        c2.commit()
        c2.pack() # force relinquished oid back into availability
        sleep(0.5) # Give time for pack to complete
        c2.abort()
        assert c2.get(a_oid) is None
        c1.abort()
        assert c1.get(a_oid)._p_is_ghost()
        r2['b']['new'] = Persistent()
        r2['b']['new'].bogus = 1
        c2.commit()
        assert c2.get(a_oid) is r2['b']['new']
        c1.abort()
        assert c1.get(a_oid).__class__ == PersistentDict
        r1['b']['new'].bogus
        assert c1.get(a_oid).__class__ == Persistent
        s1.close()

    def check_oid_reuse_with_invalidation(self):
        connection = Connection(ClientStorage(address=self.address))
        root = connection.get_root()
        root['x'] = Persistent()
        connection.commit()
        connection = Connection(ClientStorage(address=self.address))
        root = connection.get_root()
        root['x'] = Persistent()
        connection.commit()
        connection.pack()
        sleep(1) # Make sure pack finishes.
        connection = Connection(ClientStorage(address=self.address))
        root = connection.get_root()
        root['x'] = Persistent()
        connection.commit()

    def check_write_conflict(self):
        s1 = ClientStorage(address=self.address)
        c1 = Connection(s1)
        r1 = c1.get_root()
        s1.s = FakeSocket('\0\0\0\0', STATUS_INVALID)
        r1._p_note_change()
        raises(WriteConflictError, c1.commit)

    def end_protocol_error(self):
        s1 = ClientStorage(address=self.address)
        c1 = Connection(s1)
        r1 = c1.get_root()
        s1.s = FakeSocket('\0\0\0\0?')
        r1._p_note_change()
        raises(ProtocolError, c1.commit)

    def pack_protocol_error(self):
        s1 = ClientStorage(address=self.address)
        s1.s = FakeSocket('?')
        raises(ProtocolError, s1.pack)

    def load_protocol_error(self):
        s1 = ClientStorage(address=self.address)
        c1 = Connection(s1)
        s1.s = FakeSocket('?')
        raises(ProtocolError, s1.load, int8_to_str(0))

    def close(self):
        s1 = ClientStorage(address=self.address)
        s1.close()

class UnixDomainSocketTest (ClientTest):

    address = "/tmp/test.durus_server"

if __name__ == "__main__":
    ClientTest()
    try:
        from socket import AF_UNIX
        UnixDomainSocketTest()
    except ImportError:
        AF_UNIX = None # quiet the checker
