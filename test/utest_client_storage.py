"""
$URL$
$Id$
"""
from StringIO import StringIO
from durus import run_durus
from durus.client_storage import ClientStorage
from durus.connection import Connection
from durus.error import ReadConflictError, DurusKeyError, WriteConflictError
from durus.error import ProtocolError
from durus.serialize import pack_record
from durus.storage_server import STATUS_INVALID, wait_for_server
from durus.utils import int8_to_str
from os import unlink
from os.path import exists
from popen2 import Popen4
from sancho.utest import UTest, raises
from tempfile import mktemp


class FakeSocket (StringIO):
    def recv(self, n):
        print 'recv', n
        return self.read(n)
    def sendall(self, s):
        print 'sendall', repr(s)
    def write(self, s):
        print 'write', repr(s)

class ClientTest (UTest):

    address = ("", 9123)

    def _pre(self):
        self.filename = mktemp()
        if type(self.address) is tuple:
            self.server = Popen4('python %s --port=%s --file=%s' % (
                run_durus.__file__, self.address[1], self.filename))
        else:
            self.server = Popen4('python %s --address=%s --file=%s' % (
                run_durus.__file__, self.address, self.filename))
        wait_for_server(address=self.address, sleeptime=1, maxtries=10)

    def _post(self):
        run_durus.stop_durus(self.address)
        if exists(self.filename):
            unlink(self.filename)
        prepack = self.filename + '.prepack'
        if exists(prepack):
            unlink(prepack)

    def check_client_storage(self):
        b = ClientStorage(address=self.address)
        c = ClientStorage(address=self.address)
        print self.address
        oid = b.new_oid()
        assert oid == int8_to_str(0), repr(oid)
        oid = b.new_oid()
        assert oid == int8_to_str(1), repr(oid)
        oid = b.new_oid()
        assert oid == int8_to_str(2), repr(oid)
        raises(KeyError, b.load, int8_to_str(0))
        record = pack_record(int8_to_str(0), 'ok', '')
        b.begin()
        b.store(int8_to_str(0), record)
        assert b.end() is None
        b.load(int8_to_str(0))
        assert b.sync() == []
        b.begin()
        b.store(int8_to_str(1), pack_record(int8_to_str(1), 'no', ''))
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

    def check_write_conflict(self):
        s1 = ClientStorage(address=self.address)
        c1 = Connection(s1)
        r1 = c1.get_root()
        s1.s = FakeSocket('\0\0\0\0' + STATUS_INVALID)
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
    UnixDomainSocketTest()
