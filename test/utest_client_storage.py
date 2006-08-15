"""
$URL$
$Id$
"""
from durus import run_durus
from durus.client_storage import ClientStorage
from durus.error import ReadConflictError, DurusKeyError
from durus.serialize import pack_record
from durus.utils import p64
from popen2 import Popen4
from sancho.utest import UTest, raises
from time import sleep

class Test (UTest):

    address = ("", 9123)

    def _pre(self):
        if type(self.address) is tuple:
            self.server = Popen4('python %s --port=%s' % (
                run_durus.__file__, self.address[1]))
        else:
            self.server = Popen4('python %s --address=%s' % (
                run_durus.__file__, self.address))
        sleep(3) # wait for bind

    def _post(self):
        run_durus.stop_durus(self.address)

    def check_client_storage(self):
        b = ClientStorage(address=self.address)
        c = ClientStorage(address=self.address)
        print self.address
        oid = b.new_oid()
        assert oid == p64(1), repr(oid)
        oid = b.new_oid()
        assert oid == p64(2), repr(oid)
        try:
            b.load(p64(0))
            assert 0
        except KeyError: pass
        record = pack_record(p64(0), 'ok', '')
        b.begin()
        b.store(p64(0), record)
        assert b.end() is None
        b.load(p64(0))
        assert b.sync() == []
        b.begin()
        b.store(p64(1), pack_record(p64(1), 'no', ''))
        b.end()
        assert len(list(b.gen_oid_record())) == 2
        records = b.bulk_load([p64(0), p64(1)])
        assert len(list(records)) == 2
        records = b.bulk_load([p64(0), p64(1), p64(2)])
        raises(DurusKeyError, list, records)
        b.pack()
        assert len(list(b.gen_oid_record())) == 1
        raises(ReadConflictError, c.load, p64(0))
        raises(ReadConflictError, c.load, p64(0))
        assert set(c.sync()) == set([p64(0), p64(1)])
        assert record == c.load(p64(0))


class UnixDomainSocketTest(Test):

    address = "test.durus_server"

if __name__ == "__main__":
    Test()
    UnixDomainSocketTest()
