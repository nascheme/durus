"""
$URL$
$Id$
"""
from durus import run_durus
from durus.client_storage import ClientStorage
from durus.error import ReadConflictError
from durus.serialize import pack_record
from durus.utils import p64
from popen2 import Popen4
from sancho.utest import UTest, raises
from sets import Set
from time import sleep

class Test (UTest):

    def _pre(self):
        self.port = 9123
        self.server = Popen4('python %s --port=%s' % (
            run_durus.__file__, self.port))
        sleep(3) # wait for bind

    def _post(self):
        run_durus.stop_durus("", self.port)

    def check_client_storage(self):
        b = ClientStorage(port=self.port)
        c = ClientStorage(port=self.port)
        assert b.new_oid() == p64(1)
        assert b.new_oid() == p64(2)
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
        b.pack()
        assert len(list(b.gen_oid_record())) == 1
        raises(ReadConflictError, c.load, p64(0))
        raises(ReadConflictError, c.load, p64(0))
        assert Set(c.sync()) == Set([p64(0), p64(1)])
        assert record == c.load(p64(0))

if __name__ == "__main__":
    Test()

