#!/www/python/bin/python
"""
$URL$
$Id$
"""
from time import sleep
from popen2 import Popen4
from sancho.utest import UTest
from durus import run_durus
from durus.client_storage import ClientStorage
from durus.serialize import pack_record
from durus.utils import p64


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
        assert b.new_oid() == p64(1)
        assert b.new_oid() == p64(2)
        try:
            b.load(p64(0))
            assert 0
        except KeyError: pass
        record = pack_record(p64(0), 'ok', '')
        b.begin()
        b.store(record)
        b.end()
        b.load(p64(0))
        b.sync()
        b.begin()
        b.store(pack_record(p64(1), 'no', ''))
        b.end()
        b.pack()

if __name__ == "__main__":
    Test()

