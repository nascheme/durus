"""
$URL$
$Id$
"""
from sancho.utest import UTest, raises
from durus.storage import MemoryStorage
from durus.serialize import pack_record
from durus.utils import p64

class Test (UTest):

    def check_memory_storage(self):
        b = MemoryStorage()
        assert b.new_oid() == p64(1)
        assert b.new_oid() == p64(2)
        raises(KeyError, b.load, p64(0))
        record = pack_record(p64(0), 'ok', '')
        b.begin()
        b.store(p64(0), record)
        b.end()
        b.sync()
        b.begin()
        b.store(p64(1), pack_record(p64(1), 'no', ''))
        b.end()
        assert len(list(b.gen_oid_record())) == 2
        assert record == b.load(p64(0))

if __name__ == "__main__":
    Test()

