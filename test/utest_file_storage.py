#!/www/python/bin/python
"""
$URL$
$Id$
"""
from sancho.utest import UTest
from durus.file_storage import TempFileStorage, FileStorage
from durus.serialize import pack_record
from durus.utils import p64


class Test (UTest):

    def check_file_storage(self):
        b=TempFileStorage()
        filename =  b.fp.name
        assert b._get_tid() == p64(0)
        assert b.new_oid() == p64(1)
        assert b.new_oid() == p64(2)
        try:
            b.load(p64(0))
            assert 0
        except KeyError: pass
        record = pack_record(p64(0), 'ok', '')
        b.store(record)
        b.begin()
        assert b._get_tid() == p64(1)
        b.end()
        b.sync()
        b.begin()
        b.store(pack_record(p64(1), 'no', ''))
        b.end()
        b.pack()
        import durus.file_storage
        if durus.file_storage.RENAME_OPEN_FILE:
            durus.file_storage.RENAME_OPEN_FILE = False
            b.pack()
            c = FileStorage(filename, readonly=True)
            try:
                c.pack()
                assert 0 
            except IOError: # read-only storage
                pass
        b.close()
        try:
            b.pack()
            assert 0 
        except IOError: # storage closed
            pass
        try:
            b.load(0)
            assert 0
        except IOError: # storage closed
            pass

if __name__ == "__main__":
    Test()

