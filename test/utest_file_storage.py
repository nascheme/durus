"""
$URL$
$Id$
"""
from durus.file_storage import TempFileStorage, FileStorage
from durus.serialize import pack_record
from durus.utils import p64
from os import unlink
from sancho.utest import UTest, raises
from tempfile import mktemp
import durus.file_storage


class Test (UTest):

    def check_file_storage(self):
        self._check_file_storage(TempFileStorage())

    def _check_file_storage(self, storage):
        b = storage
        assert b.new_oid() == p64(0)
        assert b.new_oid() == p64(1)
        assert b.new_oid() == p64(2)
        raises(KeyError, b.load, p64(0))
        record = pack_record(p64(0), 'ok', '')
        b.store(p64(0), record)
        b.begin()
        b.end()
        b.sync()
        b.begin()
        b.store(p64(1), pack_record(p64(1), 'no', ''))
        b.end()
        assert len(list(b.gen_oid_record(start_oid=p64(0)))) == 1
        assert len(list(b.gen_oid_record())) == 2
        b.pack()
        if durus.file_storage.RENAME_OPEN_FILE:
            durus.file_storage.RENAME_OPEN_FILE = False
            b.pack()
            c = FileStorage(b.get_filename(), readonly=True)
            raises(IOError, c.pack) # read-only storage
        b.close()
        raises(IOError, b.pack) # storage closed
        raises(IOError, b.load, p64(0)) # storage closed

    def check_reopen(self):
        f = TempFileStorage()
        filename = f.get_filename()
        g = FileStorage(filename, readonly=True)

    def check_open_empty(self):
        name = mktemp()
        f = open(name, 'w')
        f.close()
        s = FileStorage(name)
        s.close()
        unlink(name)


if __name__ == "__main__":
    Test()
