"""
$URL$
$Id$
"""
from durus.file_storage import FileStorage1, FileStorage2
from durus.file_storage import TempFileStorage, FileStorage
from durus.serialize import pack_record
from durus.utils import p64
from os import unlink
from sancho.utest import UTest
from tempfile import mktemp


class Test (UTest):

    def check_file_storage(self):
        self._check_file_storage(TempFileStorage())
        self._check_file_storage(FileStorage1())

    def _check_file_storage(self, storage):
        b = storage
        assert b.new_oid() == p64(1)
        assert b.new_oid() == p64(2)
        try:
            b.load(p64(0))
            assert 0
        except KeyError: pass
        record = pack_record(p64(0), 'ok', '')
        b.store(p64(0), record)
        b.begin()
        b.end()
        b.sync()
        b.begin()
        b.store(p64(1), pack_record(p64(1), 'no', ''))
        b.end()
        assert len(list(b.gen_oid_record())) == 2
        b.pack()
        import durus.file_storage
        if durus.file_storage.RENAME_OPEN_FILE:
            durus.file_storage.RENAME_OPEN_FILE = False
            b.pack()
            c = FileStorage(b.get_filename(), readonly=True)
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

    def check_reopen(self):
        f = TempFileStorage()
        filename = f.fp.name
        g = FileStorage(filename, readonly=True)
        h = FileStorage2(filename, readonly=True)

    def check_open_empty(self):
        name = mktemp()
        f = open(name, 'w')
        f.close()
        s = FileStorage(name)
        s.close()
        unlink(name)



if __name__ == "__main__":
    Test()

