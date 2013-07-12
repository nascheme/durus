"""
$URL$
$Id$
"""
from durus.connection import Connection
from durus.file import File
from durus.file_storage import TempFileStorage, FileStorage
from durus.error import ReadConflictError
from durus.logger import direct_output
from durus.persistent import Persistent
from durus.serialize import pack_record
from durus.utils import int8_to_str, ShortRead, write_int4_str, as_bytes
from os import unlink
from pytest import raises
from tempfile import mktemp
import os
import sys


class TestFileStorage(object):

    def setup(self):
        direct_output(sys.stdout)

    def test_file_storage(self):
        name = mktemp()
        b = FileStorage(name)
        assert b.new_oid() == int8_to_str(0)
        assert b.new_oid() == int8_to_str(1)
        assert b.new_oid() == int8_to_str(2)
        raises(KeyError, b.load, int8_to_str(0))
        record = pack_record(int8_to_str(0), as_bytes('ok'), as_bytes(''))
        b.begin()
        b.store(int8_to_str(0), record)
        b.end()
        b.sync()
        b.begin()
        b.store(int8_to_str(1), pack_record(
            int8_to_str(1), as_bytes('no'), as_bytes('')))
        b.end()
        assert len(list(b.gen_oid_record(start_oid=int8_to_str(0)))) == 1
        assert len(list(b.gen_oid_record())) == 2
        b.pack()
        b.close()
        unlink(name + '.prepack')
        raises(ValueError, b.pack) # storage closed
        unlink(name + '.pack')
        raises(ValueError, b.load, int8_to_str(0)) # storage closed
        unlink(name)

    def test_reopen(self):
        f = TempFileStorage()
        filename = f.get_filename()
        if os.name == 'nt':
            f.close() # don't try to re-open an open file on windows
            return
        g = FileStorage(filename, readonly=True)
        raises(IOError, FileStorage, filename)
        f.close()
        g.close()

    def test_open_empty(self):
        name = mktemp()
        f = open(name, 'w')
        f.close()
        s = FileStorage(name)
        s.close()
        unlink(name)

    def test_short_magic(self):
        name = mktemp()
        f = open(name, 'w')
        f.write('b')
        f.close()
        raises(AssertionError, FileStorage, name)
        unlink(name)

    def test_wrong_magic(self):
        name = mktemp()
        f = open(name, 'w')
        f.write('bogusbogus')
        f.close()
        raises(AssertionError, FileStorage, name)
        unlink(name)

    def test_bad_record_size(self):
        name = mktemp()
        f = open(name, 'wb')
        g = FileStorage(name)
        f.seek(0, 2)
        write_int4_str(f, 'ok')
        g.close()
        f.close()
        raises(ShortRead, FileStorage, name)
        unlink(name)

    def test_repair(self):
        name = mktemp()
        g = FileStorage(name)
        g.close()
        f = open(name, 'r+b')
        f.seek(0, 2)
        p = f.tell()
        f.write(as_bytes('b'))
        f.flush()
        raises(ShortRead, FileStorage, name, readonly=True)
        h = FileStorage(name, repair=True)
        f.seek(0, 2)
        assert p == f.tell()
        f.close()
        h.close()
        unlink(name)


class TestShelfStorageTest(object):

    def test_a(self):
        f = File(prefix='shelftest')
        name = f.get_name()
        f.close()
        s = FileStorage(name)
        c = Connection(s)
        r = c.get_root()
        for x in range(10):
            r["a%s" % x] = Persistent()
            c.commit()
        deleted_oids = [
            r['a0']._p_oid, r['a2']._p_oid, r['a7']._p_oid, r['a8']._p_oid]
        del r['a0']
        del r['a2']
        del r['a7']
        del r['a8']
        c.commit()
        gone = c.get(deleted_oids[0])
        c.pack()
        c.abort()
        assert c.get(deleted_oids[0])._p_is_ghost()
        assert c.get(deleted_oids[1])._p_is_ghost()
        gone = c.get(deleted_oids[0])
        # this is no longer a KeyError because of improved packing
        with raises(ReadConflictError):
            getattr(gone, 'a')
        assert len([repr(oid) for oid, record in s.gen_oid_record()]) == 7
        c.commit()
        c.pack()
        new_oid = s.new_oid()
        assert new_oid == deleted_oids[-1], (new_oid, deleted_oids)
        new_oid = s.new_oid()
        assert new_oid == deleted_oids[-2], (new_oid, deleted_oids)
        new_oid = s.new_oid()
        assert new_oid == deleted_oids[-3], (new_oid, deleted_oids)
        new_oid = s.new_oid()
        assert new_oid == deleted_oids[-4], (new_oid, deleted_oids)
        new_oid = s.new_oid()
        assert new_oid == int8_to_str(11), repr(new_oid)
        new_oid = s.new_oid()
        assert new_oid == int8_to_str(12), repr(new_oid)

    def test_b(self):
        f = File(prefix='shelftest')
        name = f.get_name()
        f.close()
        s = FileStorage(name)
        c = Connection(s)
        r = c.get_root()
        for x in range(10):
            r["a%s" % x] = Persistent()
            c.commit()
        deleted_oid = r['a9']._p_oid
        del r['a9']
        c.commit()
        c.pack()
        c.abort()
        assert len([repr(oid) for oid, record in s.gen_oid_record()]) == 10
        new_oid = s.new_oid()
        assert new_oid == deleted_oid
        new_oid = s.new_oid()
        assert new_oid == int8_to_str(11)

    def test_c(self):
        f = File(prefix='shelftest')
        name = f.get_name()
        f.close()
        s = FileStorage(name)
        c = Connection(s)
        r = c.get_root()
        for x in range(10):
            r["a%s" % x] = Persistent()
            c.commit()
        deleted_oid = r['a9']._p_oid
        del r['a9']
        c.commit()
        c.pack()
        c.abort()
        r.clear()
        c.commit()
        c.pack()
        c.abort()
        new_oid = s.new_oid()
        assert new_oid == int8_to_str(1), repr(new_oid)
        new_oid = s.new_oid()
        assert new_oid == int8_to_str(2), repr(new_oid)
