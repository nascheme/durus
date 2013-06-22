"""
$URL$
$Id$
"""
from durus.file import File
from durus.utils import as_bytes
from os import unlink
from os.path import exists
from sancho.utest import UTest, raises
from tempfile import mktemp

class FileTest (UTest):

    def a(self):
        f = File()
        f.rename(f.get_name())
        assert f.is_temporary()
        raises(AssertionError, f.rename, f.get_name() + '.renamed')
        test_name = f.get_name() + '.test'
        assert not exists(test_name)
        tmp = open(test_name, 'w+b')
        tmp.close()
        g = File(test_name)
        assert not g.is_temporary()
        g.rename(g.get_name() + '.renamed')
        assert g.get_name() == test_name + '.renamed'
        f.write(as_bytes('abc'))
        f.seek(0)
        assert len(f) == 3
        assert as_bytes('a') == f.read(1)
        assert as_bytes('bc') == f.read()
        f.close()
        assert not exists(f.get_name())
        raises(OSError, f.__len__) # tmpfile removed on close
        h = File(g.get_name())
        g.write(as_bytes('a'))
        g.seek(0)
        assert g.tell() == 0
        g.seek_end()
        assert g.tell() == 1
        assert g.has_lock
        assert not h.has_lock
        raises(IOError, h.write, as_bytes('b'))
        g.flush()
        g.fsync()
        g.seek(0)
        g.truncate()
        g.close()
        h.close()
        unlink(g.get_name())

    def b(self):
        name = mktemp()
        raises(OSError, File, name, readonly=True)
        g = File(name, readonly=False)
        g.close()
        f = File(name, readonly=True)
        assert f.is_readonly()
        raises(AssertionError, f.write, 'ok')
        raises(IOError, f.file.write, 'ok') # readonly file
        f.close()
        unlink(name)

    def c(self):
        name = mktemp()
        name2 = mktemp()
        f = File(name)
        assert f.tell() == 0
        g = File(name2)
        g.close()
        f.rename(name2)
        assert exists(name2)
        assert not exists(name)
        f.close()
        unlink(name2)

if __name__ == '__main__':
    FileTest()

