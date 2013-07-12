"""
$URL$
$Id$
"""
import os, os.path
from os.path import exists
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
if os.name == 'nt':
    import win32con, win32file, pywintypes # http://sf.net/projects/pywin32/
else:
    import fcntl

class File (object):
    """
    A file wrapper that smooths over some platform-specific
    operations.
    """
    def __init__(self, name=None, readonly=False, **kwargs):
        if name is None:
            self.file = NamedTemporaryFile(**kwargs)
        else:
            if exists(name):
                if readonly:
                    self.file = open(name, 'rb')
                else:
                    self.file = open(name, 'r+b')
            else:
                if readonly:
                    raise OSError('No "%s" found.' % name)
                self.file = open(name, 'w+b')
        if readonly:
            assert self.is_readonly()
        self.has_lock = False

    def get_name(self):
        return self.file.name

    def is_temporary(self):
        return isinstance(self.file, _TemporaryFileWrapper)

    def is_readonly(self):
        return self.file.mode == 'rb'

    def seek(self, n, whence=0):
        self.file.seek(n, whence)
        if whence == 0:
            assert self.file.tell() == n

    def seek_end(self):
        self.file.seek(0, 2)

    def read(self, n=None):
        if n is None:
            return self.file.read()
        else:
            return self.file.read(n)

    def tell(self):
        return self.file.tell()

    def stat(self):
        return os.stat(self.get_name())

    def __len__(self):
        return self.stat().st_size

    def rename(self, name):
        old_name = self.get_name()
        if name == old_name:
            return
        assert not self.is_temporary()
        self.obtain_lock()
        self.close()
        if exists(name):
            os.unlink(name)
        os.rename(old_name, name)
        self.file = open(name, 'r+b')
        self.obtain_lock()

    def obtain_lock(self):
        """
        Make sure that we have an exclusive lock on self.file before
        doing a write.
        If the lock is not available, raise an exception.
        """
        assert not self.is_readonly()
        if not self.has_lock:
            if os.name == 'nt':
                try:
                    win32file.LockFileEx(
                        win32file._get_osfhandle(self.file.fileno()),
                        (win32con.LOCKFILE_EXCLUSIVE_LOCK |
                         win32con.LOCKFILE_FAIL_IMMEDIATELY),
                        0, -65536, pywintypes.OVERLAPPED())
                except pywintypes.error:
                    raise IOError("Unable to obtain lock")
            else:
                fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.has_lock = True

    def release_lock(self):
        """
        Make sure that we do not retain an exclusive lock on self.file.
        """
        if self.has_lock:
            if os.name == 'nt':
                win32file.UnlockFileEx(
                    win32file._get_osfhandle(self.file.fileno()),
                    0, -65536, pywintypes.OVERLAPPED())
            else:
                fcntl.flock(self.file, fcntl.LOCK_UN)
            self.has_lock = False

    def write(self, s):
        self.obtain_lock()
        self.file.write(s)
        # This flush helps the file knows where it ends.
        self.file.flush()

    def truncate(self):
        self.obtain_lock()
        self.file.truncate()

    def close(self):
        self.release_lock()
        self.file.close()

    def flush(self):
        self.file.flush()

    def fsync(self):
        if hasattr(os, 'fsync'):
            os.fsync(self.file)
