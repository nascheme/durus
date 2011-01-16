"""
$URL: svn+ssh://svn.mems-exchange.org/repos/trunk/durus/db_to_py3k.py $
$Id: db_to_py3k.py 31111 2008-09-16 14:02:06Z dbinger $

Due to differences in the string types, a durus FileStorage file produced
using Python 2 won't work in Python 3 without changes.  Attribute names,
for example, would be unpickled as binary strings, but Python 3 requires
that they be unicode strings.  This script attempts to produce a new storage
file, with data converted to a form that is compatible with Python 3.

This modifies the unpickler so all that values that would be binary strings
are actually unpickled as unicode strings, and everything is repickled for
placement in the new storage file.  Note that this conversion is not 
reversable, so please be especially careful to keep a backup of your storage 
file.

This converts all binary strings except _p_oid attribute values.  If your
storage has other binary string values that you don't want converted to
unicode strings, you probably need to modify this script.
"""
import sys
assert sys.version >= "3", "This script is for py3k only."

# # We modify the the Unpickler class.
import pickle
from pickle import _Unpickler, REDUCE
#

# Force Unpickler instances to use latin1 for binstrings.
_Unpickler__init__ = _Unpickler.__init__
def Unpickler_init(self, *args, **kwargs):
    kwargs['encoding'] = 'latin1'
    return _Unpickler__init__(self, *args, **kwargs)
_Unpickler.__init__ = Unpickler_init
pickle.Unpickler = _Unpickler

# The datetime class won't work with the converted unicode string.
# Here, we modify the Unpickler's load_reduce() method to work around this problem.
from datetime import datetime

def load_reduce_fixed_for_datetime(self):
    stack = self.stack
    args = stack.pop()
    func = stack[-1]
    if func == datetime:
        # Convert the unicode string back into the byte string, since
        # datetime requires it.
        args = (args[0].encode('latin1'),) + args[1:]
    try:
        stack[-1] = func(*args)
    except:
        print(func, args)
        raise

_Unpickler.load_reduce = load_reduce_fixed_for_datetime
_Unpickler.dispatch[REDUCE[0]] = _Unpickler.load_reduce

# Make a fake __builtin__ in case any of those are pickled.
import builtins
sys.modules['__builtin__'] = builtins

# Okay, now we need one more modification to Durus, since oids
# really do need to be byte strings.

from durus.connection import Connection, Cache
from durus.utils import byte_string

def oid_as_bytes(f):
    def g(self, oid, klass, connection):
        # Make sure that the oid is not a unicode string.
        if not isinstance(oid, byte_string):
            oid = oid.encode('latin1')
            assert len(oid) == 8, (oid, klass)
        return f(self, oid, klass, connection)
    return g

Cache.get_instance = oid_as_bytes(Cache.get_instance)

if __name__ == '__main__':
    from durus.connection import Connection
    from durus.file_storage import FileStorage
    from shutil import copyfile

    def usage():
        sys.stdout.write(
            "Usage: python %s <existing_file> <new_file>\n" % sys.argv[0])
        sys.stdout.write("  Creates a new py3k-compatible file ")
        sys.stdout.write("from an existing FileStorage file.\n")
        raise SystemExit

    if len(sys.argv) != 3:
        usage()
    from os.path import exists
    if not exists(sys.argv[1]):
        usage()
    if exists(sys.argv[2]):
        usage()
    copyfile(sys.argv[1], sys.argv[2])
    storage = FileStorage(sys.argv[2])
    connection = Connection(storage)
    print ("Converting %s for use with py3k." % sys.argv[2])
    for j, x in enumerate(connection.get_crawler()):
        x._p_note_change()
        if j > 0 and j % 10000 == 0:
            print(j)
            connection.commit()
    print(j)
    connection.commit()
    connection.pack()
