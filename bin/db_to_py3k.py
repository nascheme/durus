#!/usr/bin/env python
"""
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

This converts all binary strings except _p_oid attribute values and
date/datetime objects.  If your storage has other binary string values that you
don't want converted to unicode strings, you probably need to modify this
script (likely the decode_string() function).
"""

assert bytes is not str, "This script is for Python 3+ only."

import sys
from datetime import datetime, date

def decode_string(self, value):
    # This function will likely need patching depending on the application
    # database being converted.  If there are strings that should end up
    # beinng 'bytes' in the new database, they will be have to be preserved
    # here.
    if self.stack:
        last = self.stack[-1]
    else:
        last = None
    # check for data that should be retained as byte strings
    if last in [date, datetime]:
        return value
    if len(value) == 8 and value[:4] == b'\0\0\0\0':
        # assume oid, keep as bytes
        return value
    try:
        return value.decode(self.encoding, self.errors)
    except UnicodeDecodeError:
        print('decode failed %r %s' % (value[:8], self.stack))
        return value

def patch_pickler():
    import pickle
    import durus.utils
    # We will modify the the Unpickler class.
    durus.utils.Unpickler = pickle._Unpickler
    durus.utils.loads = pickle._loads

    # convert byte strings as necessary
    pickle._Unpickler._decode_string = decode_string

    # Make a fake __builtin__ in case any of those are pickled.
    import builtins
    sys.modules['__builtin__'] = builtins


def main():
    from shutil import copyfile
    from os.path import exists

    def usage():
        sys.stdout.write(
            "Usage: python %s <existing_file> <new_file>\n" % sys.argv[0])
        sys.stdout.write("  Creates a new py3k-compatible file ")
        sys.stdout.write("from an existing FileStorage file.\n")
        raise SystemExit

    if len(sys.argv) != 3:
        usage()
    infile = sys.argv[1]
    outfile = sys.argv[2]
    if not exists(infile):
        usage()
    if exists(outfile):
        if input('overwrite %r? [y/N] ' % outfile).strip().lower() != 'y':
            raise SystemExit
    copyfile(infile, outfile)

    # monkey patch pickler class, must be done before importing durus stuff
    patch_pickler()

    from durus.__main__ import get_storage_class
    from durus.connection import Connection

    storage_class = get_storage_class(outfile)
    storage = storage_class(outfile)
    connection = Connection(storage)
    print ("Converting %s for use with py3k." % outfile)
    for j, x in enumerate(connection.get_crawler()):
        x._p_note_change()
        if j > 0 and j % 10000 == 0:
            print(j)
            connection.commit()
    print(j)
    connection.commit()
    connection.pack()


if __name__ == '__main__':
    main()
