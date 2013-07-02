#!/usr/bin/env python
"""
This script creates a new storage with the oids of all instances,
(except the root) reassigned so that they are in a minimal range,
which makes FileStorage (when using the Shelf format) more compact
and efficient for some operations.
"""
from durus.connection import Connection
from os.path import exists
from tempfile import TemporaryFile
import sys

if sys.version < "3":
    from cPickle import dump, load
else:
    from pickle import dump, load

def usage():
    print("%s <old-file-storage> <new-file-storage>" % sys.argv[0])
    print(__doc__)
    raise SystemExit

def main(old_file, new_file):
    if old_file.startswith('-'):
        usage()
    if new_file.startswith('-'):
        usage()
    assert not exists(new_file)
    connection = Connection(sys.argv[1])
    tmpfile = TemporaryFile()
    print("pickling from " + old_file)
    dump(connection.get_root().__getstate__(), tmpfile, 2)
    connection = None
    tmpfile.seek(0)
    connection2 = Connection(sys.argv[2])
    print("unpickling")
    connection2.get_root().__setstate__(load(tmpfile))
    connection2.get_root()._p_note_change()
    print("commit to " + new_file)
    connection2.commit()
    print("pack")
    connection2.pack()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
    main(*sys.argv[1:])