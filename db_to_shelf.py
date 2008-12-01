#!/usr/bin/env python
"""
Creates a new Shelf-format file from an existing FileStorage file.
"""
from durus.file_storage import FileStorage
from durus.shelf import Shelf
from os.path import exists
import sys

def usage():
    print("Usage: python %s <existing_file> <new_file>\n" % sys.argv[0])
    print(__doc__)
    raise SystemExit

def main(old_file, new_file):
    if not exists(old_file) or exists(new_file):
        usage()
    storage = FileStorage(old_file)
    shelf = Shelf(new_file, items=storage.gen_oid_record())
    storage.close()
    shelf.close()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
    main(*sys.argv[1:])