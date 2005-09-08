#!/usr/bin/env python
"""
This script is for converting an existing durus database to the
other durus FileStorage format.
If the existing database is in FileStorage1 format, it is
converted to FileStorage2 format.
If the existing database is in FileStorage2 format, it is
converted to FileStorage1 format.
All objects are put in one new transaction with a zero tid,
so this removes obsolete object records.
"""
from durus.connection import Connection
from datetime import datetime
from durus.file_storage import FileStorage, FileStorage1, FileStorage2
from optparse import OptionParser
from os.path import exists
from os import rename

def duplicate_file_storage(old_storage, new_storage):
    """(old_storage:FileStorage, new_storage:FileStorage)
    Transfer the records from a storage to a new one.
    """
    assert len(new_storage.index) == 0, "new_storage must be empty"
    # Transfer the records
    new_storage.begin()
    for oid, record in old_storage.gen_oid_record():
        new_storage.store(oid, record)
    new_storage.end()

def repickle_storage(storage):
    """(storage: FileStorage)
    Force very object to be loaded and re-pickled.
    This also packs, so that all of the old pickles are removed.
    """
    connection = Connection(storage)
    for j, oid in enumerate(storage.index):
        obj = connection.get(oid)
        obj._p_note_change()
        if j and j % 10000 == 0:
            connection.commit()
    connection.commit()
    storage.pack()

def move_to_backup(name):
    if exists(name):
        rename(name, "%s.%s" % (name, datetime.now()))

def copy_to_new_format(from_file, to_file, format):
    tmp_file_name = "%s.%s.tmp" % (to_file, datetime.now())
    if format == 1:
        to_storage = FileStorage1(tmp_file_name)
    elif format == 2:
        to_storage = FileStorage2(tmp_file_name)
    from_storage = FileStorage(from_file, readonly=True)

    duplicate_file_storage(from_storage, to_storage)
    old_num_records = len(from_storage.index)
    assert len(to_storage.index) == old_num_records
    from_storage.close()
    repickle_storage(to_storage)
    assert len(to_storage.index) > max(1, old_num_records/2)
    move_to_backup(to_file)
    rename(tmp_file_name, to_file)

if __name__ == '__main__':
    parser = OptionParser()
    parser.set_description("Copies a file storage using a specified format.")
    parser.add_option(
        '--from', dest="from_file", default=None,
        help="The durus storage file to be copied. (required)")
    parser.add_option(
        '--to', dest="to_file", default=None,
        help="The target file. (required)")
    parser.add_option(
        '--format', default=2,
        type=int,
        help="The FileStorage format to use (1 or 2).  The default is 2.")
    (options, args) = parser.parse_args()
    if None in (options.from_file, options.to_file):
        print "\nThe --from and --to arguments are required.\n"
        print parser.format_help()
        raise SystemExit
    copy_to_new_format(options.from_file, options.to_file, options.format)

