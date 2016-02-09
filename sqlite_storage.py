"""
An sqlite-based storage module.  Uses a sqlite as the on-disc storage of
persistent data.

SqliteStorage compares favourably with ShelfStorage/FileStorage2 for
performance, based on limited tests. The main downside is that it does not
provide point-in-timem recovery, easy backups and asynchronous replication.
"""

import os
import sqlite3
from datetime import datetime
import collections
from durus.logger import log, is_logging
from durus.serialize import pack_record, unpack_record, split_oids
from durus.storage import Storage
from durus.utils import int8_to_str, str_to_int8, iteritems, as_bytes
import durus.connection


_DB_SCHEMA = '''\
BEGIN TRANSACTION;
CREATE TABLE objects (
    id integer primary key,
    data blob,
    refs blob
    );
COMMIT;
'''

# it is possible that WAL mode is better but for now we leave it as default
_PRAGMAS = '''\
PRAGMA journal_mode=WAL;
'''


class SqliteStorage(Storage):
    """
    Provides a Sqlite storage backend for Durus.

    Instance attributes:
      _conn: Sqlite connection
      pending_records : [ record:str ]
        Object records are accumulated here during a commit.
      pack_extra : [oid:str] | None
        oids of objects that have been committed after the pack began.  It is
        None if a pack is not in progress.
      invalid : set([oid:str])
        set of oids removed by packs since the last call to sync().
    """

    _PACK_INCREMENT = 100 # number of records to pack before yielding

    def __init__(self, filename, readonly=False, repair=False):
        if readonly:
            raise NotImplementedError
        self.filename = filename
        if not os.path.exists(filename):
            self._init()
        else:
            self._conn = sqlite3.connect(filename)
            self._last_oid = self._get_last_oid()
        self._conn.text_factory = bytes
        #self._conn.executescript(_PRAGMAS)
        self.pending_records = []
        self.pack_extra = None
        self.invalid = set()

    def _commit(self):
        self._conn.commit()

    def _init(self):
        self._conn = sqlite3.connect(self.filename)
        c = self._conn.cursor()
        c.executescript(_DB_SCHEMA)
        self._commit()
        self._last_oid = 0

    def get_filename(self):
        """() -> str
        Returns the full path name of the file that contains the data.
        """
        return self.filename

    def _get_last_oid(self):
        """() -> int
        Return the highest OID in the database as integer.
        """
        c = self._conn.cursor()
        c.execute('SELECT max(id) FROM objects')
        v = c.fetchone()
        if v is None:
            return 0
        return v[0]

    def load(self, oid):
        """(str) -> str
        Return object record identified by 'oid'.
        """
        c = self._conn.cursor()
        c.execute('SELECT id, data, refs FROM objects WHERE id = ?',
                (str_to_int8(oid),))
        v = c.fetchone()
        if v is None:
            raise KeyError(oid)
        return pack_record(int8_to_str(v[0]), v[1], v[2])

    def begin(self):
        del self.pending_records[:]

    def store(self, oid, record):
        """(str, str)"""
        self.pending_records.append(record)

    def _store_records(self, records):
        def gen_items(records):
            for record in records:
                oid, data, refdata = unpack_record(record)
                yield str_to_int8(oid), as_bytes(data), as_bytes(refdata)
                if self.pack_extra is not None:
                    # ensure object and refs are marked alive and not removed
                    self.pack_extra.append(oid)
        c = self._conn.cursor()
        c.executemany('INSERT OR REPLACE INTO objects (id, data, refs)'
                      ' VALUES (?, ?, ?)', gen_items(records))
        self._commit()

    def end(self, handle_invalidations=None):
        self._store_records(self.pending_records)
        if is_logging(20):
            log(20, "Transaction at [%s]" % datetime.now())
        self.begin()

    def sync(self):
        """() -> [str]
        """
        result = list(self.invalid)
        self.invalid.clear()
        return result

    def _list_all_oids(self):
        c = self._conn.cursor()
        c.execute('SELECT id FROM objects ORDER BY id')
        for oid, in c.fetchall():
            yield int8_to_str(oid)

    def _gen_records(self):
        c = self._conn.cursor()
        c.execute('SELECT (id, data, refs) FROM objects ORDER BY id')
        for oid, data, refs in c.fetchall():
            yield int8_to_str(oid), pack_record(oid, data, refs)

    def gen_oid_record(self, start_oid=None, **other):
        if start_oid is None:
            for item in iteritems(self._gen_records()):
                yield item
        else:
            todo = [start_oid]
            seen = set() # This eventually contains them all.
            while todo:
                oid = todo.pop()
                if oid in seen:
                    continue
                seen.add(oid)
                record = self.load(oid)
                record_oid, data, refdata = unpack_record(record)
                assert oid == record_oid
                todo.extend(split_oids(refdata))
                yield oid, record

    def new_oid(self):
        oid = int8_to_str(self._last_oid)
        self._last_oid += 1
        return oid

    def is_temporary(self):
        return False

    def is_readonly(self):
        return False

    def _get_refs(self, oid):
        c = self._conn.cursor()
        c.execute('SELECT refs FROM objects WHERE id = ?',
                (str_to_int8(oid),))
        v = c.fetchone()
        if v is None:
            raise KeyError(oid)
        return split_oids(v[0])

    def _delete(self, oids):
        def gen_ids():
            for oid in oids:
                yield (str_to_int8(oid),)
        c = self._conn.cursor()
        c.executemany('DELETE FROM objects WHERE id = ?', gen_ids())
        self._commit()


    def get_packer(self):
        if (self.pending_records or
            self.pack_extra is not None or
            self.is_temporary() or
            self.is_readonly()):
            return [x for x in []] # Don't pack.
        self.pack_extra = []
        alive = set() # will contain OIDs of all reachable from root
        def packer():
            yield "started %s" % datetime.now()
            n = 0
            # find all reachable objects.  Note that when we yield, new
            # commits may happen and pack_extra will contain new or modified
            # OIDs.
            pack_todo = collections.deque([durus.connection.ROOT_OID])
            while pack_todo or self.pack_extra:
                if self.pack_extra:
                    oid = self.pack_extra.pop()
                    # note we don't check 'alive' because it could be an
                    # object that got updated since the pack began and in
                    # that case we have to write the new record to the pack
                    # file
                else:
                    oid = pack_todo.popleft()
                    if oid in alive:
                        continue
                alive.add(oid)
                pack_todo.extend(self._get_refs(oid))
                n += 1
                if n % self._PACK_INCREMENT == 0:
                    yield None # allow server to do other work
            # identified all reachable objects, find dead ones
            # note we cannot yield while iterating over all OIDs because
            # new ones could get created
            dead = set()
            for oid in self._list_all_oids():
                if oid not in alive:
                    dead.add(oid)
            self.pack_extra = None
            # safe to yield now, we have finished identifying dead objects
            yield None
            self._delete(dead)
            yield "finished %s, %d live objects, %d removed" % (
                    datetime.now(), len(alive), len(dead))
        return packer()

    def pack(self):
        for iteration in self.get_packer():
            pass

    def close(self):
        self._conn.close()

    def __str__(self):
        return '%s(%r)' % (self.__class__.__name__, self.get_filename())

    def create_from_records(self, oid_records):
        assert self._last_oid == 0, 'db not empty'
        def gen_recs(items):
            for oid, record in items:
                yield record
        self._store_records(gen_recs(oid_records))
