"""
$URL$
$Id$
"""
from durus.error import DurusKeyError, ProtocolError
from durus.error import ReadConflictError, ConflictError, WriteConflictError
from durus.serialize import split_oids
from durus.storage import Storage
from durus.storage_server import DEFAULT_PORT, DEFAULT_HOST
from durus.storage_server import SocketAddress, StorageServer
from durus.storage_server import STATUS_OKAY, STATUS_KEYERROR, STATUS_INVALID
from durus.utils import int4_to_str, read, write, join_bytes, write_all
from durus.utils import read_int4, write_int4, write_int4_str, iteritems
from durus.utils import as_bytes


class ClientStorage (Storage):

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, address=None):
        self.address = SocketAddress.new(address or (host, port))
        self.s = self.address.get_connected_socket()
        assert self.s, "Could not connect to %s" % self.address
        self.oid_pool = []
        self.oid_pool_size = 32
        self.begin()
        protocol = StorageServer.protocol
        assert len(protocol) == 4
        write_all(self.s, 'V', protocol)
        server_protocol = read(self.s, 4)
        if server_protocol != protocol:
            raise ProtocolError("Protocol version mismatch.")

    def __str__(self):
        return "ClientStorage(%s)" % self.address

    def new_oid(self):
        if not self.oid_pool:
            batch = self.oid_pool_size
            write(self.s, 'M%s' % chr(batch))
            self.oid_pool = split_oids(read(self.s, 8 * batch))
            self.oid_pool.reverse()
            assert len(self.oid_pool) == len(set(self.oid_pool))
        oid = self.oid_pool.pop()
        assert oid not in self.oid_pool
        self.transaction_new_oids.append(oid)
        return oid

    def load(self, oid):
        write_all(self.s, 'L', oid)
        return self._get_load_response(oid)

    def _get_load_response(self, oid):
        status = read(self.s, 1)
        if status == STATUS_OKAY:
            pass
        elif status == STATUS_INVALID:
            raise ReadConflictError([oid])
        elif status == STATUS_KEYERROR:
            raise DurusKeyError(oid)
        else:
            raise ProtocolError('status=%r, oid=%r' % (status, oid))
        n = read_int4(self.s)
        record = read(self.s, n)
        return record

    def begin(self):
        self.records = {}
        self.transaction_new_oids = []

    def store(self, oid, record):
        assert len(oid) == 8
        assert oid not in self.records
        self.records[oid] = record

    def end(self, handle_invalidations=None):
        write(self.s, 'C')
        n = read_int4(self.s)
        oid_list = []
        if n != 0:
            packed_oids = read(self.s, n*8)
            oid_list = split_oids(packed_oids)
            try:
                handle_invalidations(oid_list)
            except ConflictError:
                self.transaction_new_oids.reverse()
                self.oid_pool.extend(self.transaction_new_oids)
                assert len(self.oid_pool) == len(set(self.oid_pool))
                self.begin() # clear out records and transaction_new_oids.
                write_int4(self.s, 0) # Tell server we are done.
                raise
        tdata = []
        for oid, record in iteritems(self.records):
            tdata.append(int4_to_str(8 + len(record)))
            tdata.append(as_bytes(oid))
            tdata.append(record)
        tdata = join_bytes(tdata)
        write_int4_str(self.s, tdata)
        self.records.clear()
        if len(tdata) > 0:
            status = read(self.s, 1)
            if status == STATUS_OKAY:
                pass
            elif status == STATUS_INVALID:
                raise WriteConflictError()
            else:
                raise ProtocolError('server returned invalid status %r' % status)

    def sync(self):
        write(self.s, 'S')
        n = read_int4(self.s)
        if n == 0:
            packed_oids = ''
        else:
            packed_oids = read(self.s, n*8)
        return split_oids(packed_oids)

    def pack(self):
        write(self.s, 'P')
        status = read(self.s, 1)
        if status != STATUS_OKAY:
            raise ProtocolError('server returned invalid status %r' % status)

    def bulk_load(self, oids):
        oid_str = join_bytes(oids)
        num_oids, remainder = divmod(len(oid_str), 8)
        assert remainder == 0, remainder
        write_all(self.s, 'B', int4_to_str(num_oids), oid_str)
        records = [self._get_load_response(oid) for oid in oids]
        for record in records:
            yield record

    def close(self):
        write(self.s, '.') # Closes the server side.
        self.s.close()
