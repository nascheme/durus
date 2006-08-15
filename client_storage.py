"""
$URL$
$Id$
"""
from durus.error import DurusKeyError, ProtocolError
from durus.error import ReadConflictError, ConflictError
from durus.serialize import split_oids
from durus.storage import Storage
from durus.storage_server import DEFAULT_PORT, DEFAULT_HOST, recv
from durus.storage_server import SocketAddress, StorageServer
from durus.storage_server import STATUS_OKAY, STATUS_KEYERROR, STATUS_INVALID
from durus.utils import p32, u32, p64, u64


class ClientStorage(Storage):

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, address=None):
        self.address = SocketAddress.new(address or (host, port))
        self.s = self.address.get_connected_socket()
        assert self.s, "Could not connect to %s" % self.address
        self.oid_pool = []
        self.oid_pool_size = 32
        self.begin()
        protocol = StorageServer.protocol
        assert len(protocol) == 4
        self.s.sendall('V' + protocol)
        server_protocol = recv(self.s, 4)
        if server_protocol != protocol:
            raise ProtocolError("Protocol version mismatch.")

    def new_oid(self):
        if not self.oid_pool:
            batch = self.oid_pool_size
            self.s.sendall('M%s' % chr(batch))
            self.oid_pool = split_oids(recv(self.s, 8 * batch))
            self.oid_pool.reverse()
        oid = self.oid_pool.pop()
        self.transaction_new_oids.append(oid)
        return oid

    def load(self, oid):
        self.s.sendall('L' + oid)
        return self._get_load_response(oid)

    def _get_load_response(self, oid):
        status = recv(self.s, 1)
        if status == STATUS_OKAY:
            pass
        elif status == STATUS_INVALID:
            raise ReadConflictError([oid])
        elif status == STATUS_KEYERROR:
            raise DurusKeyError(oid)
        else:
            raise ProtocolError('status=%r, oid=%r' % (status, oid))
        rlen = u32(recv(self.s, 4))
        record = recv(self.s, rlen)
        return record

    def begin(self):
        self.records = {}
        self.transaction_new_oids = []

    def store(self, oid, record):
        assert len(oid) == 8
        self.records[oid] = record

    def end(self, handle_invalidations=None):
        self.s.sendall('C')
        n = u32(recv(self.s, 4))
        if n != 0:
            packed_oids = recv(self.s, n*8)
            try:
                handle_invalidations(split_oids(packed_oids))
            except ConflictError:
                self.transaction_new_oids.reverse()
                self.oid_pool.extend(self.transaction_new_oids)
                self.begin() # clear out records and transaction_new_oids.
                self.s.sendall(p32(0)) # Tell server we are done.
                raise
        tdata = []
        for oid, record in self.records.iteritems():
            tdata.append(p32(8 + len(record)))
            tdata.append(oid)
            tdata.append(record)
        tdata = ''.join(tdata)
        self.s.sendall(p32(len(tdata)))
        self.s.sendall(tdata)
        self.records.clear()
        status = recv(self.s, 1)
        if status != STATUS_OKAY:
            raise ProtocolError, 'server returned invalid status %r' % status

    def sync(self):
        self.s.sendall('S')
        n = u32(recv(self.s, 4))
        if n == 0:
            packed_oids = ''
        else:
            packed_oids = recv(self.s, n*8)
        return split_oids(packed_oids)

    def pack(self):
        self.s.sendall('P')
        status = recv(self.s, 1)
        if status != STATUS_OKAY:
            raise ProtocolError, 'server returned invalid status %r' % status

    def bulk_load(self, oids):
        oid_str = ''.join(oids)
        num_oids = len(oid_str) / 8
        self.s.sendall('B' + p32(num_oids) + oid_str)
        for oid in oids:
            yield self._get_load_response(oid)

    def gen_oid_record(self):
        """() -> sequence([oid:str, record:str])
        A FileStorage will do a better job of this.
        """
        for oid_num in xrange(u64(self.new_oid())):
            try:
                oid = p64(oid_num)
                record = self.load(oid)
                yield oid, record
            except DurusKeyError:
                pass
