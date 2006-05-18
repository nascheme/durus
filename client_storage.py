"""$URL$
$Id$
"""

import socket
from durus.error import DurusKeyError, ProtocolError, ConflictError
from durus.error import ReadConflictError
from durus.serialize import split_oids
from durus.storage import Storage
from durus.storage_server import DEFAULT_PORT, DEFAULT_HOST, recv
from durus.storage_server import STATUS_OKAY, STATUS_KEYERROR, STATUS_INVALID
from durus.utils import p32, u32, p64, u64


class ClientStorage(Storage):

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, address=None):
        if address is None:
            self.address = (host, port)
        else:
            self.address = address
        if type(self.address) is tuple:
            address_family = socket.AF_INET
        else:
            address_family = socket.AF_UNIX
        self.s = socket.socket(address_family, socket.SOCK_STREAM)
        if address_family == socket.AF_INET:
            self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            self.s.connect(self.address)
        except socket.error, exc:
            raise socket.error, "%r %s" % (self.address, exc)
        self.records = {}

    def new_oid(self):
        self.s.sendall('N')
        oid = recv(self.s, 8)
        return oid

    def load(self, oid):
        self.s.sendall('L' + oid)
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
