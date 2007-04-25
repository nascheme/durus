"""
$URL$
$Id$
"""
from datetime import datetime
from durus.error import ReadConflictError, ConflictError
from durus.logger import log, is_logging
from durus.serialize import extract_class_name, split_oids
from durus.utils import int4_to_str, str_to_int4, str_to_int8, read, write
from durus.utils import read_int4, read_int4_str, write_int4_str
from grp import getgrnam, getgrgid
from os import unlink, stat, chown, geteuid, getegid, umask, getpid
from os.path import exists
from pwd import getpwnam, getpwuid
from time import sleep
import errno
import select
import socket


STATUS_OKAY = 'O'
STATUS_KEYERROR = 'K'
STATUS_INVALID = 'I'

TIMEOUT = 10
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 2972



class _Client (object):

    def __init__(self, s, addr):
        self.s = s
        self.addr = addr
        self.invalid = set()
        self.unused_oids = set()

class ClientError(Exception):
    pass


class SocketAddress (object):

    def new(address, **kwargs):
        if isinstance(address, SocketAddress):
            return address
        elif type(address) is tuple:
            host, port = address
            return HostPortAddress(host=host, port=port)
        elif type(address) is str:
            return UnixDomainSocketAddress(address, **kwargs)
        else:
            raise ValueError(address)
    new = staticmethod(new)

    def get_listening_socket(self):
        sock = socket.socket(self.get_address_family(), socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.bind_socket(sock)
        sock.listen(40)
        return sock

class HostPortAddress (SocketAddress):

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT):
        self.host = host
        self.port = port

    def __str__(self):
        return "%s:%s" % (self.host, self.port)

    def get_address_family(self):
        return socket.AF_INET

    def bind_socket(self, socket):
        socket.bind( (self.host, self.port))

    def get_connected_socket(self):
        sock = socket.socket(self.get_address_family(), socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            sock.connect((self.host, self.port))
        except socket.error, exc:
            error = exc.args[0]
            if error == errno.ECONNREFUSED:
                return None
            else:
                raise
        return sock

    def set_connection_options(self, s):
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.settimeout(TIMEOUT)

    def close(self, s):
        s.close()

class UnixDomainSocketAddress (SocketAddress):

    def __init__(self, filename, owner=None, group=None, umask=None):
        self.filename = filename
        self.owner = owner
        self.group = group
        self.umask = umask

    def __str__(self):
        result = self.filename
        if exists(self.filename):
            filestat = stat(self.filename)
            uid = filestat.st_uid
            gid = filestat.st_gid
            rwx = ['---', '--x', '-w-', '-wx', 'r--', 'r-x', 'rw-', 'rwx']
            owner = getpwuid(uid).pw_name
            group = getgrgid(gid).gr_name
            result += ' (%s%s%s %s %s)' % (
               rwx[filestat.st_mode >> 6 & 7],
               rwx[filestat.st_mode >> 3 & 7],
               rwx[filestat.st_mode & 7],
               owner,
               group)
        return result

    def get_address_family(self):
        return socket.AF_UNIX

    def bind_socket(self, s):
        if self.umask is not None:
            old_umask = umask(self.umask)
        try:
            s.bind(self.filename)
        except socket.error, exc:
            error = exc.args[0]
            if not exists(self.filename):
                raise
            if stat(self.filename).st_size > 0:
                raise
            if error == errno.EADDRINUSE:
                connected = self.get_connected_socket()
                if connected:
                    connected.close()
                    raise
                unlink(self.filename)
                s.bind(self.filename)
            else:
                raise
        uid = geteuid()
        if self.owner is not None:
            if type(self.owner) is int:
                uid = self.owner
            else:
                uid = getpwnam(self.owner).pw_uid
        gid = getegid()
        if self.group is not None:
            if type(self.group) is int:
                gid = self.group
            else:
                gid = getgrnam(self.group).gr_gid
        if self.owner is not None or self.group is not None:
            chown(self.filename, uid, gid)
        if self.umask is not None:
            umask(old_umask)

    def get_connected_socket(self):
        sock = socket.socket(self.get_address_family(), socket.SOCK_STREAM)
        try:
            sock.connect(self.filename)
        except socket.error, exc:
            error = exc.args[0]
            if error in (errno.ENOENT, errno.ENOTSOCK, errno.ECONNREFUSED):
                return None
            else:
                raise
        return sock

    def set_connection_options(self, s):
        s.settimeout(TIMEOUT)

    def close(self, s):
        s.close()
        if exists(self.filename):
            unlink(self.filename)


class StorageServer (object):

    protocol = int4_to_str(1)

    def __init__(self, storage, host=DEFAULT_HOST,
                 port=DEFAULT_PORT, address=None):
        self.storage = storage
        self.clients = []
        self.sockets = []
        self.packer = None
        self.address = SocketAddress.new(address or (host, port))
        self.load_record = {}

    def serve(self):
        sock = self.address.get_listening_socket()
        log(20, 'Ready on %s with %s objects', self.address,
            self.storage.get_size())
        self.sockets.append(sock)
        try:
            while 1:
                if self.packer is not None:
                    timeout = 0.0
                else:
                    timeout = None
                r, w, e = select.select(self.sockets, [], [], timeout)
                for s in r:
                    if s is sock:
                        # new connection
                        conn, addr = s.accept()
                        self.address.set_connection_options(conn)
                        self.clients.append(_Client(conn, addr))
                        self.sockets.append(conn)
                    else:
                        # command from client
                        try:
                            self.handle(s)
                        except (ClientError, socket.error, socket.timeout,
                            IOError), exc:
                            log(10, '%s', ''.join(map(str, exc.args)))
                            self.sockets.remove(s)
                            self.clients.remove(self._find_client(s))
                if self.packer is not None:
                    try:
                        self.packer.next()
                    except StopIteration:
                        log(20, 'Pack finished at %s' % datetime.now())
                        self.packer = None # done packing
        finally:
            self.address.close(sock)

    def handle(self, s):
        command_code = read(s, 1)
        handler = getattr(self, 'handle_%s' % command_code, None)
        if handler is None:
            raise ClientError('No such command code: %r' % command_code)
        handler(s)

    def _find_client(self, s):
        for client in self.clients:
            if client.s is s:
                return client
        assert 0

    def _new_oids(self, s, count):
        oids = [self.storage.new_oid() for j in xrange(count)]
        self._find_client(s).unused_oids.update(oids)
        return oids

    def handle_N(self, s):
        # new OID
        write(s, self._new_oids(s, 1)[0])

    def handle_M(self, s):
        # new OIDs
        count = ord(read(s, 1))
        log(10, "oids: %s", count)
        write(s, ''.join(self._new_oids(s, count)))

    def handle_L(self, s):
        # load
        oid = read(s, 8)
        self._send_load_response(s, oid)

    def _send_load_response(self, s, oid):
        if oid in self._find_client(s).invalid:
            write(s, STATUS_INVALID)
        else:
            try:
                record = self.storage.load(oid)
            except KeyError:
                log(10, 'KeyError %s', str_to_int8(oid))
                write(s, STATUS_KEYERROR)
            except ReadConflictError:
                log(10, 'ReadConflictError %s', str_to_int8(oid))
                write(s, STATUS_INVALID)
            else:
                if is_logging(5):
                    class_name = extract_class_name(record)
                    if class_name in self.load_record:
                        self.load_record[class_name] += 1
                    else:
                        self.load_record[class_name] = 1
                    log(4, 'Load %-7s %s', str_to_int8(oid), class_name)
                write(s, STATUS_OKAY)
                write_int4_str(s, record)

    def handle_C(self, s):
        # commit
        self._sync_storage()
        client = self._find_client(s)
        write(s, int4_to_str(len(client.invalid)) + ''.join(client.invalid))
        client.invalid.clear()
        tdata = read_int4_str(s)
        if len(tdata) == 0:
            return # client decided not to commit (e.g. conflict)
        logging_debug = is_logging(10)
        logging_debug and log(10, 'Committing %s bytes', len(tdata))
        self.storage.begin()
        i = 0
        oids = []
        while i < len(tdata):
            rlen = str_to_int4(tdata[i:i+4])
            i += 4
            oid = tdata[i:i+8]
            record = tdata[i+8:i+rlen]
            i += rlen
            if logging_debug:
                class_name = extract_class_name(record)
                log(10, '  oid=%-6s rlen=%-6s %s',
                    str_to_int8(oid), rlen, class_name)
            self.storage.store(oid, record)
            oids.append(oid)
        assert i == len(tdata)
        oid_set = set(oids)
        for other_client in self.clients:
            if other_client is not client:
                if oid_set.intersection(other_client.unused_oids):
                    raise ClientError("invalid oid: %r" % oid)
        try:
            self.storage.end(handle_invalidations=self._handle_invalidations)
        except ConflictError:
            log(20, 'Conflict during commit')
            write(s, STATUS_INVALID)
        else:
            self._report_load_record()
            log(20, 'Committed %3s objects %s bytes at %s',
                len(oids), len(tdata), datetime.now())
            write(s, STATUS_OKAY)
            client.unused_oids -= oid_set
            for c in self.clients:
                if c is not client:
                    c.invalid.update(oids)

    def _report_load_record(self):
        if self.load_record and is_logging(5):
            log(5, '[%s]\n' % getpid() + '\n'.join(
                 "%8s: %s" % (item[1], item[0])
                 for item in sorted(self.load_record.items())))
            self.load_record.clear()

    def _handle_invalidations(self, oids):
        for c in self.clients:
            c.invalid.update(oids)

    def _sync_storage(self):
        self._handle_invalidations(self.storage.sync())

    def handle_S(self, s):
        # sync
        client = self._find_client(s)
        self._report_load_record()
        self._sync_storage()
        log(8, 'Sync %s', len(client.invalid))
        write(s, int4_to_str(len(client.invalid)) + ''.join(client.invalid))
        client.invalid.clear()

    def handle_P(self, s):
        # pack
        log(20, 'Pack started at %s' % datetime.now())
        if self.packer is None:
            self.packer = self.storage.get_packer()
            if self.packer is None:
                self.storage.pack()
                log(20, 'Pack completed at %s' % datetime.now())
        write(s, STATUS_OKAY)

    def handle_B(self, s):
        # bulk read of objects
        number_of_oids = read_int4(s)
        oid_str = read(s, 8 * number_of_oids)
        oids = split_oids(oid_str)
        for oid in oids:
            self._send_load_response(s, oid)

    def handle_Q(self, s):
        # graceful quit
        log(20, 'Quit')
        self.storage.close()
        raise SystemExit

    def handle_V(self, s):
        # Verify protocol version match.
        client_protocol = read(s, 4)
        log(10, 'Client Protocol: %s', str_to_int4(client_protocol))
        assert len(self.protocol) == 4
        write(s, self.protocol)
        if client_protocol != self.protocol:
            raise ClientError("Protocol not supported.")

def wait_for_server(host=DEFAULT_HOST, port=DEFAULT_PORT, maxtries=300,
    sleeptime=2, address=None):
    # Wait for the server to bind to the port.
    server_address = SocketAddress.new(address or (host, port))
    attempt = 0
    while attempt < maxtries:
        connected = server_address.get_connected_socket()
        if connected:
            connected.close()
            return
        sleep(sleeptime)
        attempt += 1
    raise SystemExit('Timeout waiting for address: %s.' % server_address)
