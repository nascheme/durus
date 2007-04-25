#!/usr/bin/env python
"""
$URL$
$Id$
"""
import sys
import socket
from optparse import OptionParser
from durus.client_storage import ClientStorage
from durus.storage_server import DEFAULT_PORT, DEFAULT_HOST, StorageServer
from durus.storage_server import SocketAddress
from durus.file_storage import FileStorage, TempFileStorage
from durus.logger import log, logger, direct_output
from time import sleep

def get_storage(file, repair, readonly, masterhost, masterport):
    if masterport and file:
        print "Provide a file or a masterport, but not both."
        raise SystemExit
    if file:
        return FileStorage(file, repair=repair, readonly=readonly)
    elif masterport:
        port = int(masterport)
        return ClientStorage(host=masterhost, port=port)
    else:
        return TempFileStorage()

def start_durus(logfile, logginglevel, address, storage):
    if logfile is None:
        logfile = sys.stderr
    else:
        logfile = open(logfile, 'a+')
    direct_output(logfile)
    logger.setLevel(logginglevel)
    socket_address = SocketAddress.new(address)
    if isinstance(storage, FileStorage):
        log(20, 'Storage file=%s address=%s',
            storage.get_filename(), socket_address)
    else:
        log(20, 'Storage master=%s address=%s', storage.address, socket_address)
    StorageServer(storage, address=socket_address).serve()

def stop_durus(address):
    socket_address = SocketAddress.new(address)
    sock = socket_address.get_connected_socket()
    if sock is None:
        raise SystemExit("Durus server %s doesn't seem to be running." %
                          str(address))
    sock.send('Q') # graceful exit message
    sock.close()
    # Try to wait until the address is free.
    for attempt in xrange(20):
        sleep(0.5)
        sock = socket_address.get_connected_socket()
        if sock is None:
            break
        sock.close()

def run_durus_main():
    parser = OptionParser()
    parser.set_description('Run a Durus Server')
    parser.add_option(
        '--port', dest='port', default=DEFAULT_PORT, type='int',
        help='Port to listen on. (default=%s)' % DEFAULT_PORT)
    parser.add_option(
        '--file', dest='file', default=None,
        help=('If neither this nor masterhost is given, the storage is '
        'in a new temporary file.'))
    parser.add_option(
        '--host', dest='host', default=DEFAULT_HOST,
        help='Host to listen on. (default=%s)' % DEFAULT_HOST)
    parser.add_option(
        '--masterhost', dest='masterhost', default=DEFAULT_HOST,
        help='Host of master storage server. (default=%s)' % DEFAULT_HOST)
    parser.add_option(
        '--masterport', dest='masterport', default=None,
        help='Port of masterhost. (default=None)')
    if hasattr(socket, 'AF_UNIX'):
        parser.add_option(
            '--address', dest="address", default=None,
            help=(
                "Address of the server.\n"
                "If given, this is the path to a Unix domain socket for "
                "the server."))
        parser.add_option(
            '--owner', dest="owner", default=None,
            help="Owner of the Unix domain socket (the --address value).")
        parser.add_option(
            '--group', dest="group", default=None,
            help="group of the Unix domain socket (the --address value).")
        parser.add_option(
            '--umask', dest="umask", default=None, type="int",
            help="umask for the Unix domain socket (the --address value).")
    logginglevel = logger.getEffectiveLevel()
    parser.add_option(
        '--logginglevel', dest='logginglevel', default=logginglevel, type='int',
        help=('Logging level. Lower positive numbers log more. (default=%s)' %
              logginglevel))
    parser.add_option(
        '--logfile', dest='logfile', default=None,
        help=('Log file. (default=stderr)'))
    parser.add_option(
        '--repair', dest='repair', action='store_true',
        help=('Repair the filestorage by truncating to remove anything '
              'that is malformed.  Without this option, errors '
              'will cause the program to report and terminate without '
              'attempting any repair.'))
    parser.add_option(
        '--readonly', dest='readonly', action='store_true',
        help='Open the file in read-only mode.')
    parser.add_option(
        '--stop', dest='stop', action='store_true',
        help='Instead of starting the server, try to stop a running one.')
    (options, args) = parser.parse_args()
    if getattr(options, 'address', None) is None:
        address = SocketAddress.new((options.host, options.port))
    else:
        address = SocketAddress.new(address=options.address,
            owner=options.owner, group=options.group, umask=options.umask)
    if not options.stop:
        start_durus(options.logfile,
                    options.logginglevel,
                    address,
                    get_storage(options.file, options.repair, options.readonly,
                        options.masterhost, options.masterport))
    else:
        stop_durus(address)


if __name__ == '__main__':
    run_durus_main()

