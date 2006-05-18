#!/usr/bin/env python
"""
$URL$
$Id$
"""
import sys
import socket
from optparse import OptionParser
from durus.storage_server import DEFAULT_PORT, DEFAULT_HOST, StorageServer
from durus.file_storage import FileStorage, TempFileStorage
from durus.logger import log, logger, direct_output

def start_durus(logfile, logginglevel, file, repair, readonly, address):
    if logfile is None:
        logfile = sys.stderr
    else:
        logfile = open(logfile, 'a+')
    direct_output(logfile)
    logger.setLevel(logginglevel)
    if file is None:
        storage = TempFileStorage()
    else:
        storage = FileStorage(file, repair=repair,
                              readonly=readonly)
    log(20, 'Storage file=%s address=%r', storage.fp.name, address)
    StorageServer(storage, address=address).serve()

def stop_durus(address):
    if type(address) is tuple:
        address_family = socket.AF_INET
    else:
        address_family = socket.AF_UNIX
    sock = socket.socket(address_family, socket.SOCK_STREAM)
    try:
        sock.connect(address)
    except socket.error, e:
        raise SystemExit("Durus server %s doesn't seem to be running." %
                          repr(address))
    sock.send('Q') # graceful exit message
    sock.close()

def run_durus_main():
    parser = OptionParser()
    parser.set_description('Run a Durus Server')
    parser.add_option(
        '--port', dest='port', default=DEFAULT_PORT, type='int',
        help='Port to listen on. (default=%s)' % DEFAULT_PORT)
    parser.add_option(
        '--file', dest='file', default=None,
        help='If this is not given, the storage is in a new temporary file.')
    parser.add_option(
        '--host', dest='host', default=DEFAULT_HOST,
        help='Host to listen on. (default=%s)' % DEFAULT_HOST)
    parser.add_option(
        '--address', dest="address", default=None,
        help=(
            "Address of the server. (default=%s)\n"
            "If given, this is the path to a Unix domain socket for "
            "the server."))
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
    if options.address is None:
        address = (options.host, options.port)
    else:
        address = options.address
    if not options.stop:
        start_durus(options.logfile,
                    options.logginglevel,
                    options.file,
                    options.repair,
                    options.readonly,
                    address)
    else:
        stop_durus(address)


if __name__ == '__main__':
    run_durus_main()

