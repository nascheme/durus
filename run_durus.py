#!/www/python/bin/python
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

def run_durus(logfile, logginglevel, file, repair, readonly, host, port):
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
    log(20, 'Storage file=%s host=%s port=%s', storage.fp.name, host, port)
    StorageServer(storage, host=host, port=port).serve()

def stop_durus(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
    except socket.error, e:
        raise SystemExit("Durus server %s:%s doesn't seem to be running." %
                         (host, port))
    sock.send('Q') # graceful exit message
    sock.close()
    

if __name__ == '__main__':
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
    if not options.stop:
        run_durus(options.logfile,
                  options.logginglevel,
                  options.file,
                  options.repair,
                  options.readonly,
                  options.host,
                  options.port)
    else:
        stop_durus(options.host,
                   options.port)
