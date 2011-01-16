#!/usr/bin/env python
"""
$URL: svn+ssh://svn.mems-exchange.org/repos/trunk/durus/durus $
$Id: durus 30032 2007-09-02 18:30:57Z dbinger $
"""
from code import InteractiveConsole
from durus.client_storage import ClientStorage
from durus.connection import Connection
from durus.file_storage import FileStorage, TempFileStorage
from durus.logger import log, logger, direct_output
from durus.storage_server import DEFAULT_PORT, DEFAULT_HOST, DEFAULT_GCBYTES
from durus.storage_server import SocketAddress
from durus.storage_server import StorageServer, wait_for_server
from durus.utils import int8_to_str, str_to_int8, write
from optparse import OptionParser
from optparse import OptionParser
from pprint import pprint
from time import sleep
from types import ModuleType
import os
import socket
import sys


def configure_readline(namespace, history_path):
    try:
        import readline, rlcompleter, atexit
        readline.set_completer(
            rlcompleter.Completer(namespace=namespace).complete)
        readline.parse_and_bind("tab: complete")
        def save_history(history_path=history_path):
            readline.write_history_file(history_path)
        atexit.register(save_history)
        if os.path.exists(history_path):
            readline.read_history_file(history_path)
    except ImportError:
        pass

def interactive_client(file, address, cache_size, readonly, repair,
                       startup):
    if file:
        storage = FileStorage(file, readonly=readonly, repair=repair)
        description = file
    else:
        socket_address = SocketAddress.new(address)
        wait_for_server(address=socket_address)
        storage = ClientStorage(address=socket_address)
        description = socket_address
    connection = Connection(storage, cache_size=cache_size)
    console_module = ModuleType('__console__')
    sys.modules['__console__'] = console_module
    namespace = {'connection': connection,
                 'root': connection.get_root(),
                 'get': connection.get,
                 'sys': sys,
                 'os': os,
                 'int8_to_str': int8_to_str,
                 'str_to_int8': str_to_int8,
                 'pp': pprint}
    vars(console_module).update(namespace)
    configure_readline(
        vars(console_module), os.path.expanduser("~/.durushistory"))
    console = InteractiveConsole(vars(console_module))
    if startup:
        console.runsource('execfile("%s")' % os.path.expanduser(startup))
    help = ('    connection -> the Connection\n'
            '    root       -> the root instance')
    console.interact('Durus %s\n%s' % (description, help))

def client_main():
    from optparse import OptionParser
    parser = OptionParser()
    parser.set_description("Opens a client connection to a Durus server.")
    parser.add_option(
        '--file', dest="file", default=None,
        help="If this is not given, the storage is through a Durus server.")
    parser.add_option(
        '--port', dest="port", default=DEFAULT_PORT,
        type="int",
        help="Port the server is on. (default=%s)" % DEFAULT_PORT)
    parser.add_option(
        '--host', dest="host", default=DEFAULT_HOST,
        help="Host of the server. (default=%s)" % DEFAULT_HOST)
    parser.add_option(
        '--address', dest="address", default=None,
        help=(
            "Address of the server.\n"
            "If given, this is the path to a Unix domain socket for "
            "the server."))
    parser.add_option(
        '--cache_size', dest="cache_size", default=10000, type="int",
        help="Size of client cache (default=10000)")
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
        '--startup', dest='startup',
        default=os.environ.get('DURUSSTARTUP', ''),
        help=('Full path to a python startup file to execute on startup.'
              '(default=DURUSSTARTUP from environment, if set)')
        )
    (options, args) = parser.parse_args()
    if options.address is None:
        address = (options.host, options.port)
    else:
        address = options.address
    interactive_client(options.file, address,
                       options.cache_size, options.readonly, options.repair,
                       options.startup)

def get_storage(file, repair, readonly):
    if file:
        return FileStorage(file, repair=repair, readonly=readonly)
    else:
        return TempFileStorage()

def start_durus(logfile, logginglevel, address, storage, gcbytes):
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
    StorageServer(storage, address=socket_address, gcbytes=gcbytes).serve()

def stop_durus(address):
    socket_address = SocketAddress.new(address)
    sock = socket_address.get_connected_socket()
    if sock is None:
        log(20, "Durus server %s doesn't seem to be running." %
            str(address))
        return False
    write(sock, 'Q') # graceful exit message
    sock.close()
    # Try to wait until the address is free.
    for attempt in range(20):
        sleep(0.5)
        sock = socket_address.get_connected_socket()
        if sock is None:
            break
        sock.close()
    return True

def run_durus_main():
    parser = OptionParser()
    parser.set_description('Run a Durus Server')
    parser.add_option(
        '--port', dest='port', default=DEFAULT_PORT, type='int',
        help='Port to listen on. (default=%s)' % DEFAULT_PORT)
    parser.add_option(
        '--file', dest='file', default=None,
        help=('If not given, the storage is in a new temporary file.'))
    parser.add_option(
        '--host', dest='host', default=DEFAULT_HOST,
        help='Host to listen on. (default=%s)' % DEFAULT_HOST)
    parser.add_option(
        '--gcbytes', dest='gcbytes', default=DEFAULT_GCBYTES, type='int',
        help=('Trigger garbage collection after this many commits. (default=%s)' %
            DEFAULT_GCBYTES))
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
                    get_storage(options.file, options.repair, options.readonly),
                    options.gcbytes)
    else:
        stop_durus(address)

def pack_storage_main():
    parser = OptionParser()
    parser.set_description("Packs a Durus storage.")
    parser.add_option(
        '--file', dest="file", default=None,
        help="If this is not given, the storage is through a Durus server.")
    parser.add_option(
        '--port', dest="port", default=DEFAULT_PORT,
        type="int",
        help="Port the server is on. (default=%s)" % DEFAULT_PORT)
    parser.add_option(
        '--host', dest="host", default=DEFAULT_HOST,
        help="Host of the server. (default=%s)" % DEFAULT_HOST)
    (options, args) = parser.parse_args()
    if options.file is None:
        wait_for_server(options.host, options.port)
        storage = ClientStorage(host=options.host, port=options.port)
    else:
        storage = FileStorage(options.file)
    connection = Connection(storage)
    connection.pack()

def usage():
    sys.stdout.write(
        'durus [ -c | -s | -p ] [ -h ] [<specific options>]\n'
        '    -s   Start or stop a Durus storage server.\n'
        '    -c   Start a low-level interactive client.\n'
        '    -p   Pack a storage file.\n'
        '    -h   Get help on specific options.\n')

def main():
    if len(sys.argv) == 1:
        usage()
    else:
        arg = sys.argv[1]
        sys.argv[1:] = sys.argv[2:]
        if arg == '-c':
            client_main()
        elif arg == '-s':
            run_durus_main()
        elif arg == '-p':
            pack_storage_main()
        else:
            usage()


if __name__ == '__main__':
    main()
