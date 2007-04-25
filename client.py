#!/usr/bin/env python
"""
$URL$
$Id$
"""
import sys
import os
import new
from code import InteractiveConsole
from durus.utils import int8_to_str, str_to_int8
from durus.file_storage import FileStorage
from durus.client_storage import ClientStorage
from durus.connection import Connection
from durus.storage_server import DEFAULT_PORT, DEFAULT_HOST, wait_for_server
from durus.storage_server import SocketAddress
from pprint import pprint

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
    console_module = new.module('__console__')
    sys.modules['__console__'] = console_module
    namespace = {'connection': connection,
                 'root': connection.get(0),
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


if __name__ == '__main__':
    client_main()
