#!/www/python/bin/python
"""$URL$
$Id$
"""
from optparse import OptionParser
from durus.file_storage import FileStorage
from durus.storage_server import DEFAULT_PORT, DEFAULT_HOST, wait_for_server
from durus.client_storage import ClientStorage
from durus.connection import Connection

def main():
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

if __name__ == '__main__':
    main()
