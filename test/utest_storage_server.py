#!/www/python/bin/python
"""
$URL$
$Id$
"""
from random import choice
from sancho.utest import UTest
from durus.storage_server import StorageServer, recv
from durus.file_storage import TempFileStorage


class Test (UTest):

    def check_storage_server(self):
        storage = TempFileStorage()
        host = '127.0.0.1'
        port = 2972
        server=StorageServer(storage, host=host, port=port)

    def check_receive(self):
        class Dribble:
            def recv(x, n):
                return choice(['a', 'bb'])[:n]
        fake_socket = Dribble()
        recv(fake_socket, 30)


if __name__ == "__main__":
#    options.show_coverage = options.show_coverage_lines = 1
    Test()

