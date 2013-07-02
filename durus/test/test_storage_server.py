"""
$URL$
$Id$
"""
from durus.file_storage import TempFileStorage
from durus.storage_server import StorageServer
from durus.utils import read, as_bytes
from random import choice

class Test(object):

    def test_storage_server(self):
        storage = TempFileStorage()
        host = '127.0.0.1'
        port = 2972
        server=StorageServer(storage, host=host, port=port)
        file = "test.durus_server"
        server=StorageServer(storage, address=file)

    def test_receive(self):
        class Dribble:
            def recv(x, n):
                return as_bytes(choice(['a', 'bb'])[:n])
        fake_socket = Dribble()
        read(fake_socket, 30)
