#!/www/python/bin/python
"""
$URL$
$Id$
"""
from random import choice
from sancho.unittest import TestScenario, parse_args, run_scenarios
from durus.storage_server import StorageServer, recv
from durus.file_storage import TempFileStorage

tested_modules = ["durus.storage_server"]

class Test (TestScenario):

    def check_storage_server(self):
        storage = TempFileStorage()
        host = '127.0.0.1'
        port = 2972
        self.test_stmt("server=StorageServer(storage, host=host, port=port)")

    def check_receive(self):
        class Dribble:
            def recv(x, n):
                return choice(['a', 'bb'])[:n]
        fake_socket = Dribble()
        self.test_stmt("recv(fake_socket, 30)")


if __name__ == "__main__":
    (scenarios, options) = parse_args()
#    options.show_coverage = options.show_coverage_lines = 1
    run_scenarios(scenarios, options)

