#!/www/python/bin/python
"""
$URL$
$Id$
"""
from time import sleep
from os import kill
from popen2 import Popen4
from signal import SIGTERM
from sancho.unittest import TestScenario, parse_args, run_scenarios
from durus import run_durus
from durus.client_storage import ClientStorage
from durus.serialize import pack_record
from durus.utils import p64

tested_modules = ["durus.client_storage"]

class Test (TestScenario):

    def setup(self):
        self.port = 9123
        self.server = Popen4('python %s --port=%s' % (
            run_durus.__file__, self.port))
        sleep(3) # wait for bind

    def shutdown(self):
        run_durus.stop_durus("", self.port)

    def check_client_storage(self):
        self.test_stmt("b=ClientStorage(port=%s)" % self.port)
        self.test_val("b.new_oid()", p64(1))
        self.test_val("b.new_oid()", p64(2))
        self.test_exc("b.load(p64(0))", KeyError)
        record = pack_record(p64(0), 'ok', '')
        self.test_stmt("b.begin()")
        self.test_stmt("b.store(record)")
        self.test_stmt("b.end()")
        self.test_stmt("b.load(p64(0))")
        self.test_stmt("b.sync()")
        self.test_stmt("b.begin()")
        self.test_stmt("b.store(pack_record(p64(1), 'no', ''))")
        self.test_stmt("b.end()")
        self.test_stmt("b.pack()")

if __name__ == "__main__":
    (scenarios, options) = parse_args()
    options.show_coverage = options.show_coverage_lines = 1
    run_scenarios(scenarios, options)

