#!/www/python/bin/python
"""
$URL$
$Id$
"""
from sets import Set
from sancho.unittest import TestScenario, parse_args, run_scenarios
from durus.file_storage import TempFileStorage
from durus.serialize import pack_record
from durus.utils import p64

tested_modules = ["durus.file_storage"]

class Test (TestScenario):

    def check_file_storage(self):
        self.test_stmt("b=TempFileStorage()")
        self.test_val("b._get_tid()", p64(0))
        self.test_val("b.new_oid()", p64(1))
        self.test_val("b.new_oid()", p64(2))
        self.test_exc("b.load(p64(0))", KeyError)
        record = pack_record(p64(0), 'ok', '')
        self.test_stmt("b.store(record)")
        self.test_stmt("b.begin()")
        self.test_val("b._get_tid()", p64(1))
        self.test_stmt("b.end()")
        self.test_stmt("b.sync()")
        self.test_stmt("b.begin()")
        self.test_stmt("b.store(pack_record(p64(1), 'no', ''))")
        self.test_stmt("b.end()")
        self.test_stmt("b.pack()")


if __name__ == "__main__":
    (scenarios, options) = parse_args()
    options.show_coverage = options.show_coverage_lines = 1
    run_scenarios(scenarios, options)

