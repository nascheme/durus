#!/www/python/bin/python
"""
$URL$
$Id$
"""
from sancho.unittest import TestScenario, parse_args, run_scenarios
from durus.persistent import Persistent
from durus.file_storage import TempFileStorage
from durus.connection import Connection
from durus.utils import p64

tested_modules = ["durus.persistent"]

class Test (TestScenario):

    def check_getstate(self):
        self.test_stmt("p=Persistent()")
        self.test_val("p.__getstate__()", {})
        self.test_stmt("p.a = 1")
        self.test_val("p.__getstate__()", {'a':1})

    def check_setstate(self):
        self.test_stmt("p=Persistent()")
        self.test_stmt("p.__setstate__({})")
        self.test_stmt("p.__setstate__({'a':1})")
        self.test_val("p.a", 1)

    def check_change(self):
        self.test_stmt("p=Persistent()")
        self.test_stmt("p._p_changed == 0")
        self.test_stmt("p._p_note_change()")
        self.test_val("p._p_changed", True)

    def check_accessors(self):
        self.test_stmt("p=Persistent()")
        self.test_stmt("p._p_oid")
        self.test_val("p._p_format_oid()", 'None')
        self.test_stmt("p._p_oid = p64(1)")
        self.test_val("p._p_format_oid()", '1')
        self.test_val("repr(p)", "<Persistent 1>")

    def check_more(self):
        storage = TempFileStorage()
        connection = Connection(storage)
        self.test_stmt("root=connection.get_root()")
        self.test_true("root._p_is_ghost()")
        self.test_stmt("root.a = 1")
        self.test_true("root._p_is_unsaved()")
        self.test_stmt("del root.a")
        self.test_stmt("connection.abort()")
        self.test_true("root._p_is_ghost()")
        self.test_exc("root.a", AttributeError)
        self.test_stmt("root._p_set_status_saved()")
        self.test_true("root._p_is_saved()")
        self.test_stmt("root._p_set_status_unsaved()")
        self.test_true("root._p_is_unsaved()")
        self.test_stmt("root._p_set_status_ghost()")
        self.test_true("root._p_is_ghost()")
        self.test_stmt("root._p_set_status_unsaved()")

if __name__ == "__main__":
    (scenarios, options) = parse_args()
    options.show_coverage = options.show_coverage_lines = 1
    run_scenarios(scenarios, options)
