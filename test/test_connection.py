#!/www/python/bin/python
"""
$URL$
$Id$
"""

from sancho.unittest import TestScenario, parse_args, run_scenarios
from durus.file_storage import TempFileStorage
from durus.utils import p64
from durus.connection import Connection
from durus.persistent import Persistent

tested_modules = ["durus.connection"]

class Test (TestScenario):

    def check_connection(self):
        storage = TempFileStorage()
        self.test_stmt("self.conn=conn=Connection(storage)")
        self.test_stmt("self.root=root=conn.get_root()")
        self.test_val("root._p_is_ghost()", True)
        self.test_bool("root is conn.get(p64(0))")
        self.test_bool("root is conn.get(0)")
        self.test_bool("conn is root._p_connection")
        self.test_val("conn.get(p64(1))", None)
        self.test_stmt("conn.abort()")
        self.test_stmt("conn.commit()")
        self.test_val("root._p_is_ghost()", True)
        self.test_stmt("root['a'] = Persistent()")
        self.test_val("root._p_is_unsaved()", True)
        self.test_val("root['a']._p_is_unsaved()", True)
        self.test_stmt("root['a'].f=2")
        self.test_true("conn.changed.values() == [root]")
        self.test_stmt("conn.commit()")
        self.test_true("root._p_is_saved()")
        self.test_true("conn.changed.values() == []")
        self.test_stmt("root['a'] = Persistent()")
        self.test_true("conn.changed.values() == [root]")
        self.test_stmt("root['b'] = Persistent()")
        self.test_stmt("root['a'].a = 'a'")
        self.test_stmt("root['b'].b = 'b'")
        self.test_stmt("conn.commit()")
        self.test_stmt("root['a'].a = 'a'")
        self.test_stmt("root['b'].b = 'b'")
        self.test_stmt("conn.abort()")
        self.test_stmt("conn.shrink_cache()")
        self.test_stmt("root['b'].b = 'b'")
        self.test_stmt("del conn")

    def check_shrink(self):
        storage = TempFileStorage()
        self.test_stmt("self.conn=conn=Connection(storage, cache_size=3)")
        self.test_stmt("self.root=root=conn.get_root()")
        self.test_stmt("root['a'] = Persistent()")
        self.test_stmt("root['b'] = Persistent()")
        self.test_stmt("root['c'] = Persistent()")
        assert self.root._p_is_unsaved()
        self.test_stmt("conn.commit()")
        self.test_stmt("root['a'].a = 1")
        self.test_stmt("conn.commit()")
        self.test_stmt("root['b'].b = 1")
        self.test_stmt("root['c'].c = 1")
        self.test_stmt("root['d'] = Persistent()")
        self.test_stmt("root['e'] = Persistent()")
        self.test_stmt("root['f'] = Persistent()")
        self.test_stmt("conn.commit()")
        self.test_stmt("root['f'].f = 1")
        self.test_stmt("root['g'] = Persistent()")
        self.test_stmt("conn.commit()")
        self.test_stmt("conn.pack()")

if __name__ == "__main__":
    (scenarios, options) = parse_args()
    options.show_coverage = options.show_coverage_lines = 1
    run_scenarios(scenarios, options)

