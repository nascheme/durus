#!/www/python/bin/python
"""
$URL$
$Id$
"""
from sancho.unittest import TestScenario, parse_args, run_scenarios
from durus.connection import ROOT_OID
from durus.error import InvalidObjectReference
from durus.persistent import Persistent
from durus.serialize import findrefs, ObjectWriter, ObjectReader, pack_record, \
     unpack_record, split_oids

tested_modules = ["durus.serialize"]

class Test (TestScenario):

    def check_object_writer(self):
        class FakeConnection:
            def new_oid(self):
                return ROOT_OID
        connection = FakeConnection()
        self.test_stmt("self.s=s=ObjectWriter(connection)")
        x = Persistent()
        self.test_val("x._p_connection", None)
        x._p_oid = ROOT_OID
        x._p_connection = connection
        self.test_val("s._persistent_id(x)", (ROOT_OID, Persistent))
        x._p_connection = ROOT_OID
        # connection of x no longer matches connection of s.
        self.test_exc("s._persistent_id(x)", InvalidObjectReference)
        x.a = Persistent()
        self.test_val("s.get_state(x)",
                      ('\x80\x02cdurus.persistent\nPersistent\nq\x01.\x80\x02}q\x02U'
                       '\x01aU\x08\x00\x00\x00\x00\x00\x00\x00\x00q\x03h\x01\x86Qs.',
                       '\x00\x00\x00\x00\x00\x00\x00\x00'))
        self.test_val("list(s.gen_new_objects(x))", [x, x.a])
        # gen_new_objects() can only be called once.
        self.test_exc("s.gen_new_objects(3)", RuntimeError)
        self.test_stmt("s.close()")

    def check_object_reader(self):
        class FakeConnection:
            pass
        self.test_stmt("self.r = r = ObjectReader(FakeConnection())")
        root = '\x80\x02cdurus.persistent_dict\nPersistentDict\nq\x01.\x80\x02}q\x02U\x04dataq\x03}q\x04s.\x00\x00\x00\x00'
        self.test_true("r.get_ghost(root)._p_is_ghost()")

    def check_record_pack_unpack(self):
        oid = '0'*8
        data = 'sample'
        reflist = ['1'*8, '2'*8]
        refs = ''.join(reflist)
        self.test_stmt("result=unpack_record(pack_record(oid, data, refs))")
        self.test_val("result[0]", oid)
        self.test_val("result[1]", data)
        self.test_val("split_oids(result[2])", reflist)
        self.test_val("split_oids('')", [])

if __name__ == "__main__":
    (scenarios, options) = parse_args()
    options.show_coverage = options.show_coverage_lines = 1
    run_scenarios(scenarios, options)
