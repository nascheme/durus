"""
$URL$
$Id$
"""
from durus.connection import ROOT_OID
from durus.error import InvalidObjectReference
from durus.persistent import Persistent, ConnectionBase
from durus.serialize import ObjectWriter, ObjectReader, pack_record
from durus.serialize import unpack_record, split_oids
from sancho.utest import UTest


class Test (UTest):

    def check_object_writer(self):
        class FakeConnection(ConnectionBase):
            def new_oid(self):
                return ROOT_OID
            def note_access(self, obj):
                pass
        connection = FakeConnection()
        self.s=s=ObjectWriter(connection)
        x = Persistent()
        assert x._p_connection == None
        x._p_oid = ROOT_OID
        x._p_connection = connection
        assert s._persistent_id(x) == (ROOT_OID, Persistent)
        x._p_connection = FakeConnection()
        # connection of x no longer matches connection of s.
        try:
            s._persistent_id(x)
            assert 0
        except InvalidObjectReference: pass
        x.a = Persistent()
        assert s.get_state(x), (
            '\x80\x02cdurus.persistent\nPersistent\nq\x01.\x80\x02}q\x02U'
            '\x01aU\x08\x00\x00\x00\x00\x00\x00\x00\x00q\x03h\x01\x86Qs.',
            '\x00\x00\x00\x00\x00\x00\x00\x00')
        assert list(s.gen_new_objects(x)) == [x, x.a]
        # gen_new_objects() can only be called once.
        try:
            s.gen_new_objects(3)
            assert 0
        except RuntimeError: pass
        s.close()

    def check_object_reader(self):
        class FakeConnection:
            pass
        self.r = r = ObjectReader(FakeConnection())
        root = '\x80\x02cdurus.persistent_dict\nPersistentDict\nq\x01.\x80\x02}q\x02U\x04dataq\x03}q\x04s.\x00\x00\x00\x00'
        assert r.get_ghost(root)._p_is_ghost()

    def check_record_pack_unpack(self):
        oid = '0'*8
        data = 'sample'
        reflist = ['1'*8, '2'*8]
        refs = ''.join(reflist)
        result=unpack_record(pack_record(oid, data, refs))
        assert result[0] == oid
        assert result[1] == data
        assert split_oids(result[2]) == reflist
        assert split_oids('') == []

if __name__ == "__main__":
    Test()
