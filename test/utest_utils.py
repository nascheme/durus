"""
$URL$
$Id$
"""
from durus.utils import format_oid, u64, p64, u32, p32
from sancho.utest import UTest


class Test (UTest):

    def check_format_oid(self):
        assert format_oid('A'*8) == '4702111234474983745'

    def check_p64_u64(self):
        for x in range(3):
            assert len(p64(x)) == 8
            assert u64(p64(x)) == x

    def check_p32_u32(self):
        for x in range(3):
            assert len(p32(x)) == 4
            assert x == u32(p32(x))


if __name__ == "__main__":
    Test()
