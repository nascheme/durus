"""
$URL$
$Id$
"""
from durus.persistent_set import PersistentSet
from pytest import raises
import pytest


@pytest.mark.parametrize(
    ('set_type', 'other_type'),
    [(set, set),
     (PersistentSet, set),
     (PersistentSet, PersistentSet)
    ]
)
class TestSet(object):

    def test__and__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        assert set(s1 & s2) == set()
        s2 = other_type(['k'])
        assert set(s1 & s2) == set(s1)
        s1 = set_type(['j', 'k'])
        assert set(s1 & s2) == set(s2)

    def test__contains__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type(['j'])
        assert s2.__contains__('j')
        assert 'j' in s2

    def test__eq__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        if set_type != other_type:
            assert not s1.__eq__(s2)
            return
        assert s1 == s2
        assert s1.__eq__(s2)
        s2 = other_type(['k'])
        assert not (s1 == s2)
        s1 = set_type(['k'])
        assert s1 == s2
        s2 = other_type(['j', 'k'])
        assert not (s1 == s2)

    def test__ge__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        if set_type != other_type:
            assert raises(TypeError, s1.__ge__, s2)
            return
        assert s1 >= s2
        s2 = other_type(['k'])
        assert not (s1 >= s2)
        assert s2 >= s1
        s1 = set_type(['k'])
        assert s1 >= s2
        assert s1.__ge__(s2)
        assert s2 >= s1
        s2 = other_type(['j', 'k'])
        assert not (s1 >= s2)
        assert s2 >= s1

    def test__gt__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        if set_type != other_type:
            assert raises(TypeError, s1.__gt__, s2)
            return
        assert not (s1 > s2)
        s2 = other_type(['k'])
        assert not (s1 > s2)
        assert s2 > s1
        assert s2.__gt__(s1)
        s1 = set_type(['k'])
        assert not (s1 > s2)
        assert not (s2 > s1)
        s2 = other_type(['j', 'k'])
        assert not (s1 > s2)
        assert s2 > s1

    def test__iand__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        assert s1.__iand__(s2) == set_type()
        s2 = other_type(['k'])
        assert s1.__iand__(s2) == s1
        assert s1 == set_type()
        s1 = set_type(['j', 'k'])
        s1 &= s2
        assert set(s1) == set(s2)
        assert set(s1.__iand__(s2)) == set(s2)
        assert set(s1) == set(s2)

    def test__ior__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        assert s1.__ior__(s2) == set_type()
        s2 = other_type(['k'])
        s1 |= s2
        assert set(s1) == set(s2)
        s1 = set_type(['j', 'k'])
        assert s1.__ior__(s2) == s1
        s3 = set_type(['g'])
        assert s1.__ior__(s3) == set_type(['j', 'k', 'g'])
        assert s1 == set_type(['j', 'k', 'g'])

    def test__isub__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        assert s1.__isub__(s2) == s1
        assert s1 == set_type()
        s2 = other_type(['k'])
        s1 -= s2
        assert s1 == set_type()
        s1 = set_type(['j', 'k'])
        s3 = set_type(['j'])
        assert s1.__isub__(s2) == s3
        assert s1 == s3

    def test__iter__(self, set_type, other_type):
        s1 = set_type()
        assert set_type(list(s1)) == s1
        s1 = set_type('abc')
        assert set_type(list(s1)) == s1

    def test__ixor__(self, set_type, other_type):
        s1 = set_type('abc')
        s2 = other_type('cfg')
        s1.__ixor__(s2)
        assert s1 == set_type('abfg')
        s1 = set_type('abc')
        s2 = other_type('cfg')
        s1 ^= s2
        assert s1 == set_type('abfg')
        s1 ^= set_type()
        assert s1 == set_type('abfg')

    def test__le__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        if set_type != other_type:
            assert raises(TypeError, s1.__le__, s2)
            return
        assert s1 <= s2
        s2 = other_type(['k'])
        assert not (s2 <= s1)
        assert s1 <= s2
        s1 = set_type(['k'])
        assert s1 <= s2
        assert s2 <= s1
        s2 = other_type(['j', 'k'])
        assert not (s2 <= s1)
        assert s1 <= s2

    def test_len(self, set_type, other_type):
        s = set_type()
        assert len(s) == 0
        s = set_type([])
        assert len(s) == 0
        s = set_type(['a'])
        assert len(s) == 1

    def test__lt__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        if set_type != other_type:
            assert raises(TypeError, s1.__lt__, s2)
            return
        assert not (s1 < s2)
        s2 = other_type(['k'])
        assert not (s2 < s1)
        assert s1 < s2
        s1 = set_type('k')
        assert not (s1 < s2)
        s2 = other_type('jk')
        assert not (s2 < s1)
        assert s1 < s2


    def test__ne__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        if set_type != other_type:
            assert s1.__ne__(s2)
            return
        assert not s1.__ne__(s2)
        s3 = set_type('a')
        assert s1 != s3
        assert s3 != s1
        assert s1 != s3

    def test__or__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        assert s1 | s2 == set_type()
        s3 = set_type('ab')
        assert s1 | s3 == s3
        s4 = set_type('bc')
        assert s3 | s4 == set_type('abc')

    def test__ror__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        assert s1.__ror__(s2) == set_type()
        s3 = set_type('ab')
        assert s1.__ror__(s3) == s3
        s4 = set_type('bc')
        assert s3.__ror__(s4) == set_type('abc')

    def test__rand__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        assert s1.__rand__(s2) == set_type()
        s3 = set_type('ab')
        assert s1.__rand__(s3) == set_type()
        s4 = set_type('bc')
        assert s3.__rand__(s4) == set_type('b')

    def test__rsub__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        assert s1.__rsub__(s2) == set_type()
        s3 = set_type('ab')
        assert s1.__rsub__(s3) == s3
        assert s3.__rsub__(s1) == set_type()
        s4 = set_type('bc')
        assert s3.__rsub__(s4) == set_type('c')

    def test__rxor__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        assert s1.__rxor__(s2) == set_type()
        s3 = set_type('ab')
        assert s1.__rxor__(s3) == s3
        assert s3.__rxor__(s1) == s3
        s4 = set_type('bc')
        assert s3.__rxor__(s4) == set_type('ac')

    def test__sub__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        assert s1 - s2 == set_type()
        s3 = set_type('ab')
        assert s3 - s1 == s3
        assert s1 - s3 == set_type()
        s4 = set_type('bc')
        assert s4 - s3 == set_type('c')

    def test__xor__(self, set_type, other_type):
        s1 = set_type()
        s2 = other_type()
        assert s1 ^ s2 == set_type()
        s3 = set_type('ab')
        assert s1 ^ s3 == s3
        assert s3 ^ s1 == s3
        s4 = set_type('bc')
        assert s3 ^ s4 == set_type('ac')

    def test_add(self, set_type, other_type):
        s1 = set_type()
        s1.add(1)
        assert s1 == set_type([1])
        s1.add(1)
        assert s1 == set_type([1])
        s1.add(2)
        assert s1 == set_type([1, 2])

    def test_clear(self, set_type, other_type):
        s1 = set_type()
        s1.clear()
        assert s1 == set_type()
        s1 = set_type('asdf')
        s1.clear()
        assert s1 == set_type()

    def test_copy(self, set_type, other_type):
        s1 = set_type()
        s2 = s1.copy()
        assert s1 is not s2
        assert s1 == s2
        assert type(s1) is type(s2)
        s1 = set_type('asdf')
        s2 = s1.copy()
        assert s1 is not s2
        assert s1 == s2
        assert type(s1) is type(s2)

    def test_discard(self, set_type, other_type):
        s1 = set_type()
        s1.discard(1)
        assert s1 == set_type()
        s1 = set_type('asdf')
        s1.discard(1)
        s1.discard('a')
        assert s1 == set_type('sdf')

    def test_pop(self, set_type, other_type):
        raises(KeyError, set_type().pop)
        s1 = set_type('asdf')
        x = s1.pop()
        assert x not in s1
        assert len(s1) == 3
        assert (s1 | set_type(x)) == set_type('asdf')

    def test_remove(self, set_type, other_type):
        s1 = set_type()
        raises(KeyError, s1.remove, 1)
        assert s1 == set_type()
        s1 = set_type('asdf')
        s1.remove('a')
        assert s1 == set_type('sdf')

