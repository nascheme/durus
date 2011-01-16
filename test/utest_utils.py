"""
$URL: svn+ssh://svn.mems-exchange.org/repos/trunk/durus/test/utest_utils.py $
$Id: utest_utils.py 31294 2008-11-18 16:19:27Z dbinger $
"""
from durus.file import File
from durus.utils import Byte, ByteArray, BitArray, IntArray, WordArray, IntSet
from durus.utils import read, write
from durus.utils import read_int4, write_int4
from durus.utils import read_int4_str, write_int4_str
from durus.utils import read_int8, write_int8
from durus.utils import read_int8_str, write_int8_str
from durus.utils import ShortRead, xrange, iteritems, BytesIO, as_bytes
from durus.utils import str_to_int4, int4_to_str
from durus.utils import str_to_int8, int8_to_str
from sancho.utest import UTest, raises
import durus.utils


class UtilTest (UTest):

    def check_int8_to_str_str_to_int8(self):
        for x in range(3):
            assert len(int8_to_str(x)) == 8
            assert str_to_int8(int8_to_str(x)) == x

    def check_int4_to_str_str_to_int4(self):
        for x in range(3):
            assert len(int4_to_str(x)) == 4
            assert x == str_to_int4(int4_to_str(x))

    def b(self):
        s = BytesIO()
        for x in ('', 'a', 'ab', 'a' * 1000):
            x = as_bytes(x)
            s.seek(0)
            write(s, x)
            s.seek(0)
            assert x == read(s, len(x))

    def read_write_int4(self):
        s = BytesIO()
        for x in (0, 1, 2**30):
            s.seek(0)
            write_int4(s, x)
            s.seek(0)
            assert x == read_int4(s)

    def read_write_int8(self):
        s = BytesIO()
        for x in (0, 1, 2**60):
            s.seek(0)
            write_int8(s, x)
            s.seek(0)
            assert x == read_int8(s)

    def d(self):
        s = BytesIO()
        for x in ('', 'a', 'ab', 'a' * 1000):
            x = as_bytes(x)
            s.seek(0)
            write_int4_str(s, x)
            s.seek(0)
            assert x == read_int4_str(s)

    def e(self):
        s = BytesIO()
        durus.utils.TRACE = True
        for x in ('', 'a', 'ab', 'a' * 1000):
            x = as_bytes(x)
            s.seek(0)
            write_int8_str(s, x)
            s.seek(0)
            assert x == read_int8_str(s)
        durus.utils.TRACE = False

    def f(self):
        class FakeSocket(object):
            def recv(self, n):
                if n > 10:
                    return as_bytes('')
                return as_bytes('x')
            def send(self, s):
                return len(s)
        s = FakeSocket()
        write(s, 'x' * 2000000)
        read(s, 8)
        raises(ShortRead, read, s, 11)


class ByteArrayTest (UTest):

    def a(self):
        s = BytesIO()
        b = ByteArray(size=10000, file=s)
        assert list(b) == [as_bytes('\x00') for j in xrange(10000)], list(b)
        for j in xrange(10000):
            assert as_bytes('\x00') == b[j]
        for j in xrange(10000):
            b[j] = as_bytes('!')
        for j in xrange(10000):
            assert as_bytes('!') == b[j]
        assert b[0:3] == as_bytes('!!!')
        assert b[47:50] == as_bytes('!!!'), repr(b[47:50])
        s = BytesIO()
        b2 = ByteArray(file=s)
        b2.set_size(10000, init_byte=as_bytes('\xff'))
        for j in xrange(10000):
            assert as_bytes('\xff') == b2[j], (j, b2[j])
        s.seek(0)
        raises(AssertionError, ByteArray, size=20000, file=s)


    def b(self):
        b = ByteArray(size=50)
        raises(IndexError, b.__getitem__, 50)
        raises(IndexError, b.__setitem__, 50, as_bytes('x'))
        raises(ValueError, b.__setitem__, 1, as_bytes('xx'))
        raises(ValueError, b.__setitem__, 1, as_bytes(''))
        raises(IndexError, b.__getslice__, 0, 51)
        raises(IndexError, b.__setslice__, 0, 51, as_bytes('x') * 51)
        raises(ValueError, b.__setslice__, 0, 50, as_bytes('x') * 49)

class WordArrayTest (UTest):

    def a(self):
        for sample in (['a'], ['a', 'b'], ['ab', 'cd', 'ef']):
            sample = [as_bytes(x) for x in sample]
            s = BytesIO()
            number_of_words = len(sample)
            bytes_per_word = 0
            if sample:
                bytes_per_word = len(sample[0])
            word_array = WordArray(
                file=s,
                bytes_per_word=bytes_per_word,
                number_of_words=number_of_words)
            for j, word in enumerate(sample):
                word_array[j] = word
            assert list(word_array) == sample, (list(word_array), sample)
        assert raises(ValueError, word_array.__setitem__, 1, 'sdf')
        assert raises(IndexError, word_array.__setitem__, 10, 'sf')
        assert raises(IndexError, word_array.__getitem__, -10)

    def b(self):
        n = 1000
        s = BytesIO()
        word_array = WordArray(file=s, bytes_per_word=8, number_of_words=n)
        for x in xrange(n):
            word_array[x] = int8_to_str(x)
        assert word_array[-1] == int8_to_str(n - 1)
        for x in xrange(n):
            assert x == str_to_int8(word_array[x])
            word_array[x] = int8_to_str(2*x)
            assert x == str_to_int8(word_array[x]) / 2
        assert len(word_array) == n
        assert raises(IndexError, word_array.__getitem__, n + 1)
        s.seek(0)
        word_array2 = WordArray(file=s)
        word_array2[-1] = as_bytes('mmmmmmmm')
        assert word_array2[-1] == as_bytes('mmmmmmmm')

    def c(self):
        s = BytesIO(as_bytes('asdfasdfadsf'))
        s.seek(0)
        assert raises(ShortRead, WordArray, file=s)

    def d(self):
        file = File()
        word_array = WordArray(file=file, number_of_words=1, bytes_per_word=8)
        file.seek(0)
        word_array2 = WordArray(file=file, number_of_words=1, bytes_per_word=8)


class IntArrayTest (UTest):

    def a(self):
        s = BytesIO()
        for sample in ([], [0], [2, 1], range(7)):
            int_array = IntArray(file=s, number_of_ints=10, maximum_int=10)
            for j, x in enumerate(sample):
                int_array[j] = x
            non_blanks = set(int_array)
            non_blanks.discard(int_array.get_blank_value())
            assert set(sample) == non_blanks, (list(int_array), sample)
        assert raises(IndexError, int_array.__getitem__, 10)
        int_array2 = IntArray(file=BytesIO(s.getvalue()))
        int_array3 = IntArray(number_of_ints=10, maximum_int=300)
        for x in range(10):
            assert int_array3.get(x) == None
        assert int_array3[1] == int_array3.get_blank_value()
        int_array3[1] = 42
        assert int_array3.get(1)== 42
        assert len(int_array3) == 10
        raises(ValueError, int_array3.__setitem__, 2, 100000)
        int_array4 = IntArray(number_of_ints=10)
        assert int_array4.get(1, default=42) == 42
        assert int_array4.get(100, default=42) == 42
        assert list(iteritems(int_array4)) == []
        int_array4[3] = 4
        int_array4[8] = 9
        assert list(iteritems(int_array4)) == [(3, 4), (8, 9)]

    def b(self):
        file = File()
        int_array = IntArray(file=file, number_of_ints=10, maximum_int=10)
        file.seek(0)
        int_array2 = IntArray(file=file)
        assert len(int_array2) == 10

    def c(self):
        file = File()
        int_array = IntArray(file=file, number_of_ints=0, maximum_int=0)
        file.seek(0)
        int_array2 = IntArray(file=file)


class BitArrayTest (UTest):

    def a(self):
        m = 20
        b = BitArray(m)
        assert len(b) == m
        assert str(b) == '0' * m
        for x in range(m):
            assert b[x] == 0
        for x in range(m):
            b[x] = 1
        for x in range(m):
            assert b[x] == 1
        assert str(b) == '1' * m
        for x in range(m):
            b[x] = 0
        for x in range(m):
            assert b[x] == 0
        raises(IndexError, b.__getitem__, -1 - m)
        raises(IndexError, b.__getitem__, m)
        raises(IndexError, b.__setitem__, -1 - m, 1)


class ByteTest (UTest):

    def a(self):
        b = Byte(0)
        b[2] = 1
        b[-1] = 1
        assert b[-1] == 1
        assert [b[j] for j in range(8)] == [0,0,1,0,0,0,0,1]
        b[-1] = 0
        assert [b[j] for j in range(8)] == [0,0,1,0,0,0,0,0]
        assert int(b) == 32
        assert str(b) == chr(32)
        raises(TypeError, Byte, 300)
        raises(IndexError, b.__getitem__, -9)
        raises(IndexError, b.__getitem__, 8)
        raises(IndexError, b.__setitem__, -9, 1)
        raises(IndexError, b.__setitem__, 8, 1)


class IntSetTest (UTest):

    def a(self):
        int_set = IntSet(size=1000)
        for x in range(1000):
            assert x not in int_set
            int_set.add(x)
            assert x in int_set
            if x > 0:
                assert x - 1 in int_set
        for x in range(1000):
            int_set.discard(x)
            assert x not in int_set
            if x > 0:
                assert x - 1 not in int_set


if __name__ == '__main__':
    ByteTest()
    ByteArrayTest()
    BitArrayTest()
    IntSetTest()
    WordArrayTest()
    IntArrayTest()
    IntSetTest()
    UtilTest()
