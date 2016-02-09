"""
$URL$
$Id$

In Durus names, 'int4' is an unsigned 32-bit whole number,
and 'int8' is an unsigned 64-bit whole number.
"""
from struct import pack, unpack
import sys

def str_to_int8(s):
    return unpack(">Q", s)[0]

def int8_to_str(n):
    return pack(">Q", n)

def int4_to_str(v):
    return pack(">L", v)

def str_to_int4(v):
    return unpack(">L", v)[0]


if sys.version < "3":
    from __builtin__ import xrange
    from __builtin__ import str as byte_string
    def iteritems(x):
        return x.iteritems()
    def next(x):
        return x.next()
    from cStringIO import StringIO as BytesIO
    from cPickle import dumps, loads, Unpickler, Pickler
else:
    xrange = range
    from builtins import next, bytearray, bytes
    byte_string = (bytearray, bytes)
    def iteritems(x):
        return x.items()
    from io import BytesIO
    from pickle import dumps, loads, Unpickler, Pickler

_used = [dumps, loads, Unpickler, Pickler, next]  # to quiet code checker.

def as_bytes(s):
    """Return a byte_string produced from the string s."""
    if isinstance(s, byte_string):
        return s
    else:
        return s.encode('latin1')

empty_byte_string = as_bytes("")

join_bytes = empty_byte_string.join

class ShortRead (IOError):
    """
    Could not read the expected number of bytes.
    """

TRACE = False

def read(f, n):
    if TRACE:
        sys.stdout.write('read(%r, %r)' % (f, n))
    read = getattr(f, 'read', None)
    if read is not None:
        result = read(n)
        if len(result) < n:
            raise ShortRead()
    else:
        data = []
        remaining = n
        while remaining > 0:
            hunk = f.recv(min(remaining, 1000000))
            if not hunk:
                raise ShortRead()
            remaining -= len(hunk)
            data.append(hunk)
        result = join_bytes(data)
    if TRACE:
        sys.stdout.write("-> %r\n" % result)
    return result

def write(f, s):
    s_bytes = as_bytes(s)
    if TRACE:
        sys.stdout.write('write(%r, %r)\n' % (f, s_bytes))
    f_write = getattr(f, 'write', None)
    if f_write is not None:
        f_write(s_bytes)
    else:
        # was f.sendall(s_bytes)
        failures = 0
        while s_bytes:
            n = f.send(s_bytes)
            if n == 0:
                failures += 1
                if failures > 10:
                    raise IOError("send() failed")
            s_bytes = s_bytes[n:]


def write_all(f, *args):
    write(f, join_bytes(as_bytes(x) for x in args))

def read_int8(f):
    return str_to_int8(read(f, 8))

def write_int8(f, n):
    return write(f, int8_to_str(n))

def read_int8_str(f):
    return read(f, read_int8(f))

def write_int8_str(f, s):
    write_int8(f, len(s))
    write(f, s)

def read_int4(f):
    return str_to_int4(read(f, 4))

def write_int4(f, n):
    return write(f, int4_to_str(n))

def read_int4_str(f):
    return read(f, read_int4(f))

def write_int4_str(f, s):
    write_int4(f, len(s))
    write(f, s)


class ByteArray (object):
    """
    An array of bytes.  Stored internally in a file-like object
    so that it may easily be kept and accessed on disk.
    """
    def __init__(self, size=0, file=None):
        if file is None:
            self.file = BytesIO()
        else:
            self.file = file
        self.start = self.file.tell()
        self.size = 0
        self.set_size(size)

    def get_size(self):
        """() -> int
        Return the current capacity.
        """
        return self.size

    def gen_set_size(self, size, init_byte=as_bytes('\x00')):
        """(size:int)
        Append init_byte bytes to the end of the internal file as
        necessary to expand it so that at least "size" bytes
        are available for the array.
        """
        assert size >= self.size
        if size > self.size:
            self.file.seek(0, 2) # seek end of file
            # Verify that the file is big enough for the current size.
            assert self.file.tell() >= (self.start + self.size)
            if self.file.tell() < (self.start + size):
                # The file needs to be expanded.
                # We should be at the end of the array.
                assert self.file.tell() == (self.start + self.size)
                # Initialize the remainder
                remaining = size - self.size
                assert len(init_byte) == 1
                chunk = 8196 * init_byte
                while remaining > 0:
                    if remaining < len(chunk):
                        chunk = chunk[:remaining]
                    write(self.file, chunk)
                    remaining -= len(chunk)
                    yield remaining
        self.size = size
        self.file.seek(self.start + self.size)


    def set_size(self, size, init_byte=as_bytes('\x00')):
        """(size:int, init_byte:byte_string='\x00')
        Append init_bytes to the end of the internal file as
        necessary to expand it so that at least "size" bytes
        are available for the array.
        """
        for step in self.gen_set_size(size, init_byte=init_byte):
            pass

    def __getitem__(self, j):
        if isinstance(j, slice):
            if j.step in (None, 1):
                return self.__getslice__(j.start, j.stop)
            else:
                raise IndexError(j)
        if  0 <= j < self.size:
            self.file.seek(self.start + j)
            result = read(self.file, 1)
            assert len(result) == 1
            return result
        else:
            raise IndexError(j)

    def __setitem__(self, j, v):
        if isinstance(j, slice):
            if j.step in (None, 1):
                self.__setslice__(j.start, j.stop, v)
            else:
                raise IndexError(j)
        elif  0 <= j < self.size:
            if len(v) != 1:
                raise ValueError
            self.file.seek(self.start + j)
            return write(self.file, v)
        else:
            raise IndexError(j)

    def __getslice__(self, j, k):
        if  0 <= j < k <= self.size:
            self.file.seek(self.start + j)
            return read(self.file, k - j)
        else:
            raise IndexError((j, k))

    def __setslice__(self, j, k, value):
        if 0 <= j < k <= self.size:
            if len(value) != k-j:
                raise ValueError
            self.file.seek(self.start + j)
            write(self.file, value)
        else:
            raise IndexError((j, k))

    def __len__(self):
        return self.size

    def __iter__(self):
        self.file.seek(self.start)
        for j in xrange(self.size):
            byte = read(self.file, 1)
            assert len(byte) == 1
            yield byte

all_bytes = [pack("B", x) for x in range(256)]

class Byte (object):
    """
    An array of 8 bits.
    This is supposed to provide an easy way to examine and set bits in a byte.
    """
    def __init__(self, v):
        if isinstance(v, byte_string) and len(v) == 1:
            self.value = ord(v)
        elif 0 <= v <= 255:
            self.value = v
        else:
            raise TypeError(repr(v))

    def __getitem__(self, j):
        if -8 <= j <= -1:
            j += 8
        if 0 <= j <= 7:
            if (self.value & (0x80 >> j)):
                return 1
            else:
                return 0
        raise IndexError(j)

    def __setitem__(self, j, v):
        if -8 <= j <= -1:
            j += 8
        if 0 <= j <= 7:
            if v:
                self.value |= (0x80 >> j)
            else:
                self.value &= ~(0x80 >> j)
        else:
            raise IndexError(j)

    def __str__(self):
        return chr(self.value)

    def __int__(self):
        return self.value

    def byte(self):
        return all_bytes[self.value]


class BitArray (object):
    """
    This class uses a file-like object to hold an array of bits.
    """
    def __init__(self, size, file=None):
        self.byte_array = ByteArray(file=file)
        self.set_size(size)

    def get_size(self):
        return self.size

    def set_size(self, size):
        q, r = divmod(size, 8)
        if r:
            bytes = q + 1
        else:
            bytes = q
        self.byte_array.set_size(bytes)
        self.size = size

    def __getitem__(self, j):
        if j < 0:
            p = self.size + j
        else:
            p = j
        if  0 <= p < self.size:
            q, r = divmod(p, 8)
            return Byte(self.byte_array[q])[r]
        else:
            raise IndexError(j)

    def __setitem__(self, j, v):
        if j < 0:
            p = self.size + j
        else:
            p = j
        if  0 <= p < self.size:
            q, r = divmod(p, 8)
            b = Byte(self.byte_array[q])
            b[r] = v
            self.byte_array[q] = b.byte()
        else:
            raise IndexError(j)

    def __len__(self):
        return self.size

    def __iter__(self):
        n = self.size
        for byte in self.byte_array:
            for bit in Byte(byte):
                if n > 0:
                    yield bit
                n -= 1

    def __str__(self):
        return ''.join(str(x) for x in self)


class WordArray (object):
    """
    A fixed array of fixed-length words stored using a ByteArray.
    """
    def __init__(self, file=None, bytes_per_word=None, number_of_words=None):
        if file is None:
            self.file = BytesIO()
        else:
            self.file = file
        start = self.file.tell()
        self.file.seek(0, 2)
        if self.file.tell() == start:
            # Initialize for these dimensions
            for step in self.__class__.generate(self.file, bytes_per_word,
                number_of_words, as_bytes('\x00')):
                pass
        self.file.seek(start)
        dimension_str = read(self.file, 24)
        bytes = str_to_int8(dimension_str[0:8])
        bytes_per_word = str_to_int8(dimension_str[8:16])
        number_of_words = str_to_int8(dimension_str[16:24])
        assert bytes == 16 + bytes_per_word * number_of_words
        self.byte_array = ByteArray(
            bytes_per_word * number_of_words, file=file)
        self.bytes_per_word = bytes_per_word
        self.number_of_words = number_of_words

    @staticmethod
    def generate(file, bytes_per_word, number_of_words, init_byte):
        start = file.tell()
        file.seek(0, 2) # seek end
        assert start == file.tell()
        bytes = 16 + bytes_per_word * number_of_words
        write(file, int8_to_str(bytes))
        write(file, int8_to_str(bytes_per_word))
        write(file, int8_to_str(number_of_words))
        byte_array = ByteArray(size=0, file=file)
        for step in byte_array.gen_set_size(bytes_per_word * number_of_words,
            init_byte=init_byte):
            yield step
        file.seek(start)

    def __len__(self):
        return self.number_of_words

    def __getitem__(self, j):
        if j < 0:
            p = self.number_of_words + j
        else:
            p = j
        if 0 <= p < self.number_of_words:
            start = p * self.bytes_per_word
            end = start + self.bytes_per_word
            return self.byte_array[start:end]
        else:
            raise IndexError(j)

    def __setitem__(self, j, word):
        if j < 0:
            p = self.number_of_words + j
        else:
            p = j
        if 0 <= p < self.number_of_words:
            start = p * self.bytes_per_word
            end = start + self.bytes_per_word
            self.byte_array[start:end] = word
        else:
            raise IndexError(j)

    def __iter__(self):
        for j in xrange(self.number_of_words):
            yield self[j]

    def get_bytes_per_word(self):
        return self.bytes_per_word


class IntArray (object):
    """
    An array of integers, stored using a WordArray.
    If a maximum_int is provided for a new instance, the the integers are
    stored more compactly.
    The underlying WordArray is initialized using a "blank" value,
    where the word is filled with 0xff bytes.
    """
    def __init__(self, file=None, number_of_ints=None, maximum_int=None):
        if file is None:
            self.file = BytesIO()
        else:
            self.file = file
        start = self.file.tell()
        self.file.seek(0, 2)
        if self.file.tell() == start:
            # We build a new WordArray.
            for step in self.__class__.generate(
                self.file, number_of_ints, maximum_int):
                pass
        self.file.seek(start)
        self.word_array = WordArray(self.file)
        self.pad = as_bytes('\x00') * (8 - self.word_array.get_bytes_per_word())
        self.blank = as_bytes('\xff') * self.word_array.get_bytes_per_word()

    @staticmethod
    def generate(file, number_of_ints, maximum_int):
        start = file.tell()
        file.seek(0, 2)
        assert file.tell() == start
        if maximum_int is None:
            bytes_per_word = 8
        else:
            bytes_per_word=len(
                int8_to_str(maximum_int + 1).lstrip(as_bytes('\0')))
        for step in WordArray.generate(
            file, bytes_per_word, number_of_ints, as_bytes('\xff')):
            yield step
        file.seek(start)

    def get_blank_value(self):
        return str_to_int8(self.pad + self.blank)

    def get(self, j, default=None):
        try:
            word = self.word_array[j]
        except IndexError:
            return default
        if word == self.blank:
            return default
        else:
            return str_to_int8(self.pad + self.word_array[j])

    def __getitem__(self, j):
        return str_to_int8(self.pad + self.word_array[j])

    def __setitem__(self, j, value):
        s = int8_to_str(value)
        if not s.startswith(self.pad):
            raise ValueError
        word = s[len(self.pad):]
        self.word_array[j] = word

    def __iter__(self):
        for word in self.word_array:
            yield str_to_int8(self.pad + word)

    def iteritems(self):
        for j, word in enumerate(self.word_array):
            if not word == self.blank:
                yield j, str_to_int8(self.pad + word)

    items = iteritems

    def __len__(self):
        return len(self.word_array)


class IntSet (object):
    """
    A set of non-negative integers represented in a compact way.
    The space used grows with the maximum value in the set.
    """
    def __init__(self, size=(2**10 - 1), file=None):
        """
        If a file-like argument is provided, it is used to hold the set.
        If no argument is provided, as BytesIO() is used by default.
        """
        self.bit_array = BitArray(size=size, file=file)

    def add(self, n):
        if n >= self.bit_array.get_size():
            self.bit_array.set_size(int(n * 1.2))
        self.bit_array[n] = 1

    def __contains__(self, n):
        if n >= self.bit_array.get_size():
            return False
        return self.bit_array[n] == 1

    def discard(self, n):
        if n < self.bit_array.get_size():
            self.bit_array[n] = 0
