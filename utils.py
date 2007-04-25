"""
$URL$
$Id$

In Durus names, 'int4' is an unsigned 32-bit whole number,
and 'int8' is an unsigned 64-bit whole number.
"""
from cStringIO import StringIO
from struct import pack, unpack

def str_to_int8(s):
    return unpack(">Q", s)[0]

def int8_to_str(n):
    return pack(">Q", n)

def int4_to_str(v):
    return pack(">L", v)

def str_to_int4(v):
    return unpack(">L", v)[0]


class ShortRead (IOError):
    """
    Could not read the expected number of bytes.
    """

TRACE = False

def read(f, n):
    if TRACE:
        print 'read(%r, %r)' % (f, n),
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
        result = ''.join(data)
    if TRACE:
        print "-> %r" % result
    return result

def write(f, s):
    if TRACE:
        print 'write(%r, %r)' % (f, s)
    write = getattr(f, 'write', None)
    if write is not None:
        f.write(s)
    else:
        f.sendall(s)

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
    def __init__(self, size=1024, file=None):
        if file is None:
            self.file = StringIO()
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

    def set_size(self, size):
        """(size:int)
        Append null bytes to the end of the internal file as
        necessary to expand it so that at least "size" bytes
        are available for the array.
        """
        if size <= self.size:
            return
        self.file.seek(0, 2) # seek end of file
        assert self.file.tell() >= (self.start + self.size)
        if self.file.tell() < (self.start + size):
            assert self.file.tell() == (self.start + self.size)
            # Initialize the remainder with with zero bytes
            remaining = size - self.size
            chunk = 8196 * '\x00'
            while remaining > 0:
                if remaining < len(chunk):
                    chunk = chunk[:remaining]
                write(self.file, chunk)
                remaining -= len(chunk)
        self.file.seek(self.start + size)
        self.size = size

    def __getitem__(self, j):
        if  0 <= j < self.size:
            self.file.seek(self.start + j)
            result = read(self.file, 1)
            assert len(result) == 1
            return result
        else:
            raise IndexError(j)

    def __setitem__(self, j, v):
        if  0 <= j < self.size:
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

    def __str__(self):
        """
        This reads and returns the array as a str.
        """
        self.file.seek(self.start)
        result = read(self.file, self.size)
        assert len(result) == self.size
        return result


class Byte (object):
    """
    An array of 8 bits.
    This is supposed to provide an easy way to examine and set bits in a byte.
    """
    def __init__(self, v):
        if type(v) is str and len(v) == 1:
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
            self.byte_array[q] = str(b)
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
            self.file = StringIO()
        else:
            self.file = file
        start = self.file.tell()
        try:
            # Try to read the existing data.
            dimension_str = read(self.file, 24)
            bytes = str_to_int8(dimension_str[0:8])
            bytes_per_word = str_to_int8(dimension_str[8:16])
            number_of_words = str_to_int8(dimension_str[16:24])
        except ShortRead:
            if self.file.tell() != start:
                # There was something here, but not enough.
                raise ValueError
            # There was nothing here.  Initialize.
            assert bytes_per_word is not None
            assert number_of_words is not None
            bytes = 16 + bytes_per_word * number_of_words
            write(self.file, int8_to_str(bytes))
            write(self.file, int8_to_str(bytes_per_word))
            write(self.file, int8_to_str(number_of_words))
        assert bytes == 16 + bytes_per_word * number_of_words
        self.byte_array = ByteArray(bytes_per_word * number_of_words, file=file)
        self.bytes_per_word = bytes_per_word
        self.number_of_words = number_of_words

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
        if number_of_ints is None and maximum_int is None:
            # The WordArray should already exist.
            self.word_array = WordArray(file=file)
        else:
            # We build a new WordArray.
            if maximum_int is None:
                bytes_per_word = 8
            else:
                bytes_per_word=len(int8_to_str(maximum_int + 1).lstrip('\0'))
            self.word_array = WordArray(
                file=file,
                bytes_per_word=bytes_per_word,
                number_of_words=number_of_ints)
            blank = '\xff' * bytes_per_word
            for j in xrange(number_of_ints):
                self.word_array[j] = blank
        self.pad = '\x00' * (8 - self.word_array.get_bytes_per_word())
        self.blank = '\xff' * self.word_array.get_bytes_per_word()

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
        If no argument is provided, as StringIO() is used by default.
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
