"""
$URL$
$Id$
"""
from durus.file import File
from durus.utils import int8_to_str, str_to_int8, read_int8_str, IntArray
from durus.utils import iteritems, next, as_bytes
from durus.utils import read, read_int8, write, write_int8, ShortRead, xrange
import sys


class Shelf (object):
    """
    A Shelf wraps a file and uses it to hold a mapping.
    Each item in the mapping associates a string "name"
    with a string "value".  It would be like an ordinary mapping,
    except that the Shelf also maintains and provides outside
    access to the "position" for each name, which can be used
    directly to lookup the current value for a name. 
    When new values are assigned to a set of names, the changes
    occur in batches.  
    
    The Shelf class is written for use by ShelfStorage.
    The names of the Shelf are oids.
    The values of the Shelf are object records.
    The batches of changes are transactions.
    
    Here is the sequence of parts in a Shelf file:
    1) a prefix string that distinguishes the file format from that of other 
       file storages;
    2) an initial transaction;
    3) an offset mapping;
    4) a sequence of zero or more additional transactions.
    
    A transaction consists of the following:
    1) an number of bytes remaining in this transaction;
    2) a sequence of zero or more object records.

    An offset mapping consists of the following:
    1) the number of bytes remaining in this offset mapping; 
    2) the number of bytes in each entry in the mapping;
    3) the number of entries in the mapping.
 
    An object record consists of the following:
    1) the number of bytes in rest of the record;
    2) the record, as produced by durus.serialize.pack_record().

    A record produced by durus.serialize.pack_record() is as follows:
        1) an 8 byte oid;
        2) the number (4-byte, unsigned, big-endian int) of bytes in the 
           following;
        3) the pickle of the object's class followed by the (possibly zlib 
           compressed) pickle of the object's state (pickles produced in 
           sequence using the same pickler, with pickle protocol 2);
        4) a sequence of zero or more oids of persistent objects referenced 
           in the pickled object state.
    
    Except as noted, all numbers are stored as 8-byte unsigned big-endian ints.

    After the initial construction of a Shelf is completed, all subsequent
    writing happens at the end of the file.
    """
    prefix = as_bytes("SHELF-1\n")

    def __init__(self, file=None, items=None, repair=False, readonly=False):
        """(File:str:None, [(str:str)], boolean)
        """
        if file is None:
            file = File()
            assert not readonly
            assert not repair
        elif not hasattr(file, 'seek'):
            file = File(file, readonly=readonly)
        if not readonly:
            file.obtain_lock()
        file.seek(0, 2) # seek end
        if file.tell() == 0:
            # The file is empty.
            for result in self.generate_shelf(file=file, items=items or []):
                pass
        else:
            assert items is None
        # The file is not empty.
        assert self.has_format(file)
        self.file = file
        self.file.seek(len(self.prefix))
        n = read_int8(self.file) # bytes in first transaction
        self.file.seek(self.file.tell() + n)
        self.offset_map = OffsetMap(self.file)
        # Initialize the memory index.
        self.memory_index = {}
        while True:
            transaction_offsets = read_transaction_offsets(
                self.file, repair=repair)
            if transaction_offsets is None:
                break
            self.memory_index.update(transaction_offsets)
        self.file.seek_end()
        self.unused_name_generator = None

    @classmethod
    def has_format(klass, file):
        file.seek(0)
        try:
            prefix = read(file, len(klass.prefix))
        except ShortRead:
            return False
        return klass.prefix == prefix

    @classmethod
    def generate_shelf(klass, file, items):
        """(File, [(str, str)])
        This returns a generator that writes a new Shelf into file,
        iterating once through the given items.
        The use of an iterator makes it possible to build a new Shelf 
        incrementally.
        """
        file.seek_end()
        if not file.tell() == 0:
            raise ValueError("Expected %s to be empty." % file)
        write(file, klass.prefix)
        if not items:
            # Just write an empty transaction.
            write_int8(file, 0)
            # Write an empty index array.
            offset_map = OffsetMap(file)
        else:
            # Write a transaction here with the given items.
            transaction_start = file.tell()
            # Write a placeholder for the length.
            write_int8(file, 0)
            # Loop over the items, writing their records.
            # Keep track of max_key and max_offset.
            max_key = 0
            max_offset = 0
            n = 0
            for name, value in items:
                max_key = max(max_key, str_to_int8(name))
                max_offset = max(max_offset, file.tell())
                write_int8(file, len(name) + len(value))
                write(file, name)
                write(file, value)
                n += 1
                yield n
            transaction_end = file.tell()
            # Write the correct transaction length.
            file.seek(transaction_start)
            write_int8(file, transaction_end - transaction_start - 8)
            # Write the empty array with the calculated dimensions.
            file.seek(transaction_end)
            for step in OffsetMap.generate(file, max_key, max_offset):
                yield step
            offset_map = OffsetMap(file)
            # Now read through the records and record the offsets in the array.
            file.seek(transaction_start + 8)
            while file.tell() < transaction_end:
                position = file.tell()
                record_length = read_int8(file)
                name = read(file, 8)
                k = str_to_int8(name)
                offset_map[k] = position
                file.seek(position + 8 + record_length)
                n -= 1
                yield n
        for index in offset_map.gen_stitch():
            yield index

    def next_name(self):
        """() -> str
        Return the next element in a sequence of names.
        Names returned have not been used, and they have not been returned
        by previous calls to this function.
        """
        if self.unused_name_generator is None:
            def generate_unused_names():
                for j in self.offset_map.gen_holes():
                    name = int8_to_str(j)
                    if name not in self.memory_index:
                        yield name
                # Now continue with values above those in the offset map.
                j = self.offset_map.get_array_size()
                while True:
                    name = int8_to_str(j)
                    if name not in self.memory_index:
                        yield name
                    j += 1
            self.unused_name_generator = generate_unused_names()
        return next(self.unused_name_generator)

    def store(self, name_value_sequence):
        """([(str, str)]) -> [(str, int|None, int)]
        Record all of the items in the sequence.
        Return a list of triples, each giving a name, an old position (or None
        if this is a new name), and a new position.
        """
        self.file.seek_end()
        start = self.file.tell()
        write_int8(self.file, 0)
        result = []
        index = {}
        try:
            for name, value in name_value_sequence:
                new_position = self.file.tell()
                old_position = self.get_position(name)
                index[name] = new_position
                result.append((name, old_position, new_position))
                write_int8(self.file, len(name) + len(value))
                write(self.file, name)
                write(self.file, value)
        except:
            # Revert before raising.
            self.file.seek(start)
            self.file.truncate()
            raise
        end = self.file.tell()
        self.file.seek(start)
        write_int8(self.file, end - start - 8)
        self.file.seek(end)
        self.memory_index.update(index)
        return result

    def get_position(self, name):
        """(str) -> int
        Return the position of the most recent value with this name.
        """
        if len(name) != 8:
            raise ValueError("Expected a string with 8 bytes.")
        p = self.memory_index.get(name, None)
        if p is not None:
            return p
        current = self.file.tell()
        result = self.offset_map.get(str_to_int8(name), None)
        self.file.seek(current)
        if result is None or result >= self.offset_map.get_start():
            return None
        else:
            return result

    def get_item_at_position(self, position):
        """(int) -> str, str
        """
        self.file.seek(position)
        record = read_int8_str(self.file)
        return record[:8], record[8:]

    def get_value(self, name):
        position = self.get_position(name)
        if position is None:
            return None
        else:
            item = self.get_item_at_position(position)
            assert item[0] == name
            return item[1]

    def iterindex(self):
        for n, position in iteritems(self.offset_map):
            if position < self.offset_map.get_start():
                name = int8_to_str(n)
                if name not in self.memory_index:
                    yield name, position
        for item in list(self.memory_index.items()):
            yield item

    def __iter__(self):
        for name, position in self.iterindex():
            yield name

    def iteritems(self):
        for name, position in self.iterindex():
            item = self.get_item_at_position(position)
            assert item[0] == name, (name, item)
            yield item

    items = iteritems

    def __contains__(self, name):
        return self.get_position(name) != None

    def get_offset_map(self):
        return self.offset_map

    def get_file(self):
        return self.file

    def close(self):
        self.file.close()


def read_transaction_offsets(file, repair=False):
    """
    Read the offsets of one (Shelf-format) transaction in the file.
    If repair is True and the file ends in something other than a well
    formed transaction, the file is truncated to remove the ugly
    ending.  This ugliness might occur if the power fails in the middle 
    of writing a transaction.
    """
    transaction_start = transaction_end = position = file.tell()
    try:
        transaction_length = read_int8(file)
        transaction_end = transaction_start + 8 + transaction_length
        transaction_offsets = {}
        while file.tell() < transaction_end:
            position = file.tell()
            record_length = read_int8(file)
            identifier = read(file, 8)
            transaction_offsets[identifier] = position
            file.seek(position + 8 + record_length)
        if file.tell() != transaction_end:
            raise ShortRead
        return transaction_offsets
    except ShortRead:
        position = file.tell()
        if position > transaction_start:
            if repair:
                file.seek(transaction_start)
                file.truncate()
            else:
                e = sys.exc_info()[1]
                e.args = repr(dict(
                    transaction_start=transaction_start,
                    transaction_end = transaction_end,
                    position=position))
                raise
        return None


class OffsetMap (object):
    """
    An offset map holds the offsets for a set of oids.
    It uses an inner array to hold the offsets.  It does not actually
    store any oids.  Instead it uses oids as indices into an array
    that is big enough to hold all oids.  The array will probably have
    more slots than we have oids.  We call the unused slots holes, and
    this class gives us a way to iterate over the holes, so that they
    can be allocated when oids are needed for new objects.
    """
    def __init__(self, file, max_oid=-2, max_offset=0):
        self.start = file.tell()
        file.seek(0, 2)
        if file.tell() == self.start:
            for step in self.__class__.generate(file, max_oid, max_offset):
                pass
        file.seek(self.start)
        self.int_array = IntArray(file=file)

    @staticmethod
    def generate(file, max_oid=-2, max_offset=0):
        start = file.tell()
        assert max_offset < start
        for step in IntArray.generate(file=file, number_of_ints=max_oid + 2,
            maximum_int=start + max_oid + 2):
            yield step
        file.seek(start)

    def get_start(self):
        return self.start

    def get(self, j, default=None):
        result = self.int_array.get(j, default=None)
        if result is None or result >= self.start:
            return default
        else:
            return result

    def __getitem__(self, j):
        result = self.get(j)
        if result is None:
            raise IndexError(j)
        else:
            return result

    def __setitem__(self, j, value):
        """
        Note that this is not called after self.gen_stitch() is consumed.
        We don't overwrite any non-blank values.
        """
        assert self.get(j) is None
        self.int_array[j] = value

    def __iter__(self):
        for j, number in enumerate(self.int_array):
            if number < self.start:
                yield j

    def iteritems(self):
        for j, number in enumerate(self.int_array):
            if number < self.start:
                yield j, number

    items = iteritems

    def get_array_size(self):
        """() -> int
        Note that this is the total capacity of the array.
        """
        return len(self.int_array)

    def gen_stitch(self):
        """
        Return a generator that does the following as it is consumed.
        Build the linked list of holes.
        Each value is the index of the next hole plus self.start.
        The offset is added so that we can distinguish ordinary offsets,
        which are less than self.start, from elements of this linked list.
        """
        last_index = len(self.int_array) - 1
        for index in xrange(0, len(self.int_array)):
            if self.get(index) is None:
                self.int_array[index] = last_index + self.start
                last_index = index
            yield index

    def gen_holes(self):
        """Generate the sequence of holes."""
        last_index = len(self.int_array) - 1
        if last_index >= 0:
            j = last_index
            while True:
                new_j = self.int_array[j] - self.start
                yield new_j
                if new_j == last_index:
                    break
                j = new_j

