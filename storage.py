"""$URL$
$Id$
"""

from durus.serialize import unpack_record, split_oids, extract_class_name

class Storage(object):
    """
    This is the interface that Connection requires for Storage.
    """
    def __init__(self):
        raise RuntimeError("Storage is abstract")

    def load(self, oid):
        """Return the tid_record for this oid.
        """
        raise NotImplementedError

    def begin(self):
        """
        Begin a commit.  
        """
        raise NotImplementedError

    def store(self, record):
        """Include this record in the commit underway."""
        raise NotImplementedError

    def end(self):
        """Conclude a commit."""
        raise NotImplementedError

    def sync(self):
        """-> (tid:long, [oid:str])
        Return a transaction id and a list of oids of objects that have changes
        pending since that transaction's commit was initiated.
        """
        raise NotImplementedError

    def gen_oid_record(self):
        """() -> sequence([oid:str, record:str])
        """
        raise NotImplementedError


def gen_referring_oid_record(storage, referred_oid):
    """(storage:Storage, referred_oid:str) -> sequence([oid:str, record:str])
    Generate oid, record pairs for all objects that include a
    reference to the `referred_oid`.
    """
    for oid, record in storage.gen_oid_record():
        if referred_oid in split_oids(unpack_record(record[8:])[2]):
            yield oid, record

def gen_oid_class(storage, *classes):
    """(storage:Storage, classes:(str)) ->
        sequence([(oid:str, class_name:str)])
    Generate a sequence of oid, class_name pairs.
    If classes are provided, only output pairs for which the
    class_name is in `classes`.
    """
    for oid, record in storage.gen_oid_record():
        class_name = extract_class_name(record)
        if not classes or class_name in classes:
            yield oid, class_name

def get_census(storage):
    """(storage:Storage) -> {class_name:str, instance_count:int}"""
    result = {}
    for oid, class_name in gen_oid_class(storage):
        result[class_name] = result.get(class_name, 0) + 1
    return result

def get_reference_index(storage):
    """(storage:Storage) -> {oid:str : [referring_oid:str]}
    Return a full index giving the referring oids for each oid.
    This might be large.
    """
    result = {}
    for oid, record in storage.gen_oid_record():
        for ref in split_oids(unpack_record(record[8:])[2]):
            result.setdefault(ref, []).append(oid)
    return result

