"""
$URL$
$Id$
"""
from durus.utils import format_oid

class DurusError (StandardError):
    """Durus error."""

class DurusKeyError (KeyError, DurusError):
    """Key not found in database."""

    def __str__(self):
        return format_oid(self.args[0])


class ConflictError (DurusError):
    """
    There has been some kind of conflict involving the named oids.
    """
    def __init__(self, oids=None):
        self.oids = oids

    def __str__(self):
        if self.oids is None:
            return "conflicting oids not available"
        else:
            if len(self.oids) > 1:
                s = "oids=[%s ...]"
            else:
                s = "oids=[%s]"
            return s % format_oid(self.oids[0])


class WriteConflictError (ConflictError):
    """
    Two transactions tried to modify the same object at once.
    """

class ReadConflictError (ConflictError):
    """
    Conflict detected when object was loaded.
    An attempt was made to read an object that has changed in another
    transaction (eg. another process).
    """

class ProtocolError(DurusError):
    """
    An error occurred during communication between the storage server
    and the client.
    """
