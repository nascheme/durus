"""$URL$
$Id$
"""

from durus.utils import format_oid

class DurusError(StandardError):
    """Durus error."""


class DurusKeyError(KeyError, DurusError):
    """Key not found in database."""

    def __str__(self):
        return format_oid(self.args[0])

class InvalidObjectReference(DurusError):
    """
    An object contains an invalid reference to another object.

    A reference is invalid if it refers to an object managed
    by a different database connection.

    Instance attributes:
      obj: Persistent
        is the object for which the reference is invalid.
      connection: Connection
        the connection that attempted to store it.
    
    obj._p_connection != connection
    """

    def __init__(self, obj, connection):
        self.obj = obj
        self.connection = connection

    def __str__(self):
        return "Invalid reference to %r with a connection %r." % (
            self.obj,
            self.obj._p_connection)


class ConflictError(DurusError):
    """
    Two transactions tried to modify the same object at once.
    This transaction should be resubmitted.
    The object passed to the constructor should be an instance of Persistent.
    """
    def __init__(self, oids):
        self.oids = oids

    def __str__(self):
        if len(self.oids) > 1:
            s = "oids=[%s ...]"
        else:
            s = "oids=[%s]"
        return s % format_oid(self.oids[0])


class ReadConflictError(ConflictError):
    """
    Conflict detected when object was loaded.
    An attempt was made to read an object that has changed in another
    transaction (eg. another process).
    """


class ProtocolError:
    """
    An error occurred during communication between the storage server
    and the client.
    """
