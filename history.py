"""
$URL$
$Id$
"""
from durus.connection import Connection
from durus.file_storage import FileStorage2
from durus.persistent import PersistentObject


class _HistoryIndex (dict):
    """
    A substitute offset index used in HistoryFileStorage.
    This keeps a history of updates and provides methods for
    moving backward and forward in that history.

    """
    def __init__(self, other_dict):
        dict.__init__(self)
        self.history = []
        self.future = []
        self.update(other_dict)

    def update(self, arg):
        self.history.append(arg.copy())
        dict.update(self, arg)

    def previous_transaction(self):
        if not self.history:
            return None
        transaction_offsets = self.history.pop()
        self.future.append(transaction_offsets)
        for oid in transaction_offsets:
            del self[oid]
            for offsets in reversed(self.history):
                if oid in offsets:
                    self[oid] = offsets[oid]
                    break
        return transaction_offsets.copy()

    def next_transaction(self):
        if not self.future:
            return None
        transaction_offsets = self.future.pop()
        self.history.append(transaction_offsets)
        dict.update(self, transaction_offsets)
        return transaction_offsets.copy()


class HistoryFileStorage (FileStorage2):
    """
    This variant of storage allows stepping forward and backward
    among the transaction records.
    """
    def __init__(self, filename=None, repair=False):
        assert not repair
        FileStorage2.__init__(self,
            filename=filename, repair=False)
        self.invalid = {}

    def end(self):
        raise RuntimeError("No commits allowed for this storage.")

    def _set_concrete_class_for_magic(self):
        pass

    def get_history_index(self):
        return self.history_index

    def set_history_index(self, value):
        self.history_index = _HistoryIndex(value)

    index = property(get_history_index, set_history_index)

    def previous(self):
        invalidations = self.get_history_index().previous_transaction()
        if invalidations is None:
            return False
        else:
            self.invalid.update(invalidations)
            return True

    def next(self):
        invalidations = self.get_history_index().next_transaction()
        if invalidations is None:
            return False
        else:
            self.invalid.update(invalidations)
            return True

    def sync(self):
        result = self.invalid.keys()
        self.invalid.clear()
        return result


class HistoryConnection (Connection):
    """
    A Connection that provides access to a FileStorage with
    the ability reverse and advance transactions.
    """
    def __init__(self, filename):
        Connection.__init__(self, HistoryFileStorage(filename))

    def previous(self):
        """() -> bool
        Move to the previous transaction.
        Returns False when there is no previous transaction.
        """
        result = self.get_storage().previous()
        self.abort()
        return result

    def next(self):
        """() -> bool
        Move to the next transaction.
        Returns False when there is no next transaction.
        """
        result = self.get_storage().next()
        self.abort()
        return result

    def previous_instance(self, obj):
        """
        Reverse transactions until there is some change in obj.
        When you reach a transaction in which obj no longer exists,
        the obj will be a ghost, but attempts to load the state will
        fail.
        """
        assert isinstance(obj, PersistentObject)
        if obj._p_is_ghost():
            obj._p_load_state()
        while True:
            self.previous()
            if obj._p_is_ghost():
                try:
                    obj._p_load_state()
                except KeyError:
                    return None
                else:
                    return obj

    def next_instance(self, obj):
        """
        Advance transactions until there is some change in obj.
        If obj is not a ghost after calling this, it means we are
        at the current version.
        """
        assert isinstance(obj, PersistentObject)
        if obj._p_is_ghost():
            obj._p_load_state()
        while self.next():
            if obj._p_is_ghost():
                break
        return obj
