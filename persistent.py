"""
$URL$
$Id$
"""

from durus.utils import format_oid

# these must match the constants in _persistent.c
UNSAVED = 1
SAVED = 0
GHOST = -1

try:
    from _persistent import PersistentBase, ConnectionBase
    [ConnectionBase] # to silence the unused import checker
except ImportError:

    class ConnectionBase(object):

        __slots__ = ['sync_count']

        def __new__(klass, *args, **kwargs):
            instance = object.__new__(klass, *args, **kwargs)
            instance.sync_count = 0
            return instance


    _GHOST_SAFE_ATTRIBUTES = {
        '__repr__': 1,
        '__class__': 1,
        '__setstate__': 1,
    }

    class PersistentBase(object):
        """
        The faster implementation of this class is in _persistent.c.
        The __slots__ and methods of this class are the ones that typical
        applications use very frequently, so we want them to be fast.

        Instance attributes:
          _p_status: UNSAVED | SAVED | GHOST
            UNSAVED means that state that is here, self.__dict__, is usable and
              has not been stored.
            SAVED means that the state that is here, self.__dict__, is usable
              and the same as the stored state.
            GHOST means that the state that is here, self.__dict__, is empty
              and unusable until it is updated from storage.

            New instances are UNSAVED.
            UNSAVED -> SAVED
              happens when the instance state, self.__dict__, is saved.
            UNSAVED -> GHOST
              happens on an abort (if the object has previously been saved).
            SAVED   -> UNSAVED
              happens when changes are made to self.__dict__.
            SAVED   -> GHOST
              happens when the cache manager wants space.
            GHOST   -> SAVED
              happens when the instance state is loaded from the storage.
            GHOST   -> UNSAVED
              this happens when you want to make changes to self.__dict__.
              The stored state is loaded during this state transition.
          _p_touched: int
            set on every access to _p_connection.sync_count
            (if _p_connection is not None).
          _p_connection: durus.connection.Connection | None
            The Connection to the Storage that stores this instance.
            The _p_connection is None when this instance has never been stored.
          _p_oid: str | None
            The identifier assigned when the instance was first stored.
            The _p_oid is None when this instance has never been stored.
        """

        __slots__ = ['_p_status', '_p_touched', '_p_connection', '_p_oid']

        def __new__(klass, *args, **kwargs):
            instance = object.__new__(klass, *args, **kwargs)
            instance._p_status = UNSAVED
            instance._p_touched = 0
            instance._p_connection = None
            instance._p_oid = None
            return instance

        def __getattribute__(self, name):
            if name[:3] != '_p_' and name not in _GHOST_SAFE_ATTRIBUTES:
                if self._p_status == GHOST:
                    self._p_load_state()
                connection = self._p_connection
                if connection is not None:
                    self._p_touched = connection.sync_count
            return object.__getattribute__(self, name)

        def __setattr__(self, name, value):
            if name[:3] != '_p_' and name not in _GHOST_SAFE_ATTRIBUTES:
                self._p_note_change()
            object.__setattr__(self, name, value)



class Persistent(PersistentBase):
    """
    All Durus persistent objects should inherit from this class.
    """

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        object.__getattribute__(self, '__dict__').update(state)

    # This is here for ZODB-compatibility.
    # Note that setting _p_changed to a non-true value does nothing.
    _p_changed = property(
        lambda self: self._p_status == UNSAVED,
        lambda self, value: value and self._p_note_change())

    def __repr__(self):
        if self._p_oid is None:
            identifier = '@%x' % id(self)
        else:
            identifier = self._p_format_oid()
        return "<%s %s>" % (self.__class__.__name__, identifier)

    def __delattr__(self, name):
        self._p_note_change()
        PersistentBase.__delattr__(self, name)

    def _p_load_state(self):
        assert self._p_status == GHOST
        self._p_connection.load_state(self)
        self._p_set_status_saved()

    def _p_note_change(self):
        if self._p_status != UNSAVED:
            if self._p_status == GHOST:
                self._p_load_state()
            self._p_connection.note_change(self)
            self._p_status = UNSAVED

    def _p_format_oid(self):
        return format_oid(self._p_oid)

    def _p_set_status_ghost(self, getattribute=object.__getattribute__):
        d = getattribute(self, '__dict__')
        d.clear()
        self._p_status = GHOST

    def _p_set_status_saved(self):
        self._p_status = SAVED

    def _p_set_status_unsaved(self):
        if self._p_status == GHOST:
            self._p_load_state()
        self._p_status = UNSAVED

    def _p_is_ghost(self):
        return self._p_status == GHOST

    def _p_is_unsaved(self):
        return self._p_status == UNSAVED

    def _p_is_saved(self):
        return self._p_status == SAVED


_Marker = object()

class ComputedAttribute(Persistent):
    """Computed attributes do not have any state that needs to be saved in
    the database.  Instead, their value is computed based on other persistent
    objects.  Although they have no real persistent state, they do have
    OIDs.  That allows synchronize of the cached value between connections
    (necessary to maintain consistency).  If the value becomes invalid in one
    connection then it must be invalidated in all connections.  That is
    achieved by marking the object as UNSAVED and treating it like a normal
    persistent object.

    Instance attributes: none
    """

    def __getstate__(self):
        return None

    def __setstate__(self, state):
        assert state is None

    def _p_load_state(self):
        # don't need to read state from connection, there is none
        self._p_set_status_saved()

    def invalidate(self):
        """Forget value and mark object as UNSAVED.  On commit it will cause
        other connections to receive a invalidation notification and forget the
        value as well.
        """
        self.__dict__.clear()
        self._p_note_change()

    def get(self, compute):
        """(compute) -> value

        Compute the value (if necessary) and return it.  'compute' needs
        to be a function that takes no arguments.
        """
        # we are careful here not to mark object as UNSAVED
        d = self.__dict__
        value = d.get('value', _Marker)
        if value is _Marker:
            value = d['value'] = compute()
        return value
