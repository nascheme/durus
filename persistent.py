"""
$URL: svn+ssh://svn.mems-exchange.org/repos/trunk/durus/persistent.py $
$Id: persistent.py 31094 2008-09-15 11:34:19Z dbinger $
"""
from durus.utils import str_to_int8, iteritems, as_bytes
from sys import stderr

# these must match the constants in _persistent.c
UNSAVED = 1
SAVED = 0
GHOST = -1


try:
    from durus._persistent import PersistentBase, ConnectionBase
    from durus._persistent import _setattribute, _delattribute
    from durus._persistent import _getattribute, _hasattribute
    from durus._persistent import call_if_persistent
    [ConnectionBase, _hasattribute, call_if_persistent] # silence import checker
except ImportError:
    stderr.write('Using Python base classes for persistence.\n')

    _setattribute = object.__setattr__
    _delattribute = object.__delattr__
    _getattribute = object.__getattribute__

    def _hasattribute(obj, name):
        try:
            _getattribute(obj, name)
        except AttributeError:
            return False
        else:
            return True

    class ConnectionBase(object):
        """
        The faster implementation of this class is in _persistent.c.
        """

        __slots__ = ['transaction_serial']

        def __new__(klass, *args, **kwargs):
            instance = object.__new__(klass)
            instance.transaction_serial = 1
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
          _p_serial: int
            On every access, this attribute is set to self._p_connection.serial
            (if _p_connection is not None).
          _p_connection: durus.connection.Connection | None
            The Connection to the Storage that stores this instance.
            The _p_connection is None when this instance has never been stored.
          _p_oid: str | None
            The identifier assigned when the instance was first stored.
            The _p_oid is None when this instance has never been stored.
        """

        __slots__ = ['_p_status', '_p_serial', '_p_connection', '_p_oid']

        def __new__(klass, *args, **kwargs):
            instance = object.__new__(klass)
            instance._p_status = UNSAVED
            instance._p_serial = 0
            instance._p_connection = None
            instance._p_oid = None
            return instance

        def __getattribute__(self, name):
            if name[:3] != '_p_' and name not in _GHOST_SAFE_ATTRIBUTES:
                if self._p_status == GHOST:
                    self._p_load_state()
                connection = self._p_connection
                if (connection is not None and
                    self._p_serial != connection.transaction_serial):
                    connection.note_access(self)
            return _getattribute(self, name)

        def __setattr__(self, name, value):
            if name[:3] != '_p_' and name not in _GHOST_SAFE_ATTRIBUTES:
                self._p_note_change()
            _setattribute(self, name, value)

    def call_if_persistent(f, x):
        if isinstance(x, PersistentBase):
            return f(x)
        else:
            return None


class PersistentObject (PersistentBase):
    """
    All Durus persistent objects should inherit from this class.
    """
    __slots__ = ['__weakref__']

    def _p_gen_data_slots(self):
        """Generate the sequence of names of data slots that have values.
        """
        for klass in self.__class__.__mro__:
            if klass is not PersistentBase:
                for name in getattr(klass, '__slots__', []):
                    if (name not in ('__weakref__', '__dict__') and
                        _hasattribute(self, name)):
                        yield name

    def __getstate__(self):
        if self._p_status == GHOST:
            self._p_load_state()
        state = {}
        if _hasattribute(self, '__dict__'):
            state.update(_getattribute(self, '__dict__'))
        for name in self._p_gen_data_slots():
            state[name] = _getattribute(self, name)
        return state

    def __setstate__(self, state):
        if _hasattribute(self, '__dict__'):
            _getattribute(self, '__dict__').clear()
        for name in self._p_gen_data_slots():
            _delattribute(self, name)
        if state is not None:
            for key, value in iteritems(state):
                _setattribute(self, key, value)

    def __repr__(self):
        if self._p_oid is None:
            identifier = '@%x' % id(self)
        else:
            identifier = self._p_format_oid()
        return "<%s %s>" % (self.__class__.__name__, identifier)

    def __delattr__(self, name):
        self._p_note_change()
        _delattribute(self, name)

    def _p_load_state(self):
        assert self._p_status == GHOST
        self._p_connection.load_state(self)
        self._p_set_status_saved()

    def _p_note_change(self):
        if self._p_status != UNSAVED:
            self._p_set_status_unsaved()
            self._p_connection.note_change(self)

    def _p_format_oid(self):
        oid = self._p_oid
        return str(oid and str_to_int8(as_bytes(oid)))

    def _p_set_status_ghost(self):
        self.__setstate__({})
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


class Persistent (PersistentObject):
    """
    This is the traditional persistent class of Durus.  The state is stored
    in the __dict__.
    """
    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self_dict = _getattribute(self, '__dict__')
        self_dict.clear()
        self_dict.update(state)


class ComputedAttribute (PersistentObject):
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
    __slots__ = ['value']

    def __getstate__(self):
        return None

    def _p_load_state(self):
        # don't need to read state from connection, there is none
        self._p_set_status_saved()

    def invalidate(self):
        """Forget value and mark object as UNSAVED.  On commit it will cause
        other connections to receive a invalidation notification and forget the
        value as well.
        """
        self.__setstate__(None)
        self._p_note_change()

    def get(self, compute):
        """(compute) -> value

        Compute the value (if necessary) and return it.  'compute' needs
        to be a function that takes no arguments.
        """
        # we are careful here not to mark object as UNSAVED
        if _hasattribute(self, 'value'):
            value = _getattribute(self, 'value')
        else:
            value = compute()
            _setattribute(self, 'value', value)
        return value
