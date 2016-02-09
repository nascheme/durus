/* 
$URL$
$Id$
 */

#include "Python.h"
#include "structmember.h"

#if PY_VERSION_HEX < 0x03000000
	#define Integer_FromLong PyInt_FromLong
	#define AttributeName_Check PyString_Check
	#define AttributeName_AsString PyString_AS_STRING
#else
	#define Integer_FromLong PyLong_FromLong
	#define AttributeName_Check PyUnicode_Check
	#define AttributeName_AsString _PyUnicode_AsString
#endif

/* these constants must match the ones in persistent.py */
enum status { SAVED=0, UNSAVED=1, GHOST=-1 };

typedef struct {
	PyObject_HEAD
	enum status p_status;
	PyObject *p_serial;
	PyObject *p_connection;
	PyObject *p_oid;
} PersistentBaseObject;

typedef struct {
	PyObject_HEAD
	PyObject *transaction_serial;
} ConnectionBaseObject;


static PyObject *
pb_new(PyTypeObject *type, PyObject *args, PyObject *kwds) 
{
	PersistentBaseObject *x;
	x = (PersistentBaseObject *)PyType_GenericNew(type, args, kwds);
	if (x == NULL)
		return NULL;
	x->p_status = UNSAVED;
	x->p_serial = Integer_FromLong(0L);
	if (x->p_serial == NULL)
		return NULL;
	x->p_connection = Py_None;
	Py_INCREF(x->p_connection);
	x->p_oid = Py_None;
	Py_INCREF(x->p_oid);
	return (PyObject *)x;
}

static void
pb_dealloc(PersistentBaseObject *self) 
{
	PyObject_GC_UnTrack(self);
	Py_TRASHCAN_SAFE_BEGIN(self);
	Py_XDECREF(self->p_connection);
	Py_XDECREF(self->p_oid);
	Py_XDECREF(self->p_serial);
	PyObject_GC_Del(self);
	Py_TRASHCAN_SAFE_END(self); 
}

static int
pb_traverse(PersistentBaseObject *self, visitproc visit, void *arg)
{
	Py_VISIT(self->p_connection);
	Py_VISIT(self->p_oid);
	Py_VISIT(self->p_serial); 
	return 0;
}

static int
pb_clear(PersistentBaseObject *self)
{
	Py_CLEAR(self->p_connection);
	Py_CLEAR(self->p_oid);
	Py_CLEAR(self->p_serial);
	return 0;
}


/* Returns true if accessing 'name' requires that the object be loaded.
 * Don't trigger a load for any attribute starting with "_p_".	The names
 * "__repr__", "__class__", and "__setstate__" are also exempt. */

static int
load_triggering_name(char *s)
{
	if (*s++ != '_')
		return 1;
	if (*s == 'p') {
		s++;
		if (*s == '_')	
			return 0; /* _p_ */
		else
			return 1; 
	}
	else if (*s == '_') {
		s++;
		switch (*s) {
		case 'r':
			return strcmp(s, "repr__");
		case 'c':
			return strcmp(s, "class__");
		case 's':
			return strcmp(s, "setstate__");
		default:
			return 1;
		}
	}
	return 1;
}

static int 
call_method(PyObject *self, char *name, PyObject *optional_arg)
{
	PyObject *result;
	if (optional_arg == NULL)
		result = PyObject_CallMethod(self, name, NULL);
	else
		result = PyObject_CallMethod(self, name, "O", optional_arg);		
	if (result == NULL)
		return 0;
	Py_DECREF(result);
	return 1;
}

/* if self is a ghost, call self._p_load_state() */
static int
pb_load(PersistentBaseObject *self)
{
	if (self->p_status == GHOST)
 		return call_method((PyObject *)self, "_p_load_state", NULL);
	else
		return 1;
}

/* if necessary, call self._p_connection.note_access(self) */
static int
pb_note_access(PersistentBaseObject *self)
{
	ConnectionBaseObject *connection;
	connection = (ConnectionBaseObject *)self->p_connection;
	if (self->p_connection != Py_None &&
	    self->p_serial != connection->transaction_serial) {
		return call_method(
			(PyObject *)connection, "note_access", (PyObject *)self);
	} else 
		return 1;
}

static PyObject *
pb_getattro(PersistentBaseObject *self, PyObject *name)
{
	char *sname;
    sname = NULL;
	if (AttributeName_Check(name)) {
	    sname = AttributeName_AsString(name);
	} else {
		PyErr_SetString(PyExc_TypeError, "attribute name must be a string");
		return NULL;
    }
	if (load_triggering_name(sname)) {
		if (!pb_load(self))
			return NULL;
		if (!pb_note_access(self))
			return NULL;
	}
	return PyObject_GenericGetAttr((PyObject *)self, name);
}

static int
pb_setattro(PersistentBaseObject *self, PyObject *name, PyObject *value)
{
	char *sname;
    sname = NULL;
	if (AttributeName_Check(name)) {
	    sname = AttributeName_AsString(name);
	} else {
		PyErr_SetString(PyExc_TypeError, "attribute name must be a string");
        return -1;
	}
	if (load_triggering_name(sname)) {
		if (self->p_status != UNSAVED) {
			if (!call_method((PyObject *)self, "_p_note_change", NULL))
				return -1;
		}
	}
	return PyObject_GenericSetAttr((PyObject *)self, name, value);
}

static PyMemberDef pb_members[] = {
	{"_p_serial", T_OBJECT_EX, offsetof(PersistentBaseObject, p_serial)},
	{"_p_status", T_INT, offsetof(PersistentBaseObject, p_status)},
	{"_p_connection", T_OBJECT_EX, offsetof(PersistentBaseObject, p_connection)},
	{"_p_oid", T_OBJECT_EX, offsetof(PersistentBaseObject, p_oid)},		 
	{NULL}
};

static char pb_doc[] = "\
This is the C implementation of PersistentBase.\n\
	Instance attributes:\n\
		_p_serial: int\n\
		_p_status: -1 | 0 | 1\n\
		_p_connection: Connection | None\n\
		_p_oid: str | None\n\
";

static PyTypeObject PersistentBase_Type = {
#if PY_VERSION_HEX < 0x03000000
 	PyObject_HEAD_INIT(0)
 	0,					/* ob_size */
#else
    PyVarObject_HEAD_INIT(0, 0)
#endif
	"durus.persistent.PersistentBase",	/* tp_name */
	sizeof(PersistentBaseObject), /* tp_basicsize */
	0,					/* tp_itemsize */
	(destructor)pb_dealloc, /* tp_dealloc */
	0,					/* tp_print */
	0,					/* tp_getattr */
	0,					/* tp_setattr */
	0,					/* tp_compare */
	0,					/* tp_repr */
	0,					/* tp_as_number */
	0,					/* tp_as_sequence */
	0,					/* tp_as_mapping */
	0,					/* tp_hash */
	0,					/* tp_call */
	0,					/* tp_str */
	(getattrofunc)pb_getattro,	/* tp_getattro */
	(setattrofunc)pb_setattro,	/* tp_setattro */
	0,					/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE|Py_TPFLAGS_HAVE_GC, /*tp_flags*/
	pb_doc,				/* tp_doc */
	(traverseproc)pb_traverse, /*tp_traverse*/
	(inquiry)pb_clear,	/*tp_clear*/
	0,					/* tp_richcompare */
	0,					/* tp_weaklistoffset */
	0,					/* tp_iter */
	0,					/* tp_iternext */
	0,					/* tp_methods */
	pb_members,			/* tp_members */
	0,					/* tp_getset */
	0,					/* tp_base */
	0,					/* tp_dict */
	0,					/* tp_descr_get */
	0,					/* tp_descr_set */
	0,					/* tp_dictoffset */
	0,					/* tp_init */
	0,					/* tp_alloc */
	(newfunc)pb_new,	/* tp_new */
};

static PyObject *
cb_new(PyTypeObject *type, PyObject *args, PyObject *kwds) 
{
	ConnectionBaseObject *x;
	x = (ConnectionBaseObject *)PyType_GenericNew(type, args, kwds);
	if (x == NULL)
		return NULL;
	x->transaction_serial = Integer_FromLong(1L);
	if (x->transaction_serial == NULL)
		return NULL;
	return (PyObject *)x;
}

static int
cb_clear(ConnectionBaseObject *self)
{
	Py_CLEAR(self->transaction_serial);
	return 0;
}

static int
cb_traverse(ConnectionBaseObject *self, visitproc visit, void *arg)
{
	Py_VISIT(self->transaction_serial);	 
	return 0;		
}

static void
cb_dealloc(ConnectionBaseObject *self) 
{
	PyObject_GC_UnTrack(self);
	Py_TRASHCAN_SAFE_BEGIN(self);
	Py_XDECREF(self->transaction_serial);
	PyObject_GC_Del(self);
	Py_TRASHCAN_SAFE_END(self); 
}

static PyMemberDef cb_members[] = {
	{"transaction_serial", T_OBJECT_EX, offsetof(ConnectionBaseObject, transaction_serial)},
	{NULL}
};

static char cb_doc[] = "\
This is the C implementation of ConnectionBase.\n\
	Instance attributes:\n\
		transaction_serial: int\n\
";	

static PyTypeObject ConnectionBase_Type = {
#if PY_VERSION_HEX < 0x03000000
 	PyObject_HEAD_INIT(0)
 	0,					/* ob_size */
#else
    PyVarObject_HEAD_INIT(0, 0)
#endif
	"durus.persistent.ConnectionBase",	/* tp_name */
	sizeof(ConnectionBaseObject), /* tp_basicsize */
	0,					/* tp_itemsize */
	(destructor)cb_dealloc, /* tp_dealloc */
	0,					/* tp_print */
	0,					/* tp_getattr */
	0,					/* tp_setattr */
	0,					/* tp_compare */
	0,					/* tp_repr */
	0,					/* tp_as_number */
	0,					/* tp_as_sequence */
	0,					/* tp_as_mapping */
	0,					/* tp_hash */
	0,					/* tp_call */
	0,					/* tp_str */
	0,					/* tp_getattro */
	0,					/* tp_setattro */
	0,					/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE|Py_TPFLAGS_HAVE_GC,	/*tp_flags*/
	cb_doc,				/* tp_doc */
	(traverseproc)cb_traverse, /*tp_traverse*/
	(inquiry)cb_clear,	/*tp_clear*/
	0,					/* tp_richcompare */
	0,					/* tp_weaklistoffset */
	0,					/* tp_iter */
	0,					/* tp_iternext */
	0,					/* tp_methods */
	cb_members,			/* tp_members */
	0,					/* tp_getset */
	0,					/* tp_base */
	0,					/* tp_dict */
	0,					/* tp_descr_get */
	0,					/* tp_descr_set */
	0,					/* tp_dictoffset */
	0,					/* tp_init */
	0,					/* tp_alloc */
	(newfunc)cb_new,	/* tp_new */
};

static PyObject *
setattribute(PyObject *self, PyObject *args)
{
	PyObject *target, *name, *value;
	value = NULL;
	if (!PyArg_UnpackTuple(args, "", 3, 3, &target, &name, &value))
		return NULL;
	if (PyObject_GenericSetAttr(target, name, value) < 0)
		return NULL;
	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *
delattribute(PyObject *self, PyObject *args)
{
	PyObject *target, *name;
	if (!PyArg_UnpackTuple(args, "", 2, 2, &target, &name))
		return NULL;
	if (PyObject_GenericSetAttr(target, name, NULL) < 0)
		return NULL;
	Py_INCREF(Py_None);
	return Py_None;
}

PyObject *
getattribute(PyObject *self, PyObject *args)
{
	PyObject *target, *name;
	if (!PyArg_UnpackTuple(args, "", 2, 2, &target, &name))
		return NULL;
	return PyObject_GenericGetAttr(target, name);
}

PyObject *
hasattribute(PyObject *self, PyObject *args)
{
	PyObject *result;
	result = getattribute(self, args);
	if (result != NULL) {
	    Py_DECREF(result);
		result = Py_True;
	} else {
		PyErr_Clear();
		result = Py_False;
	}
	Py_INCREF(result);
	return result;
}

PyObject *
call_if_persistent(PyObject *x, PyObject *args)
{
	PyObject *f;     
	PyObject *arg; 
	if (!PyArg_UnpackTuple(args, "", 2, 2, &f, &arg)) {
		return NULL;
	}
	if (PyObject_IsInstance(arg, (PyObject *)&PersistentBase_Type)) {
		return PyObject_CallFunction(f, "O", arg);
 	} else {
		Py_INCREF(Py_None);
		return Py_None;
	}
}

static char setattribute_doc[] = "\
This function acts like object.__setattr__(), except that it\n\
does not cause a persistent instance's state to be loaded and it\n\
can be applied to instances of PersistentBase when the class is\n\
implemented in C.";

static char delattribute_doc[] = "\
This function acts like object.__delattr__(), except that it\n\
does not cause a persistent instance's state to be loaded and it\n\
can be applied to instances of PersistentBase when the class is\n\
implemented in C.";

static char hasattribute_doc[] = "\
This function acts like hasattr(), except that it does not cause\n\
a persistent instance's state to be loaded.";

static char getattribute_doc[] = "\
This function acts like object.__getattribute__().";

static char call_if_persistent_doc[] = "\
If the argument is a PersistentBase, call f on it.\n\
Otherwise, return None.";

static PyMethodDef persistent_module_methods[] = {
	{"_setattribute", setattribute, METH_VARARGS, setattribute_doc},
	{"_delattribute", delattribute, METH_VARARGS, delattribute_doc},
	{"_hasattribute", hasattribute, METH_VARARGS, hasattribute_doc},
	{"_getattribute", getattribute, METH_VARARGS, getattribute_doc},
	{"call_if_persistent", call_if_persistent, 
		METH_VARARGS, call_if_persistent_doc},
	{NULL, NULL, 0, NULL} /* sentinel */
};

#if PY_VERSION_HEX >= 0x03000000
    static struct PyModuleDef persistent_module = {
        PyModuleDef_HEAD_INIT,
        "_persistent",
        "",
        -1,
        persistent_module_methods,
        NULL,
        NULL,
        NULL,
        NULL
    };
#endif

PyObject *
init_persistent_module(void)
{
	PyObject *m, *d;
#if PY_VERSION_HEX >= 0x03000000
    m = PyModule_Create(&persistent_module);
#else
	m = Py_InitModule4("_persistent", persistent_module_methods, "",
		NULL, PYTHON_API_VERSION);
#endif
	if (m == NULL)
		return NULL;
	d = PyModule_GetDict(m);
	if (d == NULL)
		return NULL;
#if PY_VERSION_HEX < 0x03000000
	PersistentBase_Type.ob_type = &PyType_Type;
#endif
	if (PyType_Ready(&PersistentBase_Type) < 0)
		return NULL;
	Py_INCREF(&PersistentBase_Type);
	if (PyDict_SetItemString(d, "PersistentBase",
		(PyObject *)&PersistentBase_Type) < 0)
		return NULL;
#if PY_VERSION_HEX < 0x03000000
	ConnectionBase_Type.ob_type = &PyType_Type;
#endif
	if (PyType_Ready(&ConnectionBase_Type) < 0)
		return NULL;
	Py_INCREF(&ConnectionBase_Type);
	if (PyDict_SetItemString(d, "ConnectionBase",
		(PyObject *)&ConnectionBase_Type) < 0)
		return NULL;
	return m;
}


#if PY_VERSION_HEX < 0x03000000
	PyMODINIT_FUNC
	init_persistent(void)
	{
		init_persistent_module();
	}
#else
	PyMODINIT_FUNC
	PyInit__persistent(void)
	{
		return init_persistent_module();
	}
#endif
