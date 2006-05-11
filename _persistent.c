/* 
$URL$
$Id$
 */

#include "Python.h"
#include "structmember.h"

/* these constants must match the ones in persistent.py */
enum status { SAVED=0, UNSAVED=1, GHOST=-1 };

typedef struct {
	PyObject_HEAD
	enum status p_status;
	PyObject* p_touched;
	PyObject* p_connection;
	PyObject* p_oid;
} PersistentBaseObject;

typedef struct {
	PyObject_HEAD
	PyObject* sync_count;
} ConnectionBaseObject;

static PyObject *
pb_new(PyTypeObject *type, PyObject *args, PyObject *kwds) 
{
	PersistentBaseObject *x;
	x = (PersistentBaseObject *)PyType_GenericNew(type, args, kwds);
	if (x == NULL)
		return NULL;
	x->p_status = UNSAVED;
	x->p_touched = PyInt_FromLong(0L);
	x->p_connection = Py_None;
	Py_INCREF(x->p_connection);
	x->p_oid = Py_None;
	Py_INCREF(x->p_oid);
	return (PyObject *)x;
}

static void
pb_dealloc(PersistentBaseObject *self) 
{
	Py_XDECREF(self->p_touched);
	Py_XDECREF(self->p_connection);	  
	Py_XDECREF(self->p_oid);
	PyObject_GC_Del(self);
}

/* Returns true if accessing 'name' requires that the object be loaded.
 * Don't trigger a load for any attribute starting with _p_.  The names
 * __repr__ and __class__ are also exempt. */

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

static PyObject *
pb_getattro(PersistentBaseObject *self, PyObject *name)
{
	PyObject *attr;
	char *sname;
	PyObject* connection;
	if (!PyString_Check(name)) {
		PyErr_SetString(PyExc_TypeError,
				"attribute name must be a string");
		return NULL;
	}
	sname = PyString_AS_STRING(name);
	if (load_triggering_name(sname)) {
		if (self->p_status == GHOST) {
			PyObject *rv;
			rv = PyObject_CallMethod((PyObject *)self,
						 "_p_load_state", "");
			if (rv == NULL) {
				return NULL;
			}
			Py_DECREF(rv);
		}
		connection = self->p_connection;
		if (connection != Py_None) {
			self->p_touched = ((ConnectionBaseObject*)connection)->sync_count;
			Py_INCREF(self->p_touched);
		} 
	}
	attr = PyObject_GenericGetAttr((PyObject *)self, name);
	return attr;
}

static int
pb_setattro(PersistentBaseObject *self, PyObject *name, PyObject *value)
{
	char *sname;
	if (!PyString_Check(name)) {
		PyErr_SetString(PyExc_TypeError,
				"attribute name must be a string");
		return -1;
	}
	sname = PyString_AS_STRING(name);
	if (load_triggering_name(sname)) {
		if (self->p_status != UNSAVED) {
			PyObject *rv;
			rv = PyObject_CallMethod((PyObject *)self,
						 "_p_note_change", "");
			if (rv == NULL) {
				return -1;
			}
			Py_DECREF(rv);
		}
	}
	return PyObject_GenericSetAttr((PyObject *)self, name, value);
}

static int
pb_traverse(PersistentBaseObject *self, visitproc visit, void *arg)
{
	Py_VISIT(self->p_connection);	 
	Py_VISIT(self->p_oid);	  
	Py_VISIT(self->p_touched); 
	return 0;		
}

static int
pb_clear(PersistentBaseObject *self)
{
	Py_CLEAR(self->p_connection);
	Py_CLEAR(self->p_oid);
	Py_CLEAR(self->p_touched);
	return 0;
}

static PyMemberDef pb_members[] = {
	{"_p_touched", T_OBJECT_EX, offsetof(PersistentBaseObject, p_touched)},
	{"_p_status", T_INT, offsetof(PersistentBaseObject, p_status)},
	{"_p_connection", T_OBJECT_EX, offsetof(PersistentBaseObject, p_connection)},
	{"_p_oid", T_OBJECT_EX, offsetof(PersistentBaseObject, p_oid)},	   
	{NULL}
};

static char pb_doc[] = "\
This is the C implementation of PersistentBase.\n\
	Instance attributes:\n\
	  _p_touched: int\n\
	  _p_status: -1 | 0 | 1\n\
	  _p_connection: Connection | None\n\
	  _p_oid: str | None\n\
";	

static PyTypeObject PersistentBase_Type = {
	PyObject_HEAD_INIT(0)
	0,					/* ob_size */
	"durus.persistent.PersistentBase",	/* tp_name */
	sizeof(PersistentBaseObject),	/* tp_basicsize */
	0,					/* tp_itemsize */
	(destructor)pb_dealloc,	/* tp_dealloc */
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
	Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE|Py_TPFLAGS_HAVE_GC,	/*tp_flags*/
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
	x->sync_count = PyInt_FromLong(0L);
	return (PyObject *)x;
}

static void
cb_dealloc(ConnectionBaseObject *self) 
{
	Py_XDECREF(self->sync_count);
	PyObject_GC_Del(self);
}

static int
cb_traverse(ConnectionBaseObject *self, visitproc visit, void *arg)
{
	Py_VISIT(self->sync_count);	 
	return 0;		
}

static int
cb_clear(ConnectionBaseObject *self)
{
	Py_CLEAR(self->sync_count);
	return 0;
}

static PyMemberDef cb_members[] = {
	{"sync_count", T_OBJECT_EX, offsetof(ConnectionBaseObject, sync_count)},
	{NULL}
};

static char cb_doc[] = "\
This is the C implementation of ConnectionBase.\n\
	Instance attributes:\n\
	  sync_count: int\n\
";	

static PyTypeObject ConnectionBase_Type = {
	PyObject_HEAD_INIT(0)
	0,					/* ob_size */
	"durus.persistent.ConnectionBase",	/* tp_name */
	sizeof(ConnectionBaseObject),	/* tp_basicsize */
	0,					/* tp_itemsize */
	(destructor)cb_dealloc,	/* tp_dealloc */
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

static PyMethodDef persistent_module_methods[] = {
	{NULL, NULL, 0, NULL}	/* sentinel */
};

void
init_persistent(void)
{
	PyObject *m, *d;
	m = Py_InitModule4("_persistent", persistent_module_methods, "",
		NULL, PYTHON_API_VERSION);
	if (m == NULL)
		return;
	d = PyModule_GetDict(m);
	if (d == NULL)
		return;
	PersistentBase_Type.ob_type = &PyType_Type;
	if (PyType_Ready(&PersistentBase_Type) < 0)
		return;
	Py_INCREF(&PersistentBase_Type);
	if (PyDict_SetItemString(d, "PersistentBase",
		(PyObject *)&PersistentBase_Type) < 0)
		return;
	ConnectionBase_Type.ob_type = &PyType_Type;
	if (PyType_Ready(&ConnectionBase_Type) < 0)
		return;
	Py_INCREF(&ConnectionBase_Type);
	if (PyDict_SetItemString(d, "ConnectionBase",
		(PyObject *)&ConnectionBase_Type) < 0)
		return;		
}
