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
	int p_touched;
} PersistentObject;

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
persistent_getattro(PersistentObject *self, PyObject *name)
{
	PyObject *attr;
	char *sname;

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
		if (!self->p_touched)
			self->p_touched = 1;
	}
	attr = PyObject_GenericGetAttr((PyObject *)self, name);
	return attr;
}

static int
persistent_setattro(PersistentObject *self, PyObject *name, PyObject *value)
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
		if (!self->p_touched)
			self->p_touched = 1;
        }
        return PyObject_GenericSetAttr((PyObject *)self, name, value);
}

static int
persistent_traverse(PyObject *self, visitproc visit, void *arg)
{
	return 0;
}

static int
persistent_clear(PyObject *self)
{
	return 0;
}

static PyMemberDef persistent_members[] = {
    {"_p_touched", T_INT, offsetof(PersistentObject, p_touched)},
    {"_p_status", T_INT, offsetof(PersistentObject, p_status)},
    {NULL}
};

static char persistent_doc[] = "\
This is the C implementation of PersistentBase.\n\
    Instance attributes:\n\
      _p_touched: 0 | 1\n\
      _p_status: -1 | 0 | 1\n\
";	

static PyTypeObject Persistent_Type = {
    PyObject_HEAD_INIT(0)
    0,					/* ob_size */
    "durus.persistent.PersistentBase",	/* tp_name */
    sizeof(PersistentObject),		/* tp_basicsize */
    0,					/* tp_itemsize */
    0,					/* tp_dealloc */
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
    (getattrofunc)persistent_getattro,	/* tp_getattro */
    (setattrofunc)persistent_setattro,	/* tp_setattro */
    0,					/* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC |
    Py_TPFLAGS_BASETYPE, 		/* tp_flags */
    persistent_doc,			/* tp_doc */
    persistent_traverse,		/* tp_traverse */
    persistent_clear,			/* tp_clear */
    0,					/* tp_richcompare */
    0,					/* tp_weaklistoffset */
    0,					/* tp_iter */
    0,					/* tp_iternext */
    0,					/* tp_methods */
    persistent_members,			/* tp_members */
    0,					/* tp_getset */
    0,					/* tp_base */
    0,					/* tp_dict */
    0,					/* tp_descr_get */
    0,					/* tp_descr_set */
    0, 					/* tp_dictoffset */
    0,					/* tp_init */
    0,					/* tp_alloc */
    0,					/* tp_new */
};


static PyMethodDef persistent_methods[] = {
	{NULL,			NULL}		/* sentinel */
};

void
init_persistent(void)
{
	PyObject *m, *d;
	m = Py_InitModule4("_persistent", persistent_methods, "",
			   NULL, PYTHON_API_VERSION);
	if (m == NULL)
		return;
	d = PyModule_GetDict(m);
	if (d == NULL)
		return;

	Persistent_Type.ob_type = &PyType_Type;
        Persistent_Type.tp_new = PyType_GenericNew;
	if (PyType_Ready(&Persistent_Type) < 0)
		return;

	Py_INCREF(&Persistent_Type);
	if (PyDict_SetItemString(d, "PersistentBase",
				 (PyObject *)&Persistent_Type) < 0)
		return;

}
