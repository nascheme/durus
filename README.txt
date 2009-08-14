This is Durus, a persistent object system for applications written
in the Python programming language.

Durus was written by the MEMS Exchange software development team at
the Corporation for National Research Initiatives (CNRI).  Durus is
designed to be the storage component for the Python-powered web sites
operated by the MEMS Exchange, and it provides the features that we
need for this purpose, and no more.  If you are looking for a
language-independent, long term persistent storage system for digital
objects of all kinds, designed to provide secure, managed access to
extensible server-side information services, you should read more
about CNRI's Digital Object Store® at
http://www.cnri.reston.va.us/digital_object_store.html.


* Overview:

Durus offers an easy way to use and maintain a consistent collection
of object instances used by one or more processes.  Access and change
of a persistent instances is managed through a cached Connection
instance which includes commit() and abort() methods so that changes
are transactional. 

* Quick Demo:

Run "durus -s" in one window.  This starts a durus storage server
using a temporary file and listening for clients on localhost port
2972.  Run "durus -c" in another window.  This connects to the storage
server on the port 2972 on the localhost.  When you start, you have
access to only one dictionary-like persistent object, "root". If you 
make changes to items of root and run "connection.commit()", the changes 
are written to the (in this case, temporary) file.  If you make changes 
to attributes of root, and then run "connection.abort()", the attributes 
revert back to the values they had at the last commit.

Run *another* "durus -c" in a third window, and you can see how
committed changes to root in the first client are available in
the second client when it starts.  Subsequent changes committed in
any client are visible in any other client that synchronizes by calling
either "connection.abort()" or "connection.commit()".

You can stop the server by Control-C or by running "durus -s --stop".
You can stop the clients by Control-D or by your usual method of terminating
a python interaction.

This demonstrates simple transactional behavior, but not persistence, since
the temporary file is removed as soon as the durus server is stopped.

To see how persistence works, follow the same procedure again, except 
add "--file test.durus" to the command that starts the server.  Make
changes to attributes of root, run "connection.commit()", and
"durus -s --stop", and the changes to root will be stored in
test.durus, so that you"ll see the changes again if you restart again
with the "--file test.durus" option.

Finally, note that you can run "durus -c --file test.durus" (after
stopping the durus server) to use the file storage directly and
exclusively.  Everything works the same way as before, except that no
server is involved.

Both the "durus -s" and "durus -c" commands accept "--help" command
line options that explain more about their usage.


* Using Durus in a Program:

To use Durus, a Python program needs to make a Storage instance and a
Connection instance.  For the Storage instance, you have two choices:
FileStorage or ClientStorage.  If your program is to be one of several
processes accessing a shared collection of objects, then you want
ClientStorage.  If your program has no competition, then choose
FileStorage.  There is only one Connection class, and the constructor
takes a storage instance as an argument.

Example using FileStorage to open a Connection to a file:

    from durus.file_storage import FileStorage
    from durus.connection import Connection
    connection = Connection(FileStorage("test.durus"))

Example using ClientStorage to open a Connection to a Durus server:

    from durus.client_storage import ClientStorage
    from durus.connection import Connection
    connection = Connection(ClientStorage())

Note that the ClientStorage constructor supports the "address" keyword
that you can use to specify the address to use.  The value must be either
a (host, port) tuple or a string giving a path to use for a unix domain
socket. If you provide the address you should be sure to start the
storage server the same way.  The "durus" command line tool also supports 
options to specify the address.

The connection instance has a get_root() method that you can use to
obtain the root object.

In your program, you can make changes to the root object attributes,
and call connection.commit() or connection.abort() to lock in or
revert changes made since the last commit.  The root object is
actually an instance of durus.persistent_dict.PersistentDict, which
means that it can be used like a regular dict, except that changes
will be managed by the Connection.  There is a similar class,
durus.persistent_list.PersistentList that provides list-like behavior,
except managed by the Connection.

PersistentList and PersistentDict both inherit from
durus.persistent.Persistent, and this is the key to making your own
classes participate in the Durus persistence system.  Just add
Persistent class A"s list of bases, and your instances will know how
to manage changes to their attributes through a Connection.  To
actually store an instance x of A in the storage, though, you need to
commit a reference to x in some object that is already stored in the
database.  The root object is always there, for example, so you can do
something like this:
    
    # Assume mymodule defines A as a subclass of Persistent.
    from mymodule import A 
    x = A()
    root = connection.get_root() # connection set as shown above.
    root["sample"] = x           # root is dict-like
    connection.commit()          # Now x is stored.

Subsequent changes to x, or to new A instances put on attributes of X,
and so on, will all be managed by the Connection just as for the root
object.  This management of the Persistent instance continues as long
as the instance is in the storage.  Sometimes, though, we wish to
remove "garbage" Persistent instances from the storage so that the file 
can be smaller.  This garbage collection can be done manually by calling
the Connection's pack() method.  If you are using a storage server to
share a Storage, you can use the gcinterval argument to tell it to
take care of garbage collection automatically.


* Non-Persistent Containers.

When you change an attribute of a Persistent instance, the fact that
the instance has been changed is noted with the Connection, so that
the Connection knows what instances need to be stored on the next
commit().  The same change-tracking occurs automatically when you make
dict-like changes to PersistentDict instances or list-like changes to
PersistentList instances.  If, however, you make changes to a
non-persistent container, even if it is the value of an attribute of a
Persistent instance, the changes are *not* automatically noted with
the Connection.  To make sure that your changes do get saved, you must
call the _p_note_change() method of the Persistent instance that
refers to the changed non-persistent container.  You can see an
example of this by looking at the source code of PersistentDict and
PersistentList, both of which maintain a non-persistent container on a
"data" attribute, shadow the methods of the underlying container, and
add calls to self._p_note_change() in every method that makes changes.


* Copyright:

Copyright (c) Corporation for National Research Initiatives 2009. All
Rights Reserved.


* Source of Support:

This work was supported by DARPA/MTO under Contract MDA972-03-1-0022.
