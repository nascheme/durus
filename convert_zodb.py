#!/www/python/bin/python
"""$URL$
$Id$
"""
import sys, os, cPickle, cStringIO
from ZODB.FileStorage import FileStorage as ZODBFileStorage
from ZODB.referencesf import referencesf
from durus.serialize import pack_record
from durus.file_storage import FileStorage

def convert(zodb_file_name, durus_file_name):
    """Read a ZODB FileStorage and write a new Durus FileStorage."""

    def generate_durus_object_records():
        sio = cStringIO.StringIO()
        zodb_storage = ZODBFileStorage(zodb_file_name)
        n = 0
        for oid in zodb_storage._index.keys():
            n += 1
            if n % 10000 == 0:
                sys.stdout.write('.')
                sys.stdout.flush()
            p, serial = zodb_storage.load(oid, '')
            refs = referencesf(p)
            # unwrap extra tuple from class meta data
            sio.seek(0)
            sio.write(p)
            sio.truncate()
            sio.seek(0)
            def get_class(module_class):
                module, klass = module_class
                if module not in sys.modules:
                    __import__(module)
                return getattr(sys.modules[module], klass)
            class PersistentRef:
                def __init__(self, v):
                    oid, module_class = v
                    self.oid_klass = (oid, get_class(module_class))
            unpickler = cPickle.Unpickler(sio)
            unpickler.persistent_load = lambda v: PersistentRef(v)
            class_meta = unpickler.load()
            class_meta, extra = class_meta
            assert extra is None
            object_state = unpickler.load()
            if type(object_state) == dict and  '_container' in object_state:
                assert 'data' not in object_state
                object_state['data'] = object_state['_container']
                del object_state['_container']
            sio.seek(0)
            sio.truncate()
            cPickle.dump(get_class(class_meta), sio, 2)

            pickler = cPickle.Pickler(sio, 2)
            def persistent_id(v):
                if isinstance(v, PersistentRef):
                    return v.oid_klass
                return None
            pickler.persistent_id = persistent_id
            pickler.dump(object_state)
            record = pack_record(oid, sio.getvalue(), ''.join(refs))
            yield record
        print
        print n, 'objects written'
    if os.path.exists(durus_file_name):
        os.unlink(durus_file_name)
    durus_storage = FileStorage(durus_file_name)
    durus_storage._write_transaction(durus_storage.fp,
                                     generate_durus_object_records())
    durus_storage.fp.close()

def convert_main():
    from optparse import OptionParser
    parser = OptionParser()
    parser.set_description(
        "Reads a ZODB Filestorage file and creates a new Durus "
        "FileStorage file containing the same current object records.")
    parser.add_option(
        '--zodb_file', dest="zodb_file", 
        help="The ZODB FileStorage to convert.")
    parser.add_option(
        '--durus_file', dest="durus_file",
        help=("The Durus FileStorage to create. "
              "This file will be overwritten if it already exists, "
              "so be careful."))
    (options, args) = parser.parse_args()
    if (options.zodb_file and options.durus_file and
        options.zodb_file != options.durus_file):
        convert(options.zodb_file, options.durus_file)
    else:
        parser.print_help()

if __name__ == '__main__':
    convert_main()
