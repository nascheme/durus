#!/usr/bin/env python
"""A client that stress tests a Durus storage server.
"""

from __future__ import division
[division] # for checker
import os
import sys
import time
import random
from optparse import OptionParser
from durus.persistent import Persistent
from durus.storage_server import DEFAULT_PORT, DEFAULT_HOST
from durus.client_storage import ClientStorage
from durus.connection import Connection
from durus.error import ConflictError

if sys.version < "2.6":
    from md5 import new as md5_new
else:
    from hashlib import md5 as md5_new

MAX_OBJECTS = 10000
MAX_DEPTH = 20
MAX_OBJECT_SIZE = 4000

_SLEEP_TIMES = [0, 0, 0, 0, 0.1, 0.2]
def maybe_sleep():
    time.sleep(random.choice(_SLEEP_TIMES))

def randbool():
    return random.random() <= 0.5

class Counter:
    def __init__(self):
        self.value = 0

    def inc(self):
        self.value += 1


class Container(Persistent):
    def __init__(self, sum=0, value=None, children=None):
        self.sum = sum
        if value is None:
            self.value = random.randint(0, 10)
        else:
            self.value = value
        if children is None:
            self.children = []
        else:
            self.children = children
        self.generate_data()

    def get_checksum(self):
        return md5_new(self.data).digest()

    def generate_data(self):
        self.data = os.urandom(random.randint(0, MAX_OBJECT_SIZE))
        self.checksum = self.get_checksum()

    def create_children(self, counter, depth=1):
        for i in range(random.randint(1, 20)):
            if counter.value > MAX_OBJECTS:
                break
            child = Container(self.sum + self.value)
            counter.inc()
            self.children.append(child)
            if depth < MAX_DEPTH:
                child.create_children(counter, depth + 1)

    def verify(self, sum=0, all=False):
        assert self.sum == sum
        assert self.get_checksum() == self.checksum
        if self.children:
            if all:
                for child in self.children:
                    child.verify(sum + self.value)
            else:
                random.choice(self.children).verify(sum + self.value)

# make pickle happy
from durus.test.stress import Container

def init_db(connection):
    sys.stdout.write('creating object graph\n')
    root = connection.get_root()
    obj = Container()
    root['obj'] = obj
    obj.create_children(Counter())

def verify_db(connection, all=False):
    sys.stdout.write('verifying\n')
    root = connection.get_root()
    root['obj'].verify(all=all)

def mutate_db(connection):
    n = random.choice([2**i for i in range(8)])
    sys.stdout.write('mutating %s objects\n' % n)
    for i in range(n):
        depth = random.randint(1, MAX_DEPTH)
        parent = connection.get_root()['obj']
        while True:
            k = random.randint(0, len(parent.children)-1)
            depth -= 1
            if depth > 0 and parent.children[k].children:
                parent = parent.children[k]
            else:
                obj = parent.children[k]
                break
        if randbool():
            # replace object with a new instance
            k = parent.children.index(obj)
            obj = Container(obj.sum, obj.value, obj.children)
            parent.children[k] = obj
            parent._p_note_change()
        else:
            # just mutate it's data
            obj.generate_data()

def main():
    parser = OptionParser()
    parser.set_description('Stress test a Durus Server')
    parser.add_option('--port', dest='port', default=DEFAULT_PORT, type='int',
                      help='Port to listen on. (default=%s)' % DEFAULT_PORT)
    parser.add_option('--host', dest='host', default=DEFAULT_HOST,
                      help='Host to listen on. (default=%s)' % DEFAULT_HOST)
    parser.add_option('--cache_size', dest="cache_size", default=4000,
                      type="int",
                      help="Size of client cache (default=4000)")
    parser.add_option('--max-loops', dest='loops', default=None, type='int',
                      help='Maximum number of loops before exiting.')

    (options, args) = parser.parse_args()
    from durus.logger import logger
    logger.setLevel(5)
    storage = ClientStorage(host=options.host, port=options.port)
    connection = Connection(storage, cache_size=options.cache_size)
    try:
        if 'obj' not in connection.get_root():
            init_db(connection)
            verify_db(connection, all=True)
            connection.commit()
    except ConflictError:
        connection.abort()
    n = options.loops
    while n is None or n > 0:
        if n is not None:
            n -= 1
        try:
            if hasattr(sys, 'gettotalrefcount'):
                sys.stdout.write('refs = %s\n' % sys.gettotalrefcount())
            if randbool():
                connection.abort()
            verify_db(connection)
            mutate_db(connection)
            connection.commit()
            maybe_sleep()
        except ConflictError:
            sys.stdout.write('conflict\n')
            connection.abort()
            maybe_sleep()

if __name__ == '__main__':
    main()
