# -*- coding: utf-8 -*-

from __future__ import division
from gevent import socket
from functools import partial
from keyspace import Keyspace
from hashlib import md5
import json
from traceback import print_exc

from topology import GridTopology, Direction
from geohash import Geohash

class Node(object):
    def __init__(self, own_port=None, keyspace=None):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        self.socket.bind(("localhost", own_port or 0))  # 0 Chooses random port
        self.port = self.socket.getsockname()[1]
        self.keyspace = keyspace
        self.hash = {}
        self.neighbours = GridTopology(keyspace)

        # keeps track of queries that were routed to us but that we passed on,
        # so that we can pass the answer to the original queryee
        self.queries = {} # key: query key, value: address of query origin

        self.salt = 'asdndslkf' # TODO: should be proper randomized? must be shared among nodes? 🤔
        self.pepper = 'sdfjsdfoiwefslkf'

    def __str__(self):
        return "node:%s" % self.port

    def address(self):
        return ('127.0.0.1', self.port)

    def join_network(self, entry_port):
        print ("Sending JOIN from %s to port %s." % (self, entry_port))
        self.sendto(("localhost", entry_port), "JOIN")

    def hash_key(self, key):
        return md5(key.encode('utf-8')).hexdigest()

    def key_to_keyspace(self, key):
        ''' returns a point within the keyspace corresponding to the given key
        '''
        # keyspace is 2D -> split it
        hashX = self.hash_key(key + self.salt)
        hashY = self.hash_key(key + self.pepper)
        return (
            int(hashX, base=16) / (1 << 128), # convert to keyspace [0,1]
            int(hashY, base=16) / (1 << 128)
        )

    def coord_to_keyspace(self, point):
        '''
        context: Geohash

        hmmm.. what do we want to achieve here?
        - translate between geographic and keyspace coords?
        - translate between hashes and locations?
        - address content by location?

        we should be able to translate between keyspace and a geohash:
        each geohash bitpair divides the space just as we divide our keyspace
        -> longer hash -> smaller keyspace.
            problem: geohash doesnt encode edges but a single point..
        -> keyspace is ID of a node -> can be encoded via interleaved coords, if total size of keyspace is given
            -> neighbour topology can be expressed as DHT? but then we basically have a 2D kademlia?
        -> content IDs (hash of key) can be assigned to node
        '''

        x, y = point
        # TODO

    def sendto(self, address, message):
        if address:
            self.socket.sendto(message.encode('utf-8'), address)
        else: # this was a local query
            print (message)

    def query_others(self, query, origin=None):
        # query is a GET or PUT query, so second element is always a data key
        key = query.split()[1]
        point = self.key_to_keyspace(key)
        address, keyspace = self.neighbours.getNeighbourForPoint(point)
        print (address, keyspace)

        if address:
            self.sendto(address, query)
            if origin:
                # FIXME: very simplistic should be a list
                # -> fails when multiple queries for a key are in flight
                self.queries[key] = origin
        else:
            print ('No neighbour found for %s', point)

    def query(self, query, sender=None):
        '''
        query handler. hacky protocol mixed of magic strings and JSON
        queries can be coming from

        '''
        respond = partial(self.sendto, sender)

        if sender:
            print ("Received \"%s\" from %s." % (query, sender))

        if not query:
            return

        try:
            if query == "JOIN":
                senderKeyspace, splitDirection = self.keyspace.subdivide()
                print ("Own keyspace is now %s" % self.keyspace)

                # pass ourselves and all our neighbours to the new node, except for the opposite splitDirection
                neighbours = self.neighbours.getNeighbours([
                    d for d in Direction.cardinals if d != -splitDirection
                ])
                neighbours = [(addr, keysp.serialize()) for addr, keysp in neighbours]
                neighbours.append((self.address(), self.keyspace.serialize()))

                # find content from hashtable that isn't ours anymore
                content = {}
                for k, v in list(self.hash.items()):
                    if not self.key_to_keyspace(k) in self.keyspace:
                        content[k] = v
                        del self.hash[k]

                respond("SETKEYSPACE %s" % json.dumps({
                    'content': content,
                    'keyspace': senderKeyspace.serialize(),
                    'neighbours': neighbours,
                }))

                # notify our neighbours of our changed keyspace
                neighbours = self.neighbours.getNeighbours([
                    d for d in Direction.cardinals if d != splitDirection
                ])
                for addr, keysp in neighbours:
                    self.sendto(addr, 'UPDATE_NEIGHBOURS %s' % json.dumps([
                        (self.address(), self.keyspace.serialize())
                    ]))

                self.neighbours.clearNeighbours([splitDirection]) # get rid of non-adjacent neighbours
                self.neighbours.addNeighbour(sender, senderKeyspace)

            elif query.startswith("STATE"):
                print ("port: %s" % self.port)
                print ("keyspace: %s" % self.keyspace)
                print ("neighbours: %s" % self.neighbours)
                print ("hashtable: %s" % self.hash)
                print ("query routing: %s" % self.queries)

            elif query.startswith("SETKEYSPACE"):
                data = json.loads(query.lstrip("SETKEYSPACE "))
                self.keyspace = Keyspace.unserialize(data['keyspace'])
                neighbours = [(tuple(address), Keyspace.unserialize(keysp)) for address, keysp in data['neighbours']]
                self.neighbours = GridTopology(self.keyspace, neighbours)
                self.hash = data['content']

                # notify the passed neighbours about the new state
                for n in neighbours:
                    addr, keysp = n
                    if addr == sender: continue

                    self.sendto(addr, 'UPDATE_NEIGHBOURS %s' % json.dumps([
                        (self.address(), self.keyspace.serialize()),
                    ]))

            elif query.startswith("UPDATE_NEIGHBOURS"): # replaces SET_ADDRESS
                neighbours = [(tuple(addr), Keyspace.unserialize(keysp)) for addr, keysp in json.loads(query[18:])]
                for n in neighbours:
                    self.neighbours.addNeighbour(n[0], n[1])

            elif query.startswith("GET"):
                key = query.split()[1]
                point = self.key_to_keyspace(key)

                if point in self.keyspace:
                    respond('ANSWER %s' % json.dumps({
                        'key': key,
                        'value': self.hash.get(key, None),
                    }))

                else:
                    self.query_others(query, sender)

            elif query.startswith("PUT"):
                _, key, value = query.split()
                point = self.key_to_keyspace(key)

                if point in self.keyspace:
                    self.hash[key] = value
                    respond('ANSWER %s' % json.dumps({
                        'key': key,
                        'value': value,
                    }))
                    print ('Own hashtable is now %s' % self.hash)
                else:
                    self.query_others(query, sender)

            elif query.startswith("ANSWER"):
                data = json.loads(query.lstrip('ANSWER '))
                key = data['key']

                if key in self.queries:
                    # this answer is a reply to a query where we just act as router
                    dest = self.queries[key]
                    del self.queries[key]
                    print ('Routing answer to %s:%s' % dest)
                    self.sendto(dest, query)
                else:
                    # this response is for our query.
                    self.sendto(None, query)

            else:
                print ("Unrecognized query \"%s\"." % query)

        except Exception as err:
            print_exc()
