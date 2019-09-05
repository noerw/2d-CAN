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
        else:
            print (message)

    def query_others(self, query):
        point = self.key_to_keyspace(query.split()[1])
        address, keyspace = self.neighbours.getNeighbourForPoint(point)

        if address:
            self.sendto(address, query)
        else:
            print ('No neighbour found for %s', point)

    def query(self, query, sender=None):
        respond = partial(self.sendto, sender)

        # {
        #     "JOIN": commands.join,
        #     "STATE":
        # }

        if sender:
            print ("Received \"%s\" from %s." % (query, sender))

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
                respond("SETKEYSPACE %s" % json.dumps({
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

                # also removes old neighbours in splitDirection
                self.neighbours.addNeighbour(sender, senderKeyspace)

            elif query.startswith("STATE"):
                print ("neighbours: %s" % self.neighbours)
                print ("hash: %s" % self.hash)
                print ("keyspace: %s" % self.keyspace)
                print ("port: %s" % self.port)

            elif query.startswith("SETKEYSPACE"):
                data = json.loads(query[12:])
                self.keyspace = Keyspace.unserialize(data['keyspace'])
                neighbours = [(tuple(address), Keyspace.unserialize(keysp)) for address, keysp in data['neighbours']]
                self.neighbours = GridTopology(self.keyspace, neighbours)

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
                    try:
                        answer = "ANSWER %s" % self.hash[key]
                    except KeyError:
                        answer = "Key %s not found!" % key
                    respond(answer)
                else:
                    self.query_others(query)

            elif query.startswith("PUT"):
                _, key, value = query.split()
                point = self.key_to_keyspace(key)

                if point in self.keyspace:
                    self.hash[key] = value
                    print ("Own hash is now %s" % self.hash)
                    respond("ANSWER Successfully PUT { %s: %s }." % (key, value))
                else:
                    self.query_others(query)

            elif query.startswith("ANSWER"):
                print ("ANSWER: %s." % query.lstrip("ANSWER "))

            elif query == '':
                pass

            else:
                print ("Unrecognized query \"%s\"." % query)

        except Exception as err:
            print_exc()
