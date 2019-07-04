from __future__ import division
from gevent import socket
from functools import partial
from keyspace import Keyspace
from hashlib import md5
import json
# import commands

class GridTopology(object):
    # 2D topology, keeping track of neighbours and their keyspaces

    # ownKeyspace = Keyspace()
    left   = []
    right  = []
    top    = []
    bottom = []

    def addTo(self, direction, address, keyspace):
        if direction == 'left':
            self.left.append((
                adress,
                keyspace
            ))
        else:
            raise Exception('not implemented')

    def getDirection(self, point):
        # compare with self.keyspace

    def getNeighboursForDirection(self, direction):
        if direction == 'left': return self.left
        else:                   raise Exception('not implemented')

    def getNeighbourForPoint(self, point):
        # TODO: search for optimal neighbour (-> routing basically)
        x, y = point

        direction = self.getDirection(point)
        neighbours = self.getNeighboursForDirection(direction)

        # TODO: find best neighbour in respective directoin list
        raise Exception('not implemented')



class Node(object):
    def __init__(self, own_port=None, keyspace=None):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        self.socket.bind(("localhost", own_port or 0))  # 0 Chooses random port
        self.port = self.socket.getsockname()[1]
        print (self.port)
        self.keyspace = keyspace
        self.hash = {}

        # FIXME: we also need top, bottom neighbours! And they should be a list!
        self.left_address = None
        self.right_address = None
        self.neighbours = GridTopology() # TODO WIP

        self.salt = 'asdndslkf' # TODO: should be proper randomized? must be shared among nodes? 🤔
        self.pepper = 'sdfjsdfoiwefslkf'

    def __str__(self):
        return "node:%s" % self.port

    def address(self):
        print ("I'm in address. self.port = %s" % self.port)
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

    def sendto(self, address, message):
        if address:
            self.socket.sendto(message.encode('utf-8'), address)
        else:
            print (message)

    def query_others(self, query):
        key_in_keyspace = self.key_to_keyspace(query.split()[1])

        if self.left_address and self.keyspace >= key_in_keyspace:
            if self.left_address:
                self.sendto(self.left_address, query)
            else:
                print ("Left neighbor not found!")
        elif self.keyspace < key_in_keyspace:
            if self.right_address:
                self.sendto(self.right_address, query)
            else:
                print ("Right neighbor not found!")

    def query(self, query, sender=None):
        respond = partial(self.sendto, sender)

        # {
        #     "JOIN": commands.join,
        #     "STATE":
        # }

        if sender:
            print ("Received \"%s\" from %s." % (query, sender))

        if query == "JOIN":
            # TODO: join does not forward to other nodes (needs to provide a (random) point)
            # if point within own keyspace:
                # subdivide keyspace
                # send half of keyspace to new node
                # inform affected neighbours
                # update own neighbours
            # else
                # route join request to neighbour closest to the point

            if not self.left_address:
                self.left_address = sender

            # import ipdb; ipdb.set_trace()
            respond("SETKEYSPACE %s" % json.dumps({
                'keyspace': self.keyspace.subdivide().serialize(),
                'right_address': self.right_address or self.address()
            }))

            self.right_address = sender
            print ("Own keyspace is now %s" % self.keyspace)

        elif query.startswith("STATE"):
            print ("left: %s" % str(self.left_address))
            print ("right: %s" % str(self.right_address))
            print ("hash: %s" % self.hash)
            print ("keyspace: %s" % self.keyspace)
            print ("port: %s" % self.port)

        elif query.startswith("SETKEYSPACE"):
            self.left_address = sender
            data = json.loads(query[12:])
            self.keyspace = Keyspace.unserialize(data['keyspace'])
            self.right_address = tuple(data['right_address'])

            self.sendto(self.right_address, "SET_ADDRESS %s" % json.dumps({
                'neighbor': 'left_address',
                'neighbor_address': self.address()
            }))

        elif query.startswith("SET_ADDRESS"):
            data = json.loads(query[12:])
            setattr(self, data['neighbor'], tuple(data['neighbor_address']))

        elif query.startswith("GET"):
            key = query.split()[1]
            keyspace = self.key_to_keyspace(key)

            if keyspace in self.keyspace:
                try:
                    answer = "ANSWER %s" % self.hash[key]
                except KeyError:
                    answer = "Key %s not found!" % key
                respond(answer)
            else:
                self.query_others(query)

        elif query.startswith("PUT"):
            _, key, value = query.split()
            keyspace = self.key_to_keyspace(key)

            if keyspace in self.keyspace:
                self.hash[key] = value
                print ("Own hash is now %s" % self.hash)
                respond("ANSWER Successfully PUT { %s: %s }." % (key, value))
            else:
                self.query_others(query)

        elif query.startswith("ANSWER"):
            print ("ANSWER: %s." % query.lstrip("ANSWER "))

        else:
            print ("Unrecognized query \"%s\"." % query)
