from __future__ import division
from gevent import socket
from functools import partial
from keyspace import Keyspace
from hashlib import md5
import commands


class Node(object):
    def __init__(self, own_port=None, keyspace=None):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        self.socket.bind(("localhost", own_port or 0))  # 0 Chooses random port
        self.port = self.socket.getsockname()[1]
        self.keyspace = keyspace
        self.hash = {}
        self.left = None
        self.right = None

    def __str__(self):
        return "node:%s" % self.port

    def join_network(self, entry_port):
        print "Sending JOIN from %s to port %s." % (self, entry_port)
        self.sendto(("localhost", entry_port), "JOIN")

    def hash_key(self, key):
        return md5(key).hexdigest()

    def key_to_keyspace(self, key):
        return int(self.hash_key(key), base=16) / (1 << 128)

    def sendto(self, address, message):
        if address:
            self.socket.sendto(message, address)
        else:
            print message

    def query_others(self, query):
        keyspace = self.key_to_keyspace(query.split()[1])

        if self.left and self.keyspace >= keyspace:
            if self.left:
                self.sendto(self.left, query)
            else:
                print "Left neighbor not found!"
        elif self.keyspace < keyspace:
            if self.right:
                self.sendto(self.right, query)
            else:
                print "Right neighbor not found!"

    def query(self, query, sender=None):
        respond = partial(self.sendto, sender)

        # {
        #     "JOIN": commands.join,
        #     "STATE":
        # }

        if sender:
            print "Received \"%s\" from %s." % (query, sender)

        if query == "JOIN":
            respond("SETKEYSPACE " + self.keyspace.subdivide().serialize())
            self.right = sender
            print "Own keyspace is now %s" % self.keyspace

        elif query.startswith("STATE"):
            print "left: %s" % str(self.left)
            print "right: %s" % str(self.right)
            print "hash: %s" % self.hash
            print "keyspace: %s" % self.keyspace
            print "port: %s" % self.port

        elif query.startswith("SETKEYSPACE"):
            self.left = sender
            self.keyspace = Keyspace.unserialize(query.split(" ")[1])

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
                print "Own hash is now %s" % self.hash
                respond("ANSWER Successfully PUT { %s: %s }." % (key, value))
            else:
                self.query_others(query)

        elif query.startswith("ANSWER"):
            print "ANSWER: %s." % query.lstrip("ANSWER ")

        else:
            print "Unrecognized query \"%s\"." % query
