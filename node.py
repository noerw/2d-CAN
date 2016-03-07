from gevent import socket
from functools import partial
from keyspace import Keyspace


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
        try:
            return float(key) / 10.0
        except ValueError:
            pass

    def sendto(self, address, message):
        if address:
            self.socket.sendto(message, address)
        else:
            print message

    def query_others(self, query):
        hashed = self.hash_key(query.split()[1])

        if self.left and self.keyspace.lower > hashed:
            if self.right:
                self.sendto(self.left, query)
            else:
                print "Right neighbor not found!"
        elif self.keyspace.upper < hashed:
            if self.right:
                self.sendto(self.right, query)
            else:
                print "Right neighbor not found!"

    def query(self, query, sender=None):
        respond = partial(self.sendto, sender)

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
            hashed = self.hash_key(key)

            if self.keyspace.contains(hashed):
                try:
                    answer = "ANSWER %s" % self.hash[key]
                except KeyError:
                    answer = "Key %s not found!" % key
                respond(answer)
            else:
                self.query_others(query)

        elif query.startswith("PUT"):
            _, key, value = query.split()
            hashed = self.hash_key(key)

            if self.keyspace.contains(hashed):
                self.hash[key] = value
                print "Own hash is now %s" % self.hash
                respond("ANSWER Successfully PUT { %s: %s }." % (key, value))
            else:
                self.query_others(query)

        elif query.startswith("ANSWER"):
            print "ANSWER: %s." % query.lstrip("ANSWER ")

        else:
            print "Unrecognized query \"%s\"." % query
