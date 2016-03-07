import gevent
from gevent import socket
import sys


class Keyspace(object):
    def __init__(self, lower, upper):
        self.lower = float(lower)
        self.upper = float(upper)

    def __str__(self):
        return "(%s, %s)" % (self.lower, self.upper)

    def subdivide(self):
        upper = self.upper
        midpoint = ((self.upper - self.lower) / 2) + self.lower
        self.upper = midpoint
        return Keyspace(midpoint, upper)

    def serialize(self):
        return "%s-%s" % (self.lower, self.upper)

    def contains(self, val):
        return val >= self.lower and val <= self.upper

    @classmethod
    def unserialize(self, keyspace):
        return Keyspace(*keyspace.split("-"))


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
        self.socket.sendto("JOIN", ("localhost", entry_port))

    def hash_key(self, key):
        try:
            return float(key) / 10.0
        except ValueError:
            pass

    def query(self, query, sender=None):
        if sender:
            print "Received \"%s\" from %s." % (query, sender)

        if query == "JOIN":
            response = "SETKEYSPACE " + self.keyspace.subdivide().serialize()
            self.socket.sendto(response, sender)
            self.right = sender
            print "Own keyspace is now %s" % self.keyspace

        elif query.startswith("STATE"):
            print "left: %s" % str(self.left)
            print "right: %s" % str(self.right)
            print "hash: %s" % self.hash
            print "keyspace: %s" % self.keyspace
            print "port: %s" % self.port

        elif query.startswith("SETKEYSPACE "):
            self.left = sender
            self.keyspace = Keyspace.unserialize(query.split(" ")[1])
            print "Received keyspace: %s." % self.keyspace

        elif query.startswith("GET"):
            key = query.split()[1]
            hashed = self.hash_key(key)

            if self.keyspace.contains(hashed):
                try:
                    print "Answer: %s" % self.hash[key]
                except KeyError:
                    print "Key %s not found!" % key
            elif self.left and self.keyspace.lower > hashed:
                print "Need to send this to the left"
            elif self.right and self.keyspace.upper < hashed:
                print "Need to send this to the right"
            else:
                print "Neighbor doesn't exist?"

        elif query.startswith("PUT"):
            _, key, value = query.split()
            self.hash[key] = value
            print "Successfully PUT { %s: %s }." % (key, value)

        else:
            print "Unrecognized query \"%s\"." % query


def start_first_node():
    node = Node(own_port=60000, keyspace=Keyspace(0, 1))
    print "Started new DHT with %s" % node
    return node


def start_node(entry_port):
    node = Node()
    print "Started %s." % node
    node.join_network(entry_port)
    return node


try:
    entry_port = int(sys.argv[1])
    node = start_node(entry_port)
except IndexError:
    node = start_first_node()


def await_query(node):
    socket.wait_read(sys.stdin.fileno())
    data = sys.stdin.readline().rstrip("\n")
    return data


def await_request(node):
    return node.socket.recvfrom(1024)  # Buffer size is 1024 bytes


query = gevent.spawn(await_query, node)
request = gevent.spawn(await_request, node)

while True:
    if query.successful():
        node.query(query.value)
        query = gevent.spawn(await_query, node)
        gevent.sleep(0)
    if request.successful():
        node.query(*request.value)
        request = gevent.spawn(await_request, node)
        gevent.sleep(0)
    gevent.sleep(0)
