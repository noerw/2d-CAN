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

    @classmethod
    def unserialize(self, keyspace):
        return Keyspace(*keyspace.split("-"))


class Node(object):
    def __init__(self, own_port=None, keyspace=None):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        self.socket.bind(("localhost", own_port or 0))  # 0 Chooses random port
        self.port = self.socket.getsockname()[1]
        self.keyspace = keyspace

    def __str__(self):
        return "node:%s" % self.port

    def join_network(self, entry_port):
        print "%s: JOINing %s" % (self, entry_port)
        self.socket.sendto("JOIN", ("localhost", entry_port))

    def query(self, request):
        query = request[0]
        sender = request[1]
        response = None

        print "Received \"%s\" from %s." % (query, sender)

        if query == "JOIN":
            response = "SETKEYSPACE " + self.subdivide().serialize()
            print "Own keyspace is now %s" % self.keyspace

        elif query.startswith("SETKEYSPACE "):
            self.keyspace = Keyspace.unserialize(query.split(" ")[1])
            print "Received keyspace: %s." % self.keyspace

        elif query.startswith("GET"):
            arg = query.split()[1]
            response = "GET: %s." % arg

        elif query.startswith("PUT"):
            _, key, value = query.split()
            response = "PUT: %s: %s." % (key, value)

        else:
            print "Unrecognized query \"%s\"." % query

        if response:
            self.socket.sendto(response, sender)

    def subdivide(self):
        return self.keyspace.subdivide()


def start_first_node():
    node = Node(own_port=60000, keyspace=Keyspace(0, 1))
    print "Started new DHT with %s" % node
    return node


def start_node(entry_port):
    node = Node()
    node.join_network(entry_port)
    print "Started %s, now joining." % node
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
    print 'Listening on port %s.' % node.port
    return node.socket.recvfrom(1024)  # Buffer size is 1024 bytes


query = gevent.spawn(await_query, node)
request = gevent.spawn(await_request, node)

while True:
    if query.successful():
        print "Sending user input!"
        node.socket.sendto(query.value, ("localhost", entry_port))
        query = gevent.spawn(await_query, node)
        gevent.sleep(0)
    if request.successful():
        print "Greenlet exited successfully with", request.value
        node.query(request.value)
        request = gevent.spawn(await_request, node)
        gevent.sleep(0)
    gevent.sleep(0)
