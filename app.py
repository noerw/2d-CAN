from node import Node
from keyspace import Keyspace
import gevent
from gevent import socket
from sys import stdin, argv


def start_first_node():
    node = Node(own_port=60000, keyspace=Keyspace())
    print ("Started new DHT with %s" % node)
    return node


def start_node(entry_port):
    node = Node()
    print ("Started %s." % node)
    node.join_network(entry_port) # FIXME: should exit if we don't get a response
    return node


try:
    entry_port = int(argv[1])
    node = start_node(entry_port)
except IndexError:
    node = start_first_node()


def await_query(node):
    socket.wait_read(stdin.fileno())
    data = stdin.readline().rstrip("\n")
    return data


def await_request(node):
    return node.socket.recvfrom(1024)  # Buffer size is 1024 bytes


query = gevent.spawn(await_query, node)
request = gevent.spawn(await_request, node)

try:
    while True:
        if query.successful():
            node.query(query.value)
            query = gevent.spawn(await_query, node)
            gevent.sleep(0)
        if request.successful():
            queryType, sender = request.value
            node.query(queryType.decode('utf-8'), sender)
            request = gevent.spawn(await_request, node)
            gevent.sleep(0)
        gevent.sleep(0.05) # to reduce cpu usage
except KeyboardInterrupt:
    # TODO: handle network leave
    exit(0)
