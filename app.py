from node import Node
import gevent
from gevent import socket
from sys import stdin, argv
import randomLocation
from geohash32 import Geohash


def start_first_node():
    geohash32 = Geohash()
    location = randomLocation.create_location()
    geohash = geohash32.encodeGeohash(location[0], location[1], 12)
    geohashbin = geohash32.encodeBinary(location[0], location[1], 2)
    node = Node(own_port=60000, id=geohashbin, location=location)
    print("Started new DHT with %s" % node)
    return node


def start_node(entry_port):
    geohash32 = Geohash()
    location = randomLocation.create_location()
    geohashbin = geohash32.encodeBinary(location[0], location[1], 2)
    node = Node(id=geohashbin, location=location)
    print("Started %s." % node)
    node.join_network(entry_port)
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
    gevent.sleep(0)
