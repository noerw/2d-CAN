from __future__ import division
from gevent import socket
from functools import partial
from hashlib import md5
import json
from geohash32 import Geohash


class Node(object):
    def __init__(self, own_port=None, id=None, location=None):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        self.socket.bind(("localhost", own_port or 0))  # 0 Chooses random port
        self.port = self.socket.getsockname()[1]
        print(self.port)
        self.id = id
        self.hash = {}
        self.left_address = None
        self.right_address = None
        self.neighbours = {}
        self.same_id = {}
        self.location = location

    def __str__(self):
        return "node:%s and id: %s" % (self.port, self.id)

    def address(self):
        print("I'm in address. self.port = %s" % self.port)
        return '127.0.0.1', self.port, self.id

    def calculate_distance(self, key):
        distance = self.id ^ key
        print(distance)

    def join_network(self, entry_port):
        print("Sending JOIN from %s to port %s." % (self, entry_port))
        self.sendto(("localhost", entry_port), "JOIN %s" % self.id)

    def hash_key(self, key):
        return md5(key.encode('utf-8')).hexdigest()

    def refine_id(self):
        geohash32 = Geohash()
        done = False
        to_delete = []

        if self.same_id.__len__() > 0:
            if self.same_id.__len__() == 1:
                for port in self.same_id:
                    same_id = self.same_id[port]
                    if len(same_id) == len(self.id):
                        print("Splitted ID, new ID is now %s" % self.id)
                        self.sendto(("localhost", port), "SPLIT %s" % self.id)
                        self.same_id = {}
                        done = True
                        break

            if not done:
                self.id = geohash32.encodeBinary(self.location[0], self.location[1], len(self.id) + 2)
                print("Refined ID, new ID is now %s" % self.id)

        for port in self.same_id:
            same_id = self.same_id[port]
            if len(same_id) == len(self.id) and same_id == self.id:
                self.same_id = {}
                self.id = geohash32.encodeBinary(self.location[0], self.location[1], len(self.id) + 2)
                print("Splitted ID, new ID is now %s" % self.id)
                self.sendto(("localhost", port), "SPLIT %s" % self.id)
                done = True
                break

            elif len(same_id) >= len(self.id) and not same_id[:len(self.id)] == self.same_id:
                to_delete.append(port)

        for port in to_delete:
            del self.same_id[port]

        if not done and self.same_id.__len__() > 0:
            self.refine_id()

    def split(self):
        pass

    def sendto(self, address, message):
        if address:
            self.socket.sendto(message.encode('utf-8'), address)
        else:
            print(message)

    def query_others(self, query):
        key_in_keyspace = self.key_to_keyspace(query.split()[1])

        if self.left_address and self.keyspace >= key_in_keyspace:
            if self.left_address:
                self.sendto(self.left_address, query)
            else:
                print("Left neighbor not found!")
        elif self.keyspace < key_in_keyspace:
            if self.right_address:
                self.sendto(self.right_address, query)
            else:
                print("Right neighbor not found!")

    def query(self, query, sender=None):
        respond = partial(self.sendto, sender)

        geohash32 = Geohash()

        if sender:
            print("Received \"%s\" from %s." % (query, sender))

        try:
            # Receive join request from new node
            if query.startswith("JOIN"):
                new_id = query[5:]
                split = False
                # Check if own id is = new id => Need for split
                if len(self.id) == len(new_id) and self.id == new_id:
                    self.id = geohash32.encodeBinary(self.location[0], self.location[1], len(self.id) + 2)
                    print("Splitted ID, new ID is now %s" % self.id)
                    self.sendto(sender, "SPLIT %s" % self.id)
                    split = True

                # Check through neighbours if id needs to be refined
                else:
                    for port in self.neighbours:
                        id = self.neighbours[port]
                        if len(id) >= len(new_id) and id[:len(new_id)] == new_id:
                            self.sendto(("localhost", port), "NEW_NODE %s" % sender)

                # Check if own prefix is = new id
                if new_id == self.id[:len(new_id)] and not split:
                    print("SEND REFINE")
                    self.sendto(sender, "REFINE %s" % self.id)

            # Ask new node for
            elif query.startswith("NEW_NODE"):
                new_port = query[9:]
                new_port = new_port[new_port.find(',') + 2:sender.find(')')]
                self.sendto(("localhost", new_port), "SEND_ID")

            elif query.startswith("SEND_ID"):
                self.sendto(sender, "JOIN %s" % self.id)

            elif query.startswith("REFINE"):
                node_port = sender[1]
                node_id = query[query.find(' ') + 1:]
                self.same_id[node_port] = node_id
                self.refine_id()

            elif query.startswith("SPLIT"):
                node_id = query[6:]
                self.id = geohash32.encodeBinary(self.location[0], self.location[1], len(self.id) + 2)
                print("Splitted ID, new ID is now %s" % self.id)

                if node_id == self.id:
                    self.id = geohash32.encodeBinary(self.location[0], self.location[1], len(self.id) + 2)
                    self.sendto(sender, "SPLIT %s" % self.id)

            elif query.startswith("SET_NEIGHBOURS"):
                # Set neighbours
                # Inform neighbours about existance
                pass

            elif query.startswith("STATE"):
                print("left: %s" % str(self.left_address))
                print("right: %s" % str(self.right_address))
                print("hash: %s" % self.hash)
                print("port: %s" % self.port)

            elif query.startswith("SET_NEW_ID"):
                # Set a new id when a new node joins with same id
                pass

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
                    print("Own hash is now %s" % self.hash)
                    respond("ANSWER Successfully PUT { %s: %s }." % (key, value))
                else:
                    self.query_others(query)

            elif query.startswith("ANSWER"):
                print("ANSWER: %s." % query.lstrip("ANSWER "))

            else:
                print("Unrecognized query \"%s\"." % query)

        except Exception as err:
            print('ERROR -- could not parse query: %s' % err)
