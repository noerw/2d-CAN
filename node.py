from __future__ import division
import gevent
from gevent import socket
from functools import partial
from hashlib import md5
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
        self.lon_bin = ""
        self.lat_bin = ""
        for i in self.id[::2]:
            self.lon_bin += i

        for i in self.id[1::2]:
            self.lat_bin += i

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

    def get_possible_neighbours(self, id):
        lon = ""
        lat = ""
        up = ""
        down = ""
        left = ""
        right = ""
        left_neighbour = ""
        right_neighbour = ""
        up_neighbour = ""
        down_neighbour = ""
        poss_neigh = {}

        for i in id[::2]:
            lon += i

        for i in id[1::2]:
            lat += i

        lon_int = int(lon, 2)
        lat_int = int(lat, 2)

        if lon == "111111" or lon == "000000":
            if lon == "111111":
                up = "{0:06b}".format(lon_int - 1)
                down = "000000"

            if lon == "000000":
                down = "{0:06b}".format(lon_int + 1)
                up = "111111"
        else:
            up = "{0:06b}".format(lon_int - 1)
            down = "{0:06b}".format(lon_int + 1)

        if lat == "111111" or lat == "000000":
            if lat == "111111":
                left = "{0:06b}".format(lat_int - 1)
                right = "000000"

            if lat == "000000":
                right = "{0:06b}".format(lat_int + 1)
                left = "111111"
        else:
            left = "{0:06b}".format(lat_int - 1)
            right = "{0:06b}".format(lat_int + 1)

        for i in range(6):
            left_neighbour += lon[i]
            left_neighbour += left[i]

            right_neighbour += lon[i]
            right_neighbour += right[i]

        for i in range(6):
            up_neighbour += lat[i]
            up_neighbour += up[i]

            down_neighbour += lat[i]
            down_neighbour += down[i]

        poss_neigh[left] = left_neighbour
        poss_neigh[right] = right_neighbour
        poss_neigh[up] = up_neighbour
        poss_neigh[down] = down_neighbour

        return poss_neigh

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

        if sender:
            print("Received \"%s\" from %s." % (query, sender))

        try:
            # Receive join request from new node
            if query.startswith("JOIN"):
                new_id = query[5:]
                new_lon = ""
                new_lat = ""
                direction = ""

                for i in new_id[::2]:
                    new_lon += i

                for i in new_id[1::2]:
                    new_lat += i

                if int(self.lon_bin, 2) ^ int(new_lon, 2) < int(self.lat_bin, 2) ^ int(new_lat, 2):
                    xor_direction = "lat"
                else:
                    xor_direction = "lon"

                if xor_direction == "lat":
                    if int(self.lat_bin, 2) > int(new_lat, 2):
                        direction = "right"
                    else:
                        direction = "left"
                else:
                    if int(self.lon_bin, 2) > int(new_lon, 2):
                        direction = "up"
                    else:
                        direction = "down"

                xor = int(self.id, 2) ^ int(new_id, 2)

                possible_neighbours = self.get_possible_neighbours(new_id)

                # possibleNeighbours;
                if self.neighbours.__len__() > 0:
                    for neigh_direction in possible_neighbours:
                        poss_neigh_id = possible_neighbours[neigh_direction]
                        for neighbour in self.neighbours:
                            neigh_id = self.neighbours[neighbour]
                            pass
                else:
                    self.sendto(sender, "SET_NEIGHBOUR %s" % self.id + direction)

            # Ask new node for
            elif query.startswith("NEW_NODE"):
                new_port = query[9:]
                new_port = new_port[new_port.find(',') + 2:sender.find(')')]
                self.sendto(("localhost", new_port), "SEND_ID")

            elif query.startswith("SEND_ID"):
                self.sendto(sender, "JOIN %s" % self.id)

            elif query.startswith("SET_NEIGHBOURS"):
                pass

            elif query.startswith("STATE"):
                print("left: %s" % str(self.left_address))
                print("right: %s" % str(self.right_address))
                print("hash: %s" % self.hash)
                print("port: %s" % self.port)
                print("id: %s" % self.id)

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
