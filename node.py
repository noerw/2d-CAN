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
        self.neighbours = {
            "left": "",
            "right": "",
            "up": "",
            "down": ""
        }
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

        poss_neigh["left"] = left_neighbour
        poss_neigh["right"] = right_neighbour
        poss_neigh["up"] = up_neighbour
        poss_neigh["down"] = down_neighbour

        return poss_neigh

    def check_direction(self, new_id, own_id):
        new_lon = ""
        new_lat = ""
        own_lon = ""
        own_lat = ""
        direction = ""

        for i in new_id[::2]:
            new_lon += i

        for i in new_id[1::2]:
            new_lat += i

        for i in own_id[::2]:
            own_lon += i

        for i in own_id[1::2]:
            own_lat += i

        if int(own_lon, 2) ^ int(new_lon, 2) < int(own_lat, 2) ^ int(new_lat, 2):
            xor_direction = "lat"
        else:
            xor_direction = "lon"

        if xor_direction == "lat":
            if int(own_lat, 2) > int(new_lat, 2):
                direction = "right"
            else:
                direction = "left"
        else:
            if int(own_lon, 2) > int(new_lon, 2):
                direction = "up"
            else:
                direction = "down"

        return direction

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
                direction = self.check_direction(new_id, self.id)

                possible_neighbours = self.get_possible_neighbours(new_id)

                up_found = ""
                down_found = ""
                right_found = ""
                left_found = ""

                # possibleNeighbours;
                for neigh_direction in self.neighbours:
                    neigh_id = self.neighbours[neigh_direction]
                    if neigh_id != "":
                        for poss_neigh_direction in possible_neighbours:
                            poss_neigh_id = possible_neighbours[poss_neigh_direction]
                            if poss_neigh_direction == "up":
                                if up_found == "":
                                    up_found = neigh_id + "up"
                                elif int(neigh_id[:12], 2) ^ int(poss_neigh_id, 2) < int(up_found[:12], 2) ^ int(
                                        poss_neigh_id, 2):
                                    up_found = neigh_id + "up"
                            if poss_neigh_direction == "down":
                                if down_found == "":
                                    down_found = neigh_id + "down"
                                elif int(neigh_id[:12], 2) ^ int(poss_neigh_id, 2) < int(down_found[:12], 2) ^ int(
                                        poss_neigh_id, 2):
                                    down_found = neigh_id + "down"
                            if poss_neigh_direction == "left":
                                if left_found == "":
                                    left_found = neigh_id + "left"
                                elif int(neigh_id[:12], 2) ^ int(poss_neigh_id, 2) < int(left_found[:12], 2) ^ int(
                                        poss_neigh_id, 2):
                                    left_found = neigh_id + "left"
                            if poss_neigh_direction == "right":
                                if right_found == "":
                                    right_found = neigh_id + "right"
                                elif int(neigh_id[:12], 2) ^ int(poss_neigh_id, 2) < int(right_found[:12], 2) ^ int(
                                        poss_neigh_id, 2):
                                    right_found = neigh_id + "right"

                if direction == "up":
                    if up_found == "":
                        up_found = self.id
                    elif int(self.id, 2) ^ int(possible_neighbours["up"], 2) < int(up_found[:12], 2) ^ int(
                            possible_neighbours["up"], 2):
                        up_found = self.id

                if direction == "down":
                    if down_found == "":
                        down_found = self.id
                    elif int(self.id, 2) ^ int(possible_neighbours["down"], 2) < int(down_found[:12], 2) ^ int(
                            possible_neighbours["down"], 2):
                        down_found = self.id

                if direction == "left":
                    if left_found == "":
                        left_found = self.id
                    elif int(self.id, 2) ^ int(possible_neighbours["left"], 2) < int(left_found[:12], 2) ^ int(
                            possible_neighbours["left"], 2):
                        left_found = self.id

                if direction == "right":
                    if right_found == "":
                        right_found = self.id
                    elif int(self.id, 2) ^ int(possible_neighbours["right"], 2) < int(right_found[:12], 2) ^ int(
                            possible_neighbours["right"], 2):
                        right_found = self.id

                if up_found != "" and up_found != self.id:
                    self.sendto(("localhost", int(up_found[12:17])), "CHECK_NEIGHBOUR" + str(sender[1]) + up_found)
                elif up_found == self.id:
                    self.sendto(sender, "UPDATE_NEIGHBOUR_UP" + self.id)

                if down_found != "" and down_found != self.id:
                    self.sendto(("localhost", int(down_found[12:17])), "CHECK_NEIGHBOUR" + str(sender[1]) + down_found)
                elif down_found == self.id:
                    self.sendto(sender, "UPDATE_NEIGHBOUR_DOWN" + self.id)

                if left_found != "" and left_found != self.id:
                    self.sendto(("localhost", int(left_found[12:17])), "CHECK_NEIGHBOUR" + str(sender[1]) + left_found)
                elif left_found == self.id:
                    self.sendto(sender, "UPDATE_NEIGHBOUR_LEFT" + self.id)

                if right_found != "" and right_found != self.id:
                    self.sendto(("localhost", int(right_found[12:17])), "CHECK_NEIGHBOUR" + str(sender[1]) + right_found)
                elif right_found == self.id:
                    self.sendto(sender, "UPDATE_NEIGHBOUR_RIGHT" + self.id)


            # Update with new neighbour
            elif query.startswith("UPDATE_NEIGHBOUR"):
                direction = query[17:-12].lower()
                calc_direction = self.check_direction(query[-12:], self.id)

                if (calc_direction == "up" and direction == "down") or (
                        calc_direction == "down" and direction == "up") or (
                        calc_direction == "left" and direction == "right") or (
                        calc_direction == "right" and direction == "left"):

                    if self.neighbours[direction] != "":
                        self.sendto(("localhost", int(self.neighbours[direction][-5:])),
                                    "DELETE_ME" + self.id)

                    self.neighbours[direction] = query[-12:] + str(sender[1])
                    print(str(self.neighbours))

                    answer = ""
                    if direction == "left":
                        answer = "right"

                    if direction == "right":
                        answer = "left"

                    if direction == "up":
                        answer = "down"

                    if direction == "down":
                        answer = "up"

                    self.sendto(sender, "CONFIRM_UPDATE %s" % answer + self.id)

            # Get answer from new neighbour
            elif query.startswith("CONFIRM_UPDATE"):
                direction = query[15:-12].lower()

                if self.neighbours[direction] != "":
                    self.sendto(("localhost", int(self.neighbours[direction][-5:])),
                                "DELETE_ME" + self.id)

                self.neighbours[direction] = query[-12:] + str(sender[1])
                # TODO: Send own hash and divide
                print(str(self.neighbours))

            # Check if own node is nearest to new node
            elif query.startswith("CHECK_NEIGHBOUR"):
                new_id = query[20:32]
                direction = query[37:]

                xor = int(self.id, 2) ^ int(new_id, 2)

                possible_neighbours = self.get_possible_neighbours(new_id)

                found = ""
                # possibleNeighbours;
                for neigh_direction in self.neighbours:
                    neigh_id = self.neighbours[neigh_direction]
                    if neigh_id != "":
                        if found == "":
                            found = neigh_id + direction
                        else:
                            if int(neigh_id[:12], 2) ^ int(possible_neighbours[direction], 2) < int(found[:12], 2) ^ int(possible_neighbours[direction], 2):
                                found = neigh_id + direction

                if found == "":
                    found = self.id
                elif int(self.id, 2) ^ int(possible_neighbours[direction], 2) < int(found[:12], 2) ^ int(
                        possible_neighbours[direction], 2):
                    found = self.id

                if found != "" and found != self.id:
                    self.sendto(("localhost", int(found[12:17])), "CHECK_NEIGHBOUR" + query[15:20] + found)
                elif found == self.id:
                    self.sendto(("localhost", int(query[15:20])), "UPDATE_NEIGHBOUR_" + direction.upper() + self.id)

            elif query.startswith("DELETE_ME"):
                for direction in self.neighbours:
                    if self.neighbours[direction] == query[-12:] + str(sender[1]):
                        self.neighbours[direction] = ""
                print(str(self.neighbours))

            elif query.startswith("STATE"):
                print("neighbours: %s" % str(self.neighbours))
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
