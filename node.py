from sys import argv
import socket


class Node(object):
    def __init__(self, port):
        self.port = port
        pass
        # self.keyspace = keyspace

    def subdivide(self):
        # return the other half as a keyspace tuple (to be used by requester)
        # reduce own keyspace by half on success
        pass

    def query(self, query):
        return "Queried:", query

    def join_network(self, entry_port):
        # send request (tcp? udp?) to passed port - that node will subdivide
        # then i'll receive a keyspace
        # set keyspace on self, return
        print "localhost:%s" % entry_port
        return (0, 0.5)
        # sendto API


if __name__ == '__main__':
    # Ask for a own_port
    # TODO: just directly ask for a port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    own_port = s.getsockname()[1]

    # Init node with own_port
    node = Node(own_port)

    # Get Keyspace
    try:
        # Assume I'm passed a port...
        # try to connect to the passed port, ask to share its space
        entry_port = argv[1]
        keyspace = node.join_network(entry_port)  # ask the port for a keyspace tuple
        node.keyspace = keyspace
    except IndexError:
        node.keyspace = (0, 1)

    print node.keyspace
    # Listen to this own_port
    s.listen(1)

    # Wait for input
    while True:
        query = raw_input("Query: ")
        response = node.query(query)[1]
        print response
        # get 1
        # put 5 1
