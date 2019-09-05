# -*- coding: utf-8 -*-

class Direction(object):
    '''
    enum hack, allowing cardinal arithmetics:
      NORTH == -SOUTH
      LOCAL == NORTH + SOUTH
      NORTHWEST = NORTH | WEST
    '''
    NORTH = 0b01
    WEST  = 0b10
    SOUTH = -0b01
    EAST  = -0b10
    LOCAL = 0
    cardinals = [NORTH, WEST, SOUTH, EAST]

D = Direction # shorthand

class GridTopology(object):
    # 2D topology, keeping track of neighbours and their keyspaces

    keyspace = None

    neighbours = {
        D.NORTH: [], # tuples of ((ip, port), keyspace)
        D.WEST:  [],
        D.SOUTH: [],
        D.EAST:  [],
    }

    def __init__(self, keyspace, neighbours=None):
        self.keyspace = keyspace

        # sort list of neighbours into each direction, if provided
        if neighbours:
            for n in neighbours:
                self.addNeighbour(n[0], n[1])

    def addNeighbour(self, address, keyspace):
        direction = self.getDirection(keyspace.midpoint())
        if direction == D.LOCAL:
            print ('can\'t add neighbour; keyspace overlaps (%s)' % keyspace.serialize())
        else:
            print ('adding neighbour %s in dir %s' % (address, direction))
            self.neighbours[direction].append((address, keyspace))

    def getDirection(self, point):
        # compare with self.keyspace
        # if we're out of bounds both on x and y, we move on x first
        # TODO: improve this logic by preferring larger nodes first,
        # as they know more neighbours, reducing hops
        minx, miny = self.keyspace.lower
        maxx, maxy = self.keyspace.upper
        x, y = point

        if x < minx:   return D.WEST
        elif x > maxx: return D.EAST
        elif y < miny: return D.NORTH
        elif y > maxy: return D.SOUTH
        else:          return D.LOCAL # TODO: should we handle this case like that?

    def getNeighbours(self, directions=None):
        if not directions:
            directions = D.cardinals

        return [n for d in directions for n in self.neighbours[d]]

    def getNeighbourForPoint(self, point):
        # find best neighbour in respective direction list
        # TODO: search for optimal neighbour (-> routing basically)
        x, y = point

        direction = self.getDirection(point)
        if direction == D.LOCAL:
            raise Exception('not implemented')

        neighbours = self.getNeighbours([direction])
        # TODO: handle case were we have no neighbours in a direction?

        bestNeigbour = (None, None)
        minDiff = 999999999999
        for address, keyspace in neighbours:
            minx, miny = keyspace.lower
            maxx, maxy = keyspace.upper
            # finding the optimal neighbour on the other axis
            if direction in [D.WEST, D.EAST]:
                diff = abs(y < miny)
            elif direction in [D.NORTH, D.SOUTH]:
                diff = abs(x < minx)

            if diff < minDiff:
                minDiff = diff
                bestNeighbour = (address, keyspace)

        return bestNeighbour

    def __str__(self):
        # print number of neigbours
        return 'N: %i   W: %i   S: %i   E: %i' % (
            len(self.neighbours[D.NORTH]),
            len(self.neighbours[D.WEST]),
            len(self.neighbours[D.SOUTH]),
            len(self.neighbours[D.EAST]),
        )
