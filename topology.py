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
        midpoint = keyspace.midpoint()
        direction = self.getDirection(midpoint)
        if direction == D.LOCAL:
            print ('can\'t add neighbour; keyspace overlaps (%s)' % keyspace.serialize())
        else:
            ns = self.neighbours[direction]

            # check overlap with existing neighbours. if overlapping, remove old neighbour.
            for i, (addr, keysp) in enumerate(ns):
                if midpoint in keysp:
                    print ('dropping old neighbour %s at %s' % (addr, keysp))
                    del ns[i]

            # TODO: for robustness, we should check that neighbours are actually adjacent to our keyspace?
            print ('adding neighbour %s in dir %s' % (address, direction))
            ns.append((address, keyspace))

    def getDirection(self, point):
        # compare with self.keyspace
        # if we're out of bounds both on x and y, we move on x first
        # TODO: improve this logic by preferring larger nodes first, as they
        # know more neighbours, reducing hops. also check neighbours keyspace,
        # maybe they own the point!
        minx, miny = self.keyspace.lower
        maxx, maxy = self.keyspace.upper
        x, y = point

        if x < minx:   return D.WEST
        elif x > maxx: return D.EAST
        elif y < miny: return D.SOUTH
        elif y > maxy: return D.NORTH
        else:          return D.LOCAL # TODO: should we handle this case like that?

    def getNeighbours(self, directions=None):
        if not directions:
            directions = D.cardinals
        return [n for d in directions for n in self.neighbours[d]]

    def clearNeighbours(self, directions=None):
        if not directions:
            directions = D.cardinals
        for d in directions:
            self.neighbours[d] = []

    def getNeighbourForPoint(self, point):
        # find best neighbour in respective direction list

        direction = self.getDirection(point)
        if direction == D.LOCAL:
            raise Exception('not implemented')

        neighbours = self.getNeighbours([direction])
        # TODO: handle case were we have no neighbours in a direction?

        x, y = point
        bestNeigbour = (None, None)
        minDiff = 999999999999
        for address, keyspace in neighbours:
            minx, miny = keyspace.lower
            maxx, maxy = keyspace.upper
            # finding the optimal neighbour on the other axis
            if direction in [D.WEST, D.EAST]:
                diff = abs(y - miny)
            elif direction in [D.NORTH, D.SOUTH]:
                diff = abs(x - minx)

            if diff < minDiff:
                minDiff = diff
                bestNeighbour = (address, keyspace)

        return bestNeighbour

    def __str__(self):
        # print ports of neigbours
        return 'N: %s   W: %s   S: %s   E: %s' % (
            [n[0][1] for n in self.neighbours[D.NORTH]],
            [n[0][1] for n in self.neighbours[D.WEST]],
            [n[0][1] for n in self.neighbours[D.SOUTH]],
            [n[0][1] for n in self.neighbours[D.EAST]],
        )
