# -*- coding: utf-8 -*-

class Direction(object):
    NORTH = 'N'
    WEST  = 'W'
    SOUTH = 'S'
    EAST  = 'E'
    LOCAL = 'L'

# class GridTopology(Direction):
class GridTopology(object):
    # 2D topology, keeping track of neighbours and their keyspaces

    keyspace = None

    neighbours = {
        Direction.NORTH: [], # tuples of ((ip, port), keyspace)
        Direction.WEST:  [],
        Direction.SOUTH: [],
        Direction.EAST:  [],
    }

    def __init__(self, keyspace):
        self.keyspace = keyspace

    def addNeighbour(self, address, keyspace):
        direction = self.getDirection(keyspace.midpoint())
        if direction == Direction.LOCAL:
            raise Exception('invalid direction "%s"' % direction)
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

        if x < minx:   return Direction.WEST
        elif x > maxx: return Direction.EAST
        elif y < miny: return Direction.NORTH
        elif y > maxy: return Direction.SOUTH
        else:          return Direction.LOCAL # TODO: should we handle this case like that?

    def getNeighbours(self, direction=None):
        if direction:
            return self.neighbours[direction]
        else:
            return self.neighbours[Direction.NORTH]
            + self.neighbours[Direction.WEST]
            + self.neighbours[Direction.SOUTH]
            + self.neighbours[Direction.EAST]

    def getNeighbourForPoint(self, point):
        # find best neighbour in respective direction list
        # TODO: search for optimal neighbour (-> routing basically)
        x, y = point

        direction = self.getDirection(point)
        if direction == Direction.LOCAL:
            raise Exception('not implemented')

        neighbours = self.getNeighbours(direction)
        # TODO: handle case were we have no neighbours in a direction?

        bestNeigbour = (None, None)
        minDiff = 999999999999
        for address, keyspace in neighbours:
            minx, miny = keyspace.lower
            maxx, maxy = keyspace.upper
            # finding the optimal neighbour on the other axis
            if direction in [Direction.WEST, Direction.EAST]:
                diff = abs(y < miny)
            elif direction in [Direction.NORTH, Direction.SOUTH]:
                diff = abs(x < minx)

            if diff < minDiff:
                minDiff = diff
                bestNeighbour = (address, keyspace)

        return bestNeighbour

    def __str__(self):
        # print number of neigbours
        return 'N: %i   W: %i   S: %i   E: %i' % (
            len(self.neighbours[Direction.NORTH]),
            len(self.neighbours[Direction.WEST]),
            len(self.neighbours[Direction.SOUTH]),
            len(self.neighbours[Direction.EAST]),
        )
