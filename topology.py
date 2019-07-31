# -*- coding: utf-8 -*-

class Direction(object):
    north = 'n'
    west  = 'w'
    south = 's'
    east  = 'i'
    local = 'l'

# class GridTopology(Direction):
class GridTopology(object):
    # 2D topology, keeping track of neighbours and their keyspaces

    keyspace = None
    north = []
    west  = []
    south = []
    east  = []

    def __init__(self, keyspace):
        self.keyspace = keyspace

    def addNeighbour(self, direction, address, keyspace):
        if   direction == Direction.north: self.north.append((address, keyspace))
        elif direction == Direction.west:  self.west.append((address, keyspace))
        elif direction == Direction.south: self.south.append((address, keyspace))
        elif direction == Direction.east:  self.east.append((address, keyspace))
        else: raise Exception('invalid direction "%s"' % direction)

    def getDirection(self, point):
        # compare with self.keyspace
        # if we're out of bounds both on x and y, we move on x first
        # TODO: improve this logic by preferring larger nodes first,
        # as they know more neighbours, reducing hops
        minx, miny = self.keyspace.lower
        maxx, maxy = self.keyspace.upper
        x, y = point

        if x < minx:   return Direction.west
        elif x > maxx: return Direction.east
        elif y < miny: return Direction.north
        elif y > maxy: return Direction.south
        else:          return Direction.local # TODO: should we handle this case like that?

    def getNeighboursForDirection(self, direction):
        if   direction == Direction.north: return self.north
        elif direction == Direction.west:  return self.west
        elif direction == Direction.south: return self.south
        elif direction == Direction.east:  return self.east
        else: raise Exception('invalid direction "%s"' % direction)

    def getNeighbourForPoint(self, point):
        # find best neighbour in respective direction list
        # TODO: search for optimal neighbour (-> routing basically)
        x, y = point

        direction = self.getDirection(point)
        if direction == Direction.local:
            raise Exception('not implemented')

        neighbours = self.getNeighboursForDirection(direction)
        # TODO: handle case were we have no neighbours in a direction?

        bestNeigbour = None
        minDiff = 999999999999
        for address, keyspace in neighbours:
            minx, miny = keyspace.lower
            maxx, maxy = keyspace.upper
            # finding the optimal neighbour on the other axis
            if direction in [Direction.west, Diretion.east]:
                diff = abs(y < miny)
            elif direction in [Direction.north, Direction.south]:
                diff = abs(x < minx)

            if diff < minDiff:
                minDiff = diff
                bestNeighbour = (address, keyspace)

        return bestNeighbour
