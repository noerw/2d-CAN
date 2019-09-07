# -*- coding: utf-8 -*-

from direction import D
from keyspace import FULL_KEYSPACE_LOWER, FULL_KEYSPACE_UPPER

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
            raise Exception('we shouldnt end up here, point is in own keyspace')

        neighbours = self.getNeighbours([direction])
        if not neighbours:
            # TODO: route in another direction for good luck?
            raise Exception('no route to %s' % point)

        x, y = point
        bestNeighbour = (None, None)
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

    def visualize(self):
        # plot our view of the neighbours keyspaces.

        import matplotlib.pyplot as plt # load dynamically to avoid slow startup
        import matplotlib.patches as patches

        def keyspaceToRect(ks, color='blue'):
            width  = ks.upper[0] - ks.lower[0]
            height = ks.upper[1] - ks.lower[1]
            return patches.Rectangle(ks.lower, width, height,
                edgecolor='black', facecolor=color)

        fig = plt.figure()
        ax = fig.add_subplot(111, aspect='equal')

        for addr, keyspace in self.getNeighbours():
            ax.add_patch(keyspaceToRect(keyspace))
            ax.annotate(str(addr), keyspace.midpoint(), color='w', weight='bold',
                        fontsize=6, ha='center', va='center')

        ax.add_patch(keyspaceToRect(self.keyspace, 'red'))

        minx, miny = FULL_KEYSPACE_LOWER
        maxx, maxy = FULL_KEYSPACE_UPPER
        ax.set_xlim((minx, maxx))
        ax.set_ylim((miny, maxy))

        return plt
