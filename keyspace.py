import json

from topology import Direction

FULL_KEYSPACE_LOWER = (0, 0)
FULL_KEYSPACE_UPPER = (1, 1)
# FULL_KEYSPACE_LOWER = (-180, -90) # FIXME: see constructor NOTE
# FULL_KEYSPACE_UPPER = (180, 90)

class Keyspace(object):
    def __init__(self, lower=FULL_KEYSPACE_LOWER, upper=FULL_KEYSPACE_UPPER):
        '''
        2D keyspace: (minx,miny)  (maxx,maxy) tuples.
        NOTE: both dimensions should have same size, otherwise alternating
        split direction is not guaranteed!

        TODO: we should be able to translate a keyspace to geohash-ish representation
        '''
        self.lower = lower
        self.upper = upper

    def __str__(self):
        return "(%s, %s)" % (self.lower, self.upper)

    def __contains__(self, val):
        return (
            self.lower[0] <= val[0] < self.upper[0] and
            self.lower[1] <= val[1] < self.upper[1]
        )

    def midpoint(self):
        # returns the middle of the keyspace as (x,y) tuple
        return (
            (self.upper[0] - self.lower[0]) / 2.0 + self.lower[0],
            (self.upper[1] - self.lower[1]) / 2.0 + self.lower[1]
        )

    def largestDimension(self):
        keyrange = [
            self.upper[0] - self.lower[0],
            self.upper[1] - self.lower[1]
        ]
        return keyrange.index(sorted(keyrange)[-1])

    def subdivide(self):
        # splits the keyspace in half, and returns a keyspace of the remaining half.
        splitDirection = Direction.EAST if self.largestDimension() == 0 else Direction.NORTH

        midpoint = self.midpoint()
        if splitDirection == Direction.EAST:
            newUpper = (midpoint[0], self.upper[1])
            newLower = (midpoint[0], self.lower[1])
        else:
            newUpper = (self.upper[0], midpoint[1])
            newLower = (self.lower[0], midpoint[1])

        otherHalf = Keyspace(newLower, self.upper)
        self.upper = newUpper

        return otherHalf, splitDirection

    def serialize(self):
        # return "%s|%s" % (self.lower, self.upper)
        return json.dumps((self.lower, self.upper))

    @classmethod
    def unserialize(self, keyspaceString):
        lower, upper = json.loads(keyspaceString)
        return Keyspace(lower, upper)
