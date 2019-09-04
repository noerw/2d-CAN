from ast import literal_eval as make_tuple # needed for deserializing tuples

from topology import Direction

class Keyspace(object):
    def __init__(self, lower, upper):
        # 2D keyspace: (minx,miny)  (maxx,maxy) tuples
        self.lower = lower
        self.upper = upper

    def __str__(self):
        return "(%s, %s)" % (self.lower, self.upper)

    def __le__(self, arg):
        # args: (x,y)
        return self.arg >= self.lower

    def __gt__(self, arg):
        # args: (x,y)
        return self.arg < self.upper

    def __contains__(self, val):
        print (val, self.lower, self.upper)
        return self.lower <= val < self.upper

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
        splitDirection = Direction.EAST if self.largestDimension() == 0 else Direction.SOUTH

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
        return "%s-%s" % (self.lower, self.upper)

    @classmethod
    def unserialize(self, keyspace):
        # parse string as tuple convert to floats
        lower, upper = keyspace.split('-')
        return Keyspace(make_tuple(lower), make_tuple(upper))
