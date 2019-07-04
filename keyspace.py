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

    def subdivide(self):
        # divide in one dimension, not both!
        # -> find out wich range is bigger
        upper = self.upper
        keyrange = [
            self.upper[0] - self.lower[0],
            self.upper[1] - self.lower[1]
        ]

        largestDim = keyrange.index(sorted(keyrange)[-1])
        if largestDim == 0:
            midpoint = (keyrange[0] / 2.0 + self.lower[0], self.upper[1])
            midpointNeighbour = (midpoint[0], self.lower[1])
        else:
            midpoint = (self.upper[0], keyrange[1] / 2.0 + self.lower[1])
            midpointNeighbour = (self.lower[0], midpoint[1])

        self.upper = midpoint

        return Keyspace(midpointNeighbour, upper)

    def serialize(self):
        return "%s-%s" % (self.lower, self.upper)

    @classmethod
    def unserialize(self, keyspace):
        return Keyspace(*keyspace.split("-"))
