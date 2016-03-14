class Keyspace(object):
    def __init__(self, lower, upper):
        self.lower = float(lower)
        self.upper = float(upper)

    def __str__(self):
        return "(%s, %s)" % (self.lower, self.upper)

    def __le__(self, arg):
        return self.arg >= self.lower

    def __gt__(self, arg):
        return self.arg < self.upper

    def __contains__(self, val):
        return self.lower <= val < self.upper

    def subdivide(self):
        upper = self.upper
        midpoint = ((self.upper - self.lower) / 2) + self.lower
        self.upper = midpoint
        return Keyspace(midpoint, upper)

    def serialize(self):
        return "%s-%s" % (self.lower, self.upper)

    @classmethod
    def unserialize(self, keyspace):
        return Keyspace(*keyspace.split("-"))
