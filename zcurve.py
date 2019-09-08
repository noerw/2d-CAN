from direction import D

class ZCurve(object):
    '''
    Represents a position on a Z-Order Curve in 2D                    0 - 1
    https://en.wikipedia.org/wiki/Z-order_curve                         /
    https://en.wikipedia.org/wiki/Moser%E2%80%93de_Bruijn_sequence    2 - 3
    '''

    EVENBITS = 0xaaaaaaaa # 0b10101010101010101010101010101010 (32 bit)
    ODDBITS  = 0x55555555 # 0b01010101010101010101010101010101 (32 bit)

    z = 0
    depth = 0

    def __init__(self, z=0, depth=1):
        '''
        `z`      is the position on the z-order curve.
        `depth`  is the recursion depth of the curve.
            A ZCurve can have `4**depth` elements
        '''
        if z > 4 ** depth - 1:
            raise ValueError('z-value %i does not exist on depth level %i' % (z, depth))

        self.z = z
        self.depth = depth

    def fromXY(xy, depth):
        '''
        Constructs a ZCurve instance from a x,y pair, where x,y are indices to
        the moser-debruijn sequence, so z = debruijn[x] + 2*debruijn[y]
        '''
        x, y = xy
        if x >= 2 ** depth or y >= 2 ** depth:
            raise ValueError('coordinate %s does not exist on depth level %i' % (xy, depth))

        # interleave bits https://graphics.stanford.edu/~seander/bithacks.html#InterleaveTableObvious
        z = 0
        for i in range(32):
            z |= (x & 1 << i) << i | (y & 1 << i) << (i + 1)

        return ZCurve(z, depth)

    def fromBitstring(bitstring):
        if len(bitstring) % 2 != 0:
            # TODO: we should be able to deal with "halfsplits"?
            raise ValueError('len(bitstring) must be multiple of 2!')
        depth = int(len(bitstring) / 2)
        z = int(bitstring, base=2)
        return ZCurve(z, depth)

    def xy(self):
        ''' returns indices to the debruijn sequence
        '''
        x = 0
        y = 0
        # de-interleave bits
        for i in range(32):
            if i % 2 == 0:
                x |= (self.z & 1 << i) >> int(i / 2)
            else:
                y |= (self.z & 1 << i) >> int(i / 2 + 1)
        return x, y

    def debruijn(self):
        ''' given indices to the moser-debuijn sequence, returns debruijn values
        '''
        xDebruijn = self.z & self.ODDBITS
        yDebruijn = self.z & self.EVENBITS
        return xDebruijn, yDebruijn

    def neighbours(self):
        # FIXME: torus-style wrap-around gives incorrect results for
        #   `self.depth != int.bit_length(self.z | self.BITMASK_EVEN)` !

        # https://en.wikipedia.org/wiki/Z-order_curve#Coordinate_values adapted to 32bit
        return {
            D.NORTH: ZCurve(((self.z & self.EVENBITS) - 1 & self.EVENBITS) | (self.z & self.ODDBITS), self.depth),
            D.SOUTH: ZCurve(((self.z | self.ODDBITS)  + 1 & self.EVENBITS) | (self.z & self.ODDBITS), self.depth),
            D.WEST:  ZCurve(((self.z & self.ODDBITS)  - 1 & self.ODDBITS) | (self.z & self.EVENBITS), self.depth),
            D.EAST:  ZCurve(((self.z | self.EVENBITS) + 1 & self.ODDBITS) | (self.z & self.EVENBITS), self.depth),
        }

    def parent(self, depthOffset=1):
        ''' returns the corresponding cell in the z-order curve of one less recursion
        '''
        z = self.z >> (2 * depthOffset)
        depth = max(0, self.depth - depthOffset)
        return ZCurve(z, depth)

    def children(self, depthOffset=1):
        ''' returns the corresponding cells in the z-order curve of deeper recurson
        '''
        zBase = self.z << (2 * depthOffset)
        numChildren = 4 ** depthOffset
        return [ZCurve(z, self.depth + depthOffset) for z in range(zBase, zBase + numChildren)]

    def __str__(self):
        # encode as bitstring
        res = ''
        for i in range(self.depth * 2):
            bit = (self.z & (1 << i)) >> i
            res += str(bit)
        return res[::-1] # reverse, MSB first

    def __add__(self, other):
        self.ensureSameType(other)

        if other.depth != self.depth:
            deeper = other if self.depth < other.depth else self
            higher = self  if self.depth < other.depth else other
            return deeper + higher.children(deeper.depth - higher.depth)[0]

        # FIXME: torus-style wrap-around fails
        # https://en.wikipedia.org/wiki/Z-order_curve#Coordinate_values adapted to 32bit
        z = (
            ((self.z | self.EVENBITS) + (other.z & self.ODDBITS) & self.ODDBITS) |
            ((self.z | self.ODDBITS) + (other.z & self.EVENBITS) & self.EVENBITS)
        )
        return ZCurve(z, self.depth)

    def __contains__(self, other):
        self.ensureSameType(other)
        if self.depth > other.depth:
            return False
        elif self.depth < other.depth:
            return self == other.parent(other.depth - self.depth)
        else:
            return self == other

    def __lt__(self, other):
        # compares the "area" that this instance covers
        self.ensureSameType(other)
        return self.depth > other.depth

    def __gt__(self, other):
        self.ensureSameType(other)
        return self.depth < other.depth

    def __le__(self, other):
        self.ensureSameType(other)
        return self.depth <= other.depth

    def __ge__(self, other):
        self.ensureSameType(other)
        return self.depth >= other.depth

    def __eq__(self, other):
        self.ensureSameType(other)
        return self.z == other.z and self.depth == other.depth

    def ensureSameType(self, other):
        if type(other) != ZCurve:
            raise TypeError('cant compare ZCurve with %s', type(other))
