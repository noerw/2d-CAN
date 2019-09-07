from direction import D

class ZCurve(object):
    '''
    Operations on a Z-Order Curve in 2D
    https://en.wikipedia.org/wiki/Z-order_curve
    https://en.wikipedia.org/wiki/Moser%E2%80%93de_Bruijn_sequence

    TODO: This encodes a single curve. When integrating with geohash / topology,
      we'll need a representation considering the recursive nature of the curve.
      For this, we need to know the depth, which is unreliable here, as z-values
      can have a leading 0. Encoding as bitstring would help?

    TODO: Make this objectoriented with a state (value & depth?), where parent()
      etc return a new ZCurve(z) ?
    '''

    BITMASK_EVEN = 0xaaaaaaaa # 0b10101010101010101010101010101010 (32 bit)
    BITMASK_ODD  = 0x55555555 # 0b01010101010101010101010101010101 (32 bit)

    def z(x, y):
        ''' returns debruijn[x] + 2*debruijn[y], where x, y are indices to the moser-debruijn sequence
        '''
        # interleave bits https://graphics.stanford.edu/~seander/bithacks.html#InterleaveTableObvious
        z = 0
        for i in range(32):
            z |= (x & 1 << i) << i | (y & 1 << i) << (i + 1)
        return z

    def xy(z):
        ''' returns indices to the debruijn sequence
        '''
        x = 0
        y = 0
        # de-interleave bits
        for i in range(32):
            if i % 2 == 0:
                x |= (z & 1 << i) >> int(i / 2)
            else:
                y |= (z & 1 << i) >> int(i / 2 + 1)
        return x, y

    def debruijn(x, y, z=None):
        ''' given indices to the moser-debuijn sequence, returns debruijn values
        '''
        if z is None:
            z = ZCurve.z(x, y)
        xDebruijn = z & BITMASK_ODD
        yDebruijn = z & BITMASK_EVEN
        return xDebruijn, yDebruijn

    def neighbours(z):
        # https://en.wikipedia.org/wiki/Z-order_curve#Coordinate_values adapted to 32bit
        return {
            D.NORTH: ((z & BITMASK_EVEN) - 1 & BITMASK_EVEN) | (z & BITMASK_ODD),
            D.SOUTH: ((z | BITMASK_ODD)  + 1 & BITMASK_EVEN) | (z & BITMASK_ODD),
            D.WEST:  ((z & BITMASK_ODD)  - 1 & BITMASK_ODD) | (z & BITMASK_EVEN),
            D.EAST:  ((z | BITMASK_EVEN) + 1 & BITMASK_ODD) | (z & BITMASK_EVEN),
        }

    def zPlusVec(z, dX=0, dY=0, w=None):
        ''' adds two z values in 2D
        '''
        if w is None:
            w = ZCurve.z(dX, dY)
        # https://en.wikipedia.org/wiki/Z-order_curve#Coordinate_values adapted to 32bit
        # faster than doing ZCurve.z(ZCurve.xy(z) + (dX,dY))
        return (
            ((z | BITMASK_EVEN) + (w & BITMASK_ODD) & BITMASK_ODD) |
            ((z | BITMASK_ODD) + (w & BITMASK_EVEN) & BITMASK_EVEN)
        )

    def parent(z):
        ''' returns the corresponding cell in the z-order curve of one level recursion less
        '''
        return z >> 2

    def children(z):
        ''' returns the corresponding cells in the z-order curve of one level recursion more
        '''
        z <<= 2
        return [z, z + 1, z + 2, z + 3]
