BASE_32 = '0123456789bcdefghjkmnpqrstuvwxyz'

class Geohash(object):
    '''
    Simplistic implementation of a hash function that encodes coordinate pairs
    to a BASE32 string.
    Coordinates are interleaved, meaning that the BASE32 encoding can be
    truncated and still give valid coordinates, but with less spatial accuracy.
    Inspired by geohash.org; Ported implementation from https://developer-should-know.tumblr.com/post/87283491372/geohash-encoding-and-decoding-algorithm
    '''

    def divideRangeByValue(value, valRange):
        mid = Geohash.middle(valRange)
        if value >= mid:
            valRange[0] = mid
            return 1
        else:
            valRange[1] = mid
            return 0

    def divideRangeByBit(bit, valRange):
        mid = Geohash.middle(valRange)
        if bit > 0:
            valRange[0] = mid
        else:
            valRange[1] = mid

    def middle(valRange):
        return (valRange[0] + valRange[1]) / 2.0

    def encode(latitude, longitude, precision):
        ''' precision is number of resulting BASE32 characters
        '''
        latRange = [-90.0, 90.0]
        lonRange = [-180.0, 180.0]
        isEven = True
        bit = 0
        base32CharIndex = 0
        geohash = ''

        while len(geohash) < precision:
            if isEven:
                base32CharIndex = (base32CharIndex << 1) | Geohash.divideRangeByValue(longitude, lonRange)
            else:
                base32CharIndex = (base32CharIndex << 1) | Geohash.divideRangeByValue(latitude, latRange)

            isEven = not isEven

            if bit < 4:
                bit += 1
            else:
                geohash += BASE_32[base32CharIndex]
                bit = 0
                base32CharIndex = 0

        return geohash

    def decode(geohash):
        latRange = [-90.0, 90.0]
        lonRange = [-180.0, 180.0]
        isEvenBit = True

        for char in geohash:
            base32CharIndex = BASE_32.index(char)
            for j in [4,3,2,1,0]: # 5 bits per BASE32 character
                if isEvenBit:
                    Geohash.divideRangeByBit((base32CharIndex >> j) & 1, lonRange)
                else:
                    Geohash.divideRangeByBit((base32CharIndex >> j) & 1, latRange)
                isEvenBit = not isEvenBit

        return [Geohash.middle(latRange), Geohash.middle(lonRange)]


def test():
    assert "u4pruydqqvj8" == Geohash.encode(57.64911, 10.40744, 12)
    print(Geohash.decode("u4pruydqqvj8"))
