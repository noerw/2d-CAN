BASE_32 = '0123456789bcdefghjkmnpqrstuvwxyz'

class Geohash(object):
    '''
    Simplistic implementation of a hash function that encodes coordinate pairs
    to a BASE32 string.
    Coordinates are interleaved, meaning that the BASE32 encoding can be
    truncated and still give valid coordinates, but with less spatial accuracy.
    Inspired by geohash.org; Ported implementation from https://developer-should-know.tumblr.com/post/87283491372/geohash-encoding-and-decoding-algorithm
    '''

    NUMERIC = 'numeric'
    BASE32 = 'base32'
    BITSTRING = 'bitstring'

    LAT_RANGE = (-90.0, 90.0)
    LON_RANGE = (-180.0, 180.0)

    def divideRangeByValue(value, valRange):
        '''
        Splits `valRange` in half (in place) so that `value` is within that range.
        Returns 1 if the upper half was chosen, 0 for the lower half.
        '''
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

    def intToBitstring(val, precision):
        ''' converts an LSB-first integer to a MSB-first bitstring
        '''
        result = ''
        while val:
            result += str(1 & val)
            val >>= 1

        # if the last bits are all zero, the loop stops too early; append missing zeros
        missing = precision - len(result)
        if missing:
            result += ''.join(['0'] * missing)

        return result

    def intToBase32(val):
        ''' convert an LSB-first integer to base32 encoding
        '''
        result = ''
        bitmask = 0b11111 # (one base32 char encodes 5 bit)
        i = 0
        while val:
            masked = (bitmask & val) >> (5 * i) # mask 5 bits of the hash
            val &= ~bitmask                     # zero out the processed bits (loop termination)
            bitmask <<= 5                       # mask the next 5 bits
            i += 1

            base32Index = 0                     # reverse bits in groups of 5, as they are LSB-first
            for k in range(5):
                bitK = (masked & (1 << k)) >> k
                base32Index |= bitK << (4 - k)

            result += BASE_32[base32Index]      # convert to BASE32

        return result

    def base32ToInt(string):
        ''' convert a base32 string to LSB-first integer
        '''
        bitsDecoded = 0
        result = 0
        for char in string:
            base32CharIndex = BASE_32.index(char)
            for i in [4,3,2,1,0]: # 5 bits per BASE32 character, start with MSB
                bit = (base32CharIndex & (1 << i)) >> i # extract bit
                bit <<= bitsDecoded
                result |= bit
                bitsDecoded += 1
        return result

    def encodeBits(lat, lon, precision):
        '''
        encodes a coordinate pair into `precision` interleaved bits
        returns a LSB-first integer.
        max precision is 128 bits, afterwards we're overflowing
        '''
        lonRange = list(Geohash.LON_RANGE) # we modify the range, so make a copy
        latRange = list(Geohash.LAT_RANGE)
        bitsEncoded = 0
        geohash = 0

        while bitsEncoded < precision:
            if bitsEncoded % 2 == 0: # even bit
                bit = Geohash.divideRangeByValue(lon, lonRange)
            else:
                bit = Geohash.divideRangeByValue(lat, latRange)

            # geohash = (geohash << 1) | bit # MSB first
            geohash |= bit << bitsEncoded  # LSB first
            bitsEncoded += 1

        return geohash

    def decodeBits(geohash, precision=None):
        lonRange = list(Geohash.LON_RANGE) # we modify the range, so make a copy
        latRange = list(Geohash.LAT_RANGE)
        bitsDecoded = 0

        # FIXME problem: we don't know when all bits are processed; we are
        # stopping when no more bits are set, and we processed an even count.
        # precision could actually be higher, when the remaining bits are all 0,
        # but we don't know about that... with base32 we also have that issue (I
        # think?), even if the blocksize (5) is known
        while geohash or bitsDecoded % 2 != 0:
            if precision and bitsDecoded >= precision:
                break

            bit = geohash & 1   # extract bit
            geohash &= ~1       # clear bit
            geohash >>= 1       # go for the next bit

            if bitsDecoded % 2 == 0:
                Geohash.divideRangeByBit(bit, lonRange)
            else:
                Geohash.divideRangeByBit(bit, latRange)

            bitsDecoded += 1

        # TODO: allow to return either point or range
        return [Geohash.middle(latRange), Geohash.middle(lonRange)]

    def decode(geohash, precision = None):
        if type(geohash) == str:
            precision = len(geohash) * 5
            geohash = Geohash.base32ToInt(geohash)

        return Geohash.decodeBits(geohash, precision)

    def encodePoint(lat, lon, precision, output=NUMERIC):
        if output == Geohash.BASE32:
            precision *= 5 # one char encodes 5 bit

        hashNumeric = Geohash.encodeBits(lat, lon, precision)

        if output == Geohash.BASE32:
            return Geohash.intToBase32(hashNumeric)
        if output == Geohash.BITSTRING:
            return Geohash.intToBitstring(hashNumeric, precision)
        else:
            return hashNumeric

    def encodeRange(xRange, yRange, format=NUMERIC):
        # TODO
        pass


def test():
    assert "u4pruydqqvj8" == Geohash.encodePoint(57.64911, 10.40744, 12, Geohash.BASE32)
    print(Geohash.decode("u4pruydqqvj8"))

