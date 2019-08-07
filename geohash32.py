class Geohash(object):
    def __init__(self):
        pass

    base_32 = "0123456789bcdefghjkmnpqrstuvwxyz"

    # base_32 = "0123456789abcdefghijklmnopqrstuv"

    def divideRangeByValue(self, value, range123):
        mid = self.middle(range123)
        if value >= mid:
            range123[0] = mid
            return 1
        else:
            range123[1] = mid
            return 0

    def divideRangeByBit(self, bit, range):
        mid = self.middle(range)
        if bit > 0:
            range[0] = mid
        else:
            range[1] = mid

    def middle(self, range):
        return (range[0] + range[1]) / 2

    def encodeBinary(self, lat, lon, prec):
        latRange = [-90.0, 90.0]
        lonRange = [-180.0, 180.0]
        isEven = True
        binaryhash = ""
        while binaryhash.__len__() < prec:
            if isEven:
                res = self.divideRangeByValue(lon, lonRange)

                binaryhash += str(res)
            else:
                res = self.divideRangeByValue(lat, latRange)

                binaryhash += str(res)

            isEven = not isEven

        return binaryhash

    def encodeGeohash(self, latitide, longitide, precision):
        latRange = [-90.0, 90.0]
        lonRange = [-180.0, 180.0]
        isEven = True
        bit = 0
        base32CharIndex = 0
        geohash = ""
        while geohash.__len__() < precision:
            if isEven:
                base32CharIndex = (base32CharIndex << 1) | self.divideRangeByValue(longitide, lonRange)
            else:
                base32CharIndex = (base32CharIndex << 1) | self.divideRangeByValue(latitide, latRange)

            if isEven:
                isEven = False
            else:
                isEven = True
            if bit < 4:
                bit = bit + 1
            else:
                geohash += self.base_32[base32CharIndex]
                bit = 0
                base32CharIndex = 0

        return geohash

    def decodeGeohash(self, geohash):
        latRange = [-90.0, 90.0]
        lonRange = [-180.0, 180.0]
        isEvenBit = True
        index = 0
        for char in geohash:
            base32CharIndex = self.base_32[index]
            jIndex = 4
            for i in range(4):
                if isEvenBit:
                    self.divideRangeByBit((base32CharIndex >> jIndex) & 1, lonRange)
                    isEvenBit = False
                else:
                    self.divideRangeByBit((base32CharIndex >> jIndex) & 1, latRange)
                    isEvenBit = True
                jIndex = jIndex - 1

        return [self.middle(latRange), self.middle(lonRange)]
