class Direction(object):
    '''
    enum hack, allowing cardinal arithmetics:
      NORTH == -SOUTH
      LOCAL == NORTH + SOUTH
      NORTHWEST = NORTH | WEST
    '''
    NORTH = 0b01
    WEST  = 0b10
    SOUTH = -0b01
    EAST  = -0b10
    LOCAL = 0
    cardinals = [NORTH, WEST, SOUTH, EAST]

D = Direction
