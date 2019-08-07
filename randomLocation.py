import random


def create_location():
    longitude = round(random.uniform(-180.0, 180.0), 1)
    latitude = round(random.uniform(-90.0, 90.0), 1)

    return latitude, longitude
