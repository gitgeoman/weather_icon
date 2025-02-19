from enum import Enum

class Area(Enum):
    POLAND = {
        "lat_min": 49.00,
        "lat_max": 54.84,
        "lon_min": 14.07,
        "lon_max": 24.15,
    }
    UKRAINE = {
        "lat_min": 44.38,
        "lat_max": 52.38,
        "lon_min": 22.14,
        "lon_max": 40.22,
    }
    GERMANY = {
        "lat_min": 47.27,
        "lat_max": 55.03,
        "lon_min": 5.87,
        "lon_max": 15.04,
    }
    RUSSIA = {
        "lat_min": 41.19,
        "lat_max": 81.86,
        "lon_min": 19.64,
        "lon_max": 180.00,
    }
    EUROPE = {
        "lat_min": 35.00,
        "lat_max": 71.00,
        "lon_min": -10.00,
        "lon_max": 40.00,
    }
    def get_bounds(self):
        return self.value

