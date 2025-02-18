import sqlalchemy

from sqlalchemy import text
from weather.weather_factories import FactoryWeatherICONToday


def handle_icon_weather_current() -> None:
    weather_current = FactoryWeatherICONToday()
    weather_current.process()


if __name__ == "__main__":
    handle_icon_weather_current()