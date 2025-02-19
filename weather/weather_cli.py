import sqlalchemy

from sqlalchemy import text
from weather.weather_factories import FactoryWeatherICONPolandToday, FactoryWeatherICONPolandForecast


def handle_icon_weather_today() -> None:
    weather_current = FactoryWeatherICONPolandToday()
    weather_current.process()


def handle_icon_weather_forecast() -> None:
    weather_current = FactoryWeatherICONPolandForecast()
    weather_current.process()


if __name__ == "__main__":
    handle_icon_weather_today()
