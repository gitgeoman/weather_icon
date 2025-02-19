from weather.weather_factories import FactoryWeatherICONPolandToday, FactoryWeatherICONPolandForecast, \
    FactoryWeatherOWSUBREGIONToday, FactoryWeatherOWSUBREGIONForecast


def handle_icon_weather_today() -> None:
    handler = FactoryWeatherICONPolandToday()
    handler.process()


def handle_icon_weather_forecast() -> None:
    handler = FactoryWeatherICONPolandForecast()
    handler.process()


def handle_ow_weather_today() -> None:
    handler = FactoryWeatherOWSUBREGIONToday()
    handler.process()


def handle_ow_weather_forecast() -> None:
    handler = FactoryWeatherOWSUBREGIONForecast()
    handler.process()


if __name__ == "__main__":
    handle_ow_weather_today()
