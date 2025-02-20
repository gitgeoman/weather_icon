import unittest
from unittest.mock import patch
from weather.weather_factories import (
    FactoryWeatherICONPolandToday,
    FactoryWeatherICONPolandForecast,
    FactoryWeatherOWSUBREGIONToday,
    FactoryWeatherOWSUBREGIONForecast,
)


class TestWeatherFactories(unittest.TestCase):

    def test_factory_weather_icon_poland_today(self):
        factory = FactoryWeatherICONPolandToday()
        config = factory.get_config()
        self.assertIn('AREA', config)
        self.assertIn('TABLE_NAME', config)
        self.assertIn('FORECAST_HOURS', config)
        self.assertEqual(config['TABLE_NAME'], 'weather')

    def test_factory_weather_icon_poland_forecast(self):
        factory = FactoryWeatherICONPolandForecast()
        config = factory.get_config()
        self.assertIn('AREA', config)
        self.assertIn('TABLE_NAME', config)
        self.assertIn('FORECAST_HOURS', config)
        self.assertEqual(config['TABLE_NAME'], 'forecast')

    def test_factory_weather_ow_subregion_today(self):
        factory = FactoryWeatherOWSUBREGIONToday()
        config = factory.get_config()
        self.assertEqual(config['URL_ELEM'], 'weather')

    def test_factory_weather_ow_subregion_forecast(self):
        factory = FactoryWeatherOWSUBREGIONForecast()
        config = factory.get_config()
        self.assertEqual(config['URL_ELEM'], 'forecast')


if __name__ == '__main__':
    unittest.main()
