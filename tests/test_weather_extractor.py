import unittest
from unittest.mock import MagicMock
from weather.weather_extractor import (
    OpenWeatherApiExtractorToday,
    OpenWeatherApiExtractorForecast,
    IconEuExtractor
)


class TestOpenWeatherApiExtractorToday(unittest.TestCase):

    def test_extractor_today(self):
        extractor = OpenWeatherApiExtractorToday({
            "TMP_DF": [(1, {"main": {"temp": 25}, "weather": [{"id": 801, "main": "Clouds"}]})]
        })
        extractor.run()
        self.assertIn('temp', extractor.config['TMP_DF'])


class TestOpenWeatherApiExtractorForecast(unittest.TestCase):

    def test_extractor_forecast(self):
        extractor = OpenWeatherApiExtractorForecast({
            "TMP_DF": [(1, {"city": "Test City", "list": [{"main": {"temp": 25}, "weather": [{"id": 800}]}, ]})]
        })
        extractor.run()
        self.assertEqual(len(extractor.config['TMP_DF']), 1)


if __name__ == '__main__':
    unittest.main()
