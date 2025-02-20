import unittest
from unittest.mock import patch, MagicMock
from weather.weather_downloader import IconEuApiDownloader, OpenWeatherApiDownloader


class TestIconEuApiDownloader(unittest.TestCase):

    @patch('os.makedirs')
    @patch('weather.weather_downloader.requests.get')
    def test_icon_eu_downloader(self, mock_requests, mock_makedirs):
        mock_requests.return_value.status_code = 200
        mock_requests.return_value.iter_content = MagicMock(return_value=[b'data'])

        downloader = IconEuApiDownloader({
            "DATE": "20231025",
            "LEVELS_T_SO": [0],
            "LEVELS_W_SO": [0],
            "FORECAST_HOUR": "00",
            "FORECAST_HOURS": ["000", "003"],
            "BASE_URL": "https://test-url.com",
            "DOWNLOAD_FOLDER_ICON": "/tmp/test-icon",
        })

        downloader.run()
        mock_makedirs.assert_called()
        mock_requests.assert_called()


class TestOpenWeatherApiDownloader(unittest.TestCase):

    @patch('weather.weather_downloader.get_centroids')
    @patch('weather.weather_downloader.requests.get')
    def test_openweather_downloader(self, mock_requests, mock_get_centroids):
        mock_requests.return_value.json.return_value = {"weather": "clear"}
        mock_get_centroids.return_value = MagicMock(head=lambda x: [{"id": 1, "geom": MagicMock(x=10.0, y=20.0)}])

        downloader = OpenWeatherApiDownloader({
            "API_KEYS": ["test-api-key"],
            "URL_ELEM": "weather",
            "TMP_DF": [],
        })
        downloader.run()
        mock_requests.assert_called()


if __name__ == '__main__':
    unittest.main()
