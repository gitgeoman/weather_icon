import unittest
from unittest.mock import patch, MagicMock
from weather.weather_transformer import IconEuTransformer


class TestIconEuTransformer(unittest.TestCase):

    @patch('os.listdir')
    @patch('os.path.exists')
    def test_transformer(self, mock_exists, mock_listdir):
        mock_exists.return_value = True
        mock_listdir.return_value = ['test.grib2']
        transformer = IconEuTransformer({
            "DOWNLOAD_FOLDER_ICON": "/tmp/test-downloads",
            "TMP_FOLDER": "/tmp/test-processed",
            "DATE": "20231025",
            "AREA": {"lat_min": 0, "lat_max": 50, "lon_min": 0, "lon_max": 50},
            "FORECAST_HOURS": ["000"]
        })
        transformer.run()

        mock_listdir.assert_called()


if __name__ == '__main__':
    unittest.main()
