import unittest
from unittest.mock import patch, MagicMock
from weather.weather_uploader import OpenWeatherApiUploader, IconEUDBUploader


class TestOpenWeatherApiUploader(unittest.TestCase):

    @patch('weather.weather_uploader.connect_to_db')
    def test_uploader(self, mock_connect_to_db):
        mock_engine = MagicMock()
        mock_connect_to_db.return_value = mock_engine
        uploader = OpenWeatherApiUploader({
            "TMP_DF": MagicMock(to_sql=MagicMock()),
            "URL_ELEM": "test_table"
        })
        uploader.run()
        uploader.config['TMP_DF'].to_sql.assert_called()


class TestIconEUDBUploader(unittest.TestCase):

    @patch('os.listdir')
    @patch('geopandas.read_file')
    @patch('weather.weather_uploader.connect_to_db')
    def test_uploader(self, mock_connect_to_db, mock_read_file, mock_listdir):
        mock_listdir.return_value = ["test.fgb"]
        mock_read_file.return_value = MagicMock(to_postgis=MagicMock())
        uploader = IconEUDBUploader({
            "DOWNLOAD_FOLDER_ICON": "/tmp",
            "DATE": "20231025",
            "TMP_FOLDER": "/tmp",
            "TABLE_NAME": "test_table"
        })
        uploader.run()
        mock_read_file.assert_called()


if __name__ == '__main__':
    unittest.main()
