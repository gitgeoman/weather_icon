import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from dotenv import load_dotenv

from weather import weather_interfaces
from pass_logging import logger
from weather_areas import Area


class WeatherArea(ABC):
    """Abstract class for weather area processing from different sources."""

    def __init__(self):
        self.config = self.get_config()

    @abstractmethod
    def get_config(self) -> dict:
        """Each factory must define its own configuration."""
        pass

    resolution = str
    config: dict
    handler: weather_interfaces.HandlerWeatherFactory

    def process(self):
        downloader = self.handler.get_downloader(config=self.config)
        extractor = self.handler.get_extractor(config=self.config)
        transformer = self.handler.get_transformer(config=self.config)
        uploader = self.handler.get_uploader(config=self.config)

        logger.info(f"...starting process ")
        downloader.get_data()
        extractor.extract()
        transformer.transform_data()
        uploader.upload_data()

        logger.info(f"...process completed")


class FactoryWeatherICONPolandToday(WeatherArea):
    def get_config(self) -> dict:
        return {
            'DATE': datetime.now(timezone.utc).strftime("%Y%m%d"),
            'LEVELS_T_SO': [0],
            'LEVELS_W_SO': [0],
            'FORECAST_HOUR': "00",  # Prognozowana godzina ("00", "06", "12", "18")
            'FORECAST_HOURS': [
                "000", "003", "006", "009", "012", "015", "018", "021", "024"
            ],
            'BASE_URL': "https://opendata.dwd.de/weather/nwp/icon-eu/grib",
            'DOWNLOAD_FOLDER_ICON': "./downloaded_files",
            'TMP_FOLDER': './tmp',
            'AREA': Area.POLAND.get_bounds()
        }

    handler = weather_interfaces.HandlerIconEuWeather()


class FactoryWeatherICONPolandForecast(WeatherArea):
    def get_config(self) -> dict:
        return {
            'DATE': datetime.now(timezone.utc).strftime("%Y%m%d"),
            'LEVELS_T_SO': [0],
            'LEVELS_W_SO': [0],
            'FORECAST_HOUR': "00",  # Prognozowana godzina ("00", "06", "12", "18")
            'FORECAST_HOURS': [
                "000", "003", "006", "009", "012", "015", "018", "021", "024", "027",
                "030", "033", "036", "039", "042", "048"
            ],
            'BASE_URL': "https://opendata.dwd.de/weather/nwp/icon-eu/grib",
            'DOWNLOAD_FOLDER_ICON': "./downloaded_files",
            'TMP_FOLDER': './tmp',
            'AREA': Area.POLAND.get_bounds()
        }

    handler = weather_interfaces.HandlerIconEuWeather()


load_dotenv('../.env')

db_name: str = os.getenv('DB_NAME')
db_user: str = os.getenv('DB_USER')
db_password: str = os.getenv('DB_PASSWORD')
db_port: str = os.getenv('DB_PORT')
db_host: str = os.getenv('DB_HOST')
WEATHER_API_KEY_1: str = os.getenv('WEATHER_API_KEY_1')
WEATHER_API_KEY_2: str = os.getenv('WEATHER_API_KEY_2')
WEATHER_API_KEY_3: str = os.getenv('WEATHER_API_KEY_3')
WEATHER_API_KEY_4: str = os.getenv('WEATHER_API_KEY_4')
WEATHER_API_KEY_5: str = os.getenv('WEATHER_API_KEY_5')
WEATHER_API_KEY_6: str = os.getenv('WEATHER_API_KEY_6')
WEATHER_API_KEY_7: str = os.getenv('WEATHER_API_KEY_7')
WEATHER_API_KEY_8: str = os.getenv('WEATHER_API_KEY_8')
WEATHER_API_KEY_9: str = os.getenv('WEATHER_API_KEY_9')
WEATHER_API_KEY_10: str = os.getenv('WEATHER_API_KEY_10')
WEATHER_API_KEY_11: str = os.getenv('WEATHER_API_KEY_11')
WEATHER_API_KEY_12: str = os.getenv('WEATHER_API_KEY_12')

WEATHER_API_KEYS: list = [
    # WEATHER_API_KEY_1,
    WEATHER_API_KEY_2,
    WEATHER_API_KEY_3,
    WEATHER_API_KEY_4,
    WEATHER_API_KEY_5,
    WEATHER_API_KEY_6,
    WEATHER_API_KEY_7,
    WEATHER_API_KEY_8,
    WEATHER_API_KEY_9,
    WEATHER_API_KEY_10,
    WEATHER_API_KEY_11,
    WEATHER_API_KEY_12,
]


class FactoryWeatherOWSUBREGIONToday(WeatherArea):

    def get_config(self) -> dict:
        return {
            'TMP_DF': [],
            "URL_ELEM": "weather",
            "API_KEYS": WEATHER_API_KEYS,
            'AREA': Area.POLAND.get_bounds()
        }

    handler = weather_interfaces.HandlerOWREGIONWeather()
