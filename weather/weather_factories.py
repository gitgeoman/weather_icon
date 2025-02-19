import os

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from dotenv import load_dotenv

from weather import weather_interfaces
from weather_areas import Area
from pass_logging import logger


class WeatherArea(ABC):
    """Abstract class for weather area processing from different sources."""

    def __init__(self):
        self.config:dict  = self.get_config()

    @abstractmethod
    def get_config(self) -> dict:
        """Each factory must define its own configuration."""
        pass

    resolution = str
    handler: weather_interfaces.HandlerWeatherFactory

    def process(self):
        steps = [
            self.handler.get_downloader,
            self.handler.get_extractor,
            self.handler.get_transformer,
            self.handler.get_uploader,
        ]

        logger.info("...starting process")

        for step in steps:
            step(config=self.config).run()

        logger.info("...process completed")


# -----------------------------------------------------------------
# ICON
# -----------------------------------------------------------------

class ICONWeatherConfigHelper:
    """Helper class for ICON weather configuration."""
    @staticmethod
    def get_base_config():
        return {
            'DATE': datetime.now(timezone.utc).strftime("%Y%m%d"),
            'LEVELS_T_SO': [0],
            'LEVELS_W_SO': [0],
            'FORECAST_HOUR': "00",  # Prognozowana godzina ("00", "06", "12", "18")
            'BASE_URL': "https://opendata.dwd.de/weather/nwp/icon-eu/grib",
            'DOWNLOAD_FOLDER_ICON': "./downloaded_files",
            'TMP_FOLDER': './tmp',
        }


class FactoryWeatherICONPolandToday(WeatherArea):
    def get_config(self) -> dict:
        config = ICONWeatherConfigHelper.get_base_config()
        config.update({
            'TABLE_NAME': 'weather',
            'FORECAST_HOURS': [
                "000", "003", "006", "009", "012", "015", "018", "021", "024"
            ],
            'AREA': Area.POLAND.get_bounds()
        })
        return config

    handler = weather_interfaces.HandlerIconEuWeather()


class FactoryWeatherICONPolandForecast(WeatherArea):

    def get_config(self) -> dict:
        config = ICONWeatherConfigHelper.get_base_config()
        config.update({
            'TABLE_NAME': 'forecast',
            'FORECAST_HOURS': [
                "000", "003", "006", "009", "012", "015", "018", "021", "024", "027",
                "030", "033", "036", "039", "042", "048"
            ],
            'AREA': Area.POLAND.get_bounds()
        })
        return config


    handler = weather_interfaces.HandlerIconEuWeather()


load_dotenv('../.env')

db_name: str = os.getenv('DB_NAME')
db_user: str = os.getenv('DB_USER')
db_password: str = os.getenv('DB_PASSWORD')
db_port: str = os.getenv('DB_PORT')
db_host: str = os.getenv('DB_HOST')


def load_api_keys(prefix: str = "WEATHER_API_KEY_", count: int = 12) -> list:
    return [os.getenv(f"{prefix}{i}") for i in range(1, count + 1)]

WEATHER_API_KEYS = load_api_keys()

# -----------------------------------------------------------------
# OW
# -----------------------------------------------------------------
class FactoryWeatherOWSUBREGIONToday(WeatherArea):

    def get_config(self) -> dict:
        return {
            "TMP_DF": [],
            "URL_ELEM": "weather",
            "API_KEYS": WEATHER_API_KEYS,
        }

    handler = weather_interfaces.HandlerOWREGIONWeatherToday()


class FactoryWeatherOWSUBREGIONForecast(WeatherArea):

    def get_config(self) -> dict:
        return {
            "TMP_DF": [],
            "URL_ELEM": "forecast",
            "API_KEYS": WEATHER_API_KEYS,
        }

    handler = weather_interfaces.HandlerOWREGIONWeatherForecast()
