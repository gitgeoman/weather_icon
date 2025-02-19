from abc import ABC
from datetime import datetime, timezone

from weather import weather_interfaces
from pass_logging import logger
from weather_areas import Area

class WeatherArea(ABC):
    """Abstract class for weather area processing from different sources."""

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
    config = {
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
    config = {
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
