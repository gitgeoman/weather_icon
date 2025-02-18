from abc import ABC
from datetime import datetime, timezone

from weather import weather_interfaces
from pass_logging import logger

class WeatherArea(ABC):
    "Base area of interest"

    aoi_name = str
    resolution = str

    handler: weather_interfaces.HandlerWeatherFactory

    def process(self):
        downloader = self.handler.get_downloader()
        extractor = self.handler.get_extractor()
        transformer = self.handler.get_transformer()
        uploader = self.handler.get_uploader()

        logger.info(f"...rozpoczynam pobieranie danych ICON ")
        downloader.get_data()
        extractor.extract()
        transformer.transform_data()
        uploader.upload_data()


        logger.info(f"...zakoczono ŁADOWNIE danych ICON")




class FactoryWeatherICONToday(WeatherArea):

    handler = weather_interfaces.HandlerIconEuWeather()