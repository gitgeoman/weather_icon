from abc import ABC, abstractmethod

from weather import weather_downloader, weather_extractor, weather_uploader, weather_transformer


class HandlerWeatherFactory(ABC):
    """handlerFactory gets, unpacks, uploads depending on source and target"""

    @abstractmethod
    def get_downloader(self) -> weather_downloader.Downloader:
        ...

    @abstractmethod
    def get_extractor(self) -> weather_extractor.Extractor:
        ...

    @abstractmethod
    def get_transformer(self) -> weather_transformer.Transformer:
        ...

    @abstractmethod
    def get_uploader(self) -> weather_uploader.Uploader:
        ...


class IconEuWeatherFactory(HandlerWeatherFactory):
    """Factory for handling ICON-EU weather data."""

    def get_downloader(self) -> weather_downloader.Downloader:
        return weather_downloader.IconEuApiDownloader()

    def get_extractor(self) -> weather_extractor.Extractor:
        return weather_extractor.IconEuExtractor()

    def get_transformer(self) -> weather_transformer.Transformer:
        return weather_transformer.IconEuTransformer()

    def get_uploader(self) -> weather_uploader.Uploader:
        return weather_uploader.DBUploader()
