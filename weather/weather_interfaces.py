from abc import ABC, abstractmethod

from weather import weather_downloader, weather_extractor, weather_uploader, weather_transformer


class HandlerWeatherFactory(ABC):
    """handlerFactory gets, unpacks, uploads depending on source and target"""

    @abstractmethod
    def get_downloader(self, **kwargs) -> weather_downloader.Downloader:
        ...

    @abstractmethod
    def get_extractor(self, **kwargs) -> weather_extractor.Extractor:
        ...

    @abstractmethod
    def get_transformer(self, **kwargs) -> weather_transformer.Transformer:
        ...

    @abstractmethod
    def get_uploader(self, **kwargs) -> weather_uploader.Uploader:
        ...


class HandlerIconEuWeather(HandlerWeatherFactory):
    """Factory for handling ICON-EU weather data."""

    def get_downloader(self, **kwargs) -> weather_downloader.Downloader:
        return weather_downloader.IconEuApiDownloader(config=kwargs["config"])

    def get_extractor(self, **kwargs) -> weather_extractor.Extractor:
        return weather_extractor.IconEuExtractor(config=kwargs["config"])

    def get_transformer(self, **kwargs) -> weather_transformer.Transformer:
        return weather_transformer.IconEuTransformer(config=kwargs["config"])

    def get_uploader(self, **kwargs) -> weather_uploader.Uploader:
        return weather_uploader.IconEUDBUploader(config=kwargs["config"])


class HandlerOWREGIONWeather(HandlerWeatherFactory):
    """Factory for handling ICON-EU weather data."""

    def get_downloader(self, **kwargs) -> weather_downloader.Downloader:
        return weather_downloader.OpenWeatherApiDownloader(config=kwargs["config"])

    def get_extractor(self, **kwargs) -> weather_extractor.Extractor:
        return weather_extractor.OpenWeatherApiExtractor(config=kwargs["config"])

    def get_transformer(self, **kwargs) -> weather_transformer.Transformer:
        return weather_transformer.OpenWeatherApiTransformer(config=kwargs["config"])

    def get_uploader(self, **kwargs) -> weather_uploader.Uploader:
        return weather_uploader.OpenWeatherApiUploader(config=kwargs["config"])
