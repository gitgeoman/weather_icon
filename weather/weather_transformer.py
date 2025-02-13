import bz2
import os

import pandas as pd
import geopandas as gpd
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta

import pygrib

from pass_logging import logger
from pass_utils import make_parallel

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

class Transformer(ABC):
    @abstractmethod
    def get_transform(self):
        """transform method varies depending on weather source"""
        ...


class IconEuTransformer(Transformer):

    def get_transform(self):
        grib_files: list = self.get_grb_files_list_from_dir()
        data_frames = []
        for grib_file in grib_files:
            metadane = grib_file.split("/")[-1].split("_")[3:]
            print(metadane[2])
            with pygrib.open(grib_file) as grb:

                for message in grb:
                    print(f"Przetwarzanie pliku: {grib_file}, Parametr={message.parameterName}")

                    lats, lons = message.latlons()
                    values = message.values
                    valid_time = message.validDate

                    # Tworzenie DataFrame
                    df = pd.DataFrame({
                        'latitude': lats.flatten(),
                        'longitude': lons.flatten(),
                        f"{message.parameterName} ({message.units})": values.flatten(),
                        'valid_time': valid_time,
                    })
                    print(df)
                    data_frames.append({'df_data':df,'metadane':valid_time})
        # for df in data_frames:
        #     print(df['metadane'])




    def get_grb_files_list_from_dir(self):
        grib2_files = []
        for root, _, files in os.walk('./downloaded_files'):
            grib2_files.extend(
                os.path.join(root, file) for file in files if file.endswith(".grib2")
            )
        return grib2_files


if __name__ == "__main__":
    # # --------------------------- DOWNLOAD -------------------------------
    # from weather_downloader import IconEuApiDownloader
    # from weather.weather_extractor import IconEuExtractor
    #
    # date = datetime.now(timezone.utc).strftime("%Y%m%d")  # Current date (YYYYMMDD format)
    # forecast_hour = "00"  # Forecast hour ("00", "06", "12", "18" are typical values)
    # output_directory = "./downloaded_files"
    #
    # os.makedirs(output_directory, exist_ok=True)
    #
    # downloader = IconEuApiDownloader()
    # downloaded_files: list = downloader.get_data((output_directory, forecast_hour), date)
    #
    # logger.info(f"Downloaded files: {downloaded_files}")
    #
    # # --------------------------- EXTRACT  -------------------------------
    # extractor = IconEuExtractor()
    # logger.info(f"Rozpakowywanie {len(downloaded_files)} plików .bz2...")
    #
    # decompressed_files = [
    #     result for result in make_parallel(extractor.extract, downloaded_files, output_directory=output_directory)
    #     if result is not None
    # ]
    #
    # logger.info("Rozpakowywanie zakończone!")
    # logger.info(f"Rozpakowane pliki: {decompressed_files}")

    # --------------------------- TRANSFORM  -------------------------------
    logger.info("Transformacja...")
    transormer = IconEuTransformer()
    transformed_data = transormer.get_transform()

    # --------------------------- LOAD  -------------------------------
