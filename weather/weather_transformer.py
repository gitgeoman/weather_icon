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
        data_frames = {}
        final_data_frames = []

        min_lat, max_lat = 49.0, 55.0
        min_lon, max_lon = 14.0, 24.0

        for grib_file in grib_files:
            metadane = grib_file.split("/")[-1].split("_")[3:]
            print(metadane[2])
            with pygrib.open(grib_file) as grb:
                for message in grb:
                    print(f"Przetwarzanie pliku: {grib_file}, Parametr={message.parameterName}")

                    lats, lons = message.latlons()
                    values = message.values
                    valid_time = message.validDate
                    # Maskowanie współrzędnych spoza obszaru Polski
                    mask = (
                            (lats >= min_lat) & (lats <= max_lat) &
                            (lons >= min_lon) & (lons <= max_lon)
                    )

                    # Filtrowanie danych na podstawie maski
                    filtered_lats = lats[mask]
                    filtered_lons = lons[mask]
                    filtered_values = values[mask]

                    if filtered_lats.size == 0 or filtered_lons.size == 0:
                        logger.warning(
                            f"Brak danych w obszarze Polski dla pliku {grib_file} i parametru {message.parameterName}.")
                        continue

                    # Tworzenie DataFrame
                    df = pd.DataFrame({
                        'latitude': filtered_lats.flatten(),
                        'longitude': filtered_lons.flatten(),
                        f"precip_rate" if message.parameterName == "Total precipitation rate" else
                        f"soil_moisture" if message.parameterName == "Column-integrated soil moisture" else
                        f"temp" if message.parameterName == "Temperature" else
                        f"soil_temp" if message.parameterName == "Soil temperature" else
                        f"{message.parameterName}":
                            filtered_values.flatten() - 273.15 if message.parameterName in ["Temperature",
                                                                                            "Soil temperature"] else
                            filtered_values.flatten(),
                        'valid_time': valid_time,
                    })
                    tmp_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['longitude'], df['latitude']),crs="EPSG:4326")
                    tmp_gdf.to_file(f'./{grib_file.split("/")[-1]}',driver="FlatGeobuf")
                    key = (valid_time, lats.shape[0], lons.shape[1])
                    if key not in data_frames:
                        data_frames[key] = df
                    else:
                        data_frames[key] = pd.merge(
                            data_frames[key],
                            df.drop(columns=["valid_time", "latitude", "longitude"]),
                            left_index=True,
                            right_index=True
                        )
        for key, df in data_frames.items():
            final_data_frames.append(df)

        # Convert final DataFrame list to a GeoDataFrame
        if final_data_frames:
            combined_df = pd.concat(final_data_frames, ignore_index=True)
            gdf = gpd.GeoDataFrame(
                combined_df,
                geometry=gpd.points_from_xy(combined_df['longitude'], combined_df['latitude']),
                crs="EPSG:4326"  # Assigning latitude/longitude CRS
            )
            return gdf
        else:
            logger.error("No data frames were created during transformation.")
            return None

    def get_grb_files_list_from_dir(self):
        grib2_files = []
        for root, _, files in os.walk('./downloaded_files'):
            grib2_files.extend(
                os.path.join(root, file) for file in files if file.endswith(".grib2")
            )
        return grib2_files


if __name__ == "__main__":
    # --------------------------- DOWNLOAD -------------------------------
    from weather_downloader import IconEuApiDownloader
    from weather.weather_extractor import IconEuExtractor

    date = datetime.now(timezone.utc).strftime("%Y%m%d")  # Current date (YYYYMMDD format)
    forecast_hour = "00"  # Forecast hour ("00", "06", "12", "18" are typical values)
    output_directory = "./downloaded_files"

    os.makedirs(output_directory, exist_ok=True)

    downloader = IconEuApiDownloader()
    downloaded_files: list = downloader.get_data((output_directory, forecast_hour), date)

    logger.info(f"Downloaded files: {downloaded_files}")

    # --------------------------- EXTRACT  -------------------------------
    extractor = IconEuExtractor()
    logger.info(f"Rozpakowywanie {len(downloaded_files)} plików .bz2...")

    decompressed_files = [
        result for result in make_parallel(extractor.extract, downloaded_files, output_directory=output_directory)
        if result is not None
    ]

    logger.info("Rozpakowywanie zakończone!")
    logger.info(f"Rozpakowane pliki: {decompressed_files}")

    # --------------------------- TRANSFORM  -------------------------------
    logger.info("Transformacja...")
    transformer = IconEuTransformer()
    gdf = transformer.get_transform()
    gdf.to_file("transformed_data.fgb", driver="FlatGeobuf")

    # --------------------------- LOAD  -------------------------------
