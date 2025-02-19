import bz2
import os
import pandas as pd
import geopandas as gpd
import pygrib
from abc import ABC, abstractmethod
from shapely.geometry import Point
from datetime import datetime, timezone, timedelta

from pass_logging import logger
from pass_utils import make_parallel

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


class Transformer(ABC):
    @abstractmethod
    def transform_data(self):
        """transform method varies depending on weather source"""
        ...


class IconEuTransformer(Transformer):

    def __init__(self, config):
        self.output_folder = config["DOWNLOAD_FOLDER_ICON"]
        self.temp_folder = config["TMP_FOLDER"]
        self.day = config["DATE"]
        self.FORECAST_HOURS = config["FORECAST_HOURS"]
        self.area = config["AREA"]


    def transform_data(self):
        downloaded_files: list = [
            os.path.join(self.output_folder, filename)
            for filename in os.listdir(self.output_folder)
            if filename.endswith(".grib2") and self.day in filename
        ]

        for hour in self.FORECAST_HOURS:
            all_dataframes = []
            for file_path in downloaded_files:
                if hour in file_path:

                    logger.info(f"Przetwarzanie pliku GRIB2: {file_path}")
                    try:
                        df = self.transform_single_file(file_path)
                        all_dataframes.append(df)
                    except Exception as e:
                        logger.info(f"Błąd przetwarzania pliku {file_path}: {e}")

            if not all_dataframes:
                logger.info(f"No valid data to process for hour {hour}")
                continue

            combined_dataframe = pd.concat(all_dataframes, ignore_index=True)
            combined_dataframe = combined_dataframe.groupby(['latitude', 'longitude'], as_index=False).first()

            combined_dataframe["geometry"] = combined_dataframe.apply(
                lambda row: Point(row["longitude"], row["latitude"]), axis=1)
            gdf = gpd.GeoDataFrame(
                combined_dataframe,
                geometry="geometry",
                crs="EPSG:4326"
            )

            logger.info(gdf.head())

            if not os.path.exists(self.temp_folder):
                os.makedirs(self.temp_folder)
            output_file = os.path.join(self.temp_folder, f"combined_grib_data_{self.day}_{hour}.fgb")
            gdf.to_file(output_file, driver="flatgeobuf")

            logger.info(f"Combined data saved to file: {output_file}")


    def transform_single_file(self, file_path):
        data_records = []

        def process_grib_message(grb):
            lats, lons = grb.latlons()
            values = grb.values
            parameter_name = grb.parameterName
            validity_datetime = datetime.strptime(
                f"{grb.validityDate:08d}{grb.validityTime:04d}", "%Y%m%d%H%M"
            )
            bounds = self.area  # This is set to `AREA_BOUNDS` from the config
            lat_min, lat_max = bounds["lat_min"], bounds["lat_max"]
            lon_min, lon_max = bounds["lon_min"], bounds["lon_max"]

            mask = (lats >= lat_min) & (lats <= lat_max) & (lons >= lon_min) & (lons <= lon_max)

            filtered_lats = lats[mask]
            filtered_lons = lons[mask]
            filtered_values = values[mask]

            return [
                {
                    "latitude": lat,
                    "longitude": lon,
                    parameter_name: value,
                    "update_on": validity_datetime,
                }
                for lat, lon, value in zip(filtered_lats.flatten(), filtered_lons.flatten(), filtered_values.flatten())
            ]

        with pygrib.open(file_path) as grbs:
            grib_messages = list(grbs)
            results = make_parallel(process_grib_message, grib_messages)

        data_records = [record for result in results for record in result] #splaszczania do df

        return pd.DataFrame(data_records)


if __name__ == "__main__":
    # --------------------------- Transform  -------------------------------
    transformer = IconEuTransformer()
    transformer.transform_data()
