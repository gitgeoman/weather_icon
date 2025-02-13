import bz2
import os

from abc import ABC, abstractmethod
import pygrib

from shapely.geometry import Point
import pandas as pd
import geopandas as gpd
import time

from datetime import datetime, timezone, timedelta

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
    output_folder = "./downloaded_files"

    def get_transform(self):
        downloaded_files: list = [
            os.path.join(self.output_folder, filename)
            for filename in os.listdir(self.output_folder)
            if filename.endswith(".grib2")
        ]
        task_start_time = time.time()
        # FORECAST_HOURS = ["000", "003", "006"] # TODO comment after tests
        FORECAST_HOURS = ["000", "003", "006", "009", "012", "015", "018", "021", "024", "027", "030", "033", "036",
                          "039", "042", "048"]  # TODO uncomment after tests
        for hour in FORECAST_HOURS:
            all_dataframes = []
            hour_start_time = time.time()
            for file_path in downloaded_files:
                if hour in file_path:

                    print(f"Przetwarzanie pliku GRIB2: {file_path}")
                    try:
                        # Corrected function name
                        df = self.transform_single_file(file_path)
                        all_dataframes.append(df)
                    except Exception as e:
                        print(f"Błąd przetwarzania pliku {file_path}: {e}")

            if not all_dataframes:
                print(f"No valid data to process for hour {hour}")
                continue  # Skip this hour if no files were processed successfully

            # Combine all DataFrame objects into one
            combined_dataframe = pd.concat(all_dataframes, ignore_index=True)

            # Remove duplicates in case of merging the same coordinates with different parameters
            combined_dataframe = combined_dataframe.groupby(['latitude', 'longitude'], as_index=False).first()

            gdf = gpd.GeoDataFrame(
                combined_dataframe,
                geometry=[
                    Point(lon, lat) for lon, lat in zip(combined_dataframe["longitude"], combined_dataframe["latitude"])
                ],
                crs="EPSG:4326"  # Standard coordinate reference system WGS84
            )

            print(gdf.head())

            output_file = f"combined_grib_data_{hour}.fgb"
            gdf.to_file(output_file, driver="flatgeobuf")
            print(f"Combined data saved to file: {output_file}")
            hour_end_time = time.time()
            print(f"Time taken for forecast hour {hour}: {hour_end_time - hour_start_time:.2f} seconds\n")

        task_end_time = time.time()
        print(f"Total task duration: {task_end_time - task_start_time:.2f} seconds")

    def transform_single_file(self, file_path):  # Corrected function name
        data_records = []

        # Function to process GRIB messages
        def process_grib_message(grb):
            lats, lons = grb.latlons()
            values = grb.values
            parameter_name = grb.parameterName
            validity_datetime = datetime.strptime(
                f"{grb.validityDate:08d}{grb.validityTime:04d}", "%Y%m%d%H%M"
            )
            return [
                {
                    "latitude": lat,
                    "longitude": lon,
                    parameter_name: value,
                    "update_on": validity_datetime,
                }
                for lat, lon, value in zip(lats.flatten(), lons.flatten(), values.flatten())
            ]

        # Open the GRIB file
        with pygrib.open(file_path) as grbs:
            # Parallel processing
            grib_messages = list(grbs)
            results = make_parallel(process_grib_message, grib_messages)

        # Flatten the collective results
        data_records = [record for result in results for record in result]

        # Convert the list of records to a Pandas DataFrame
        return pd.DataFrame(data_records)


if __name__ == "__main__":
    # --------------------------- Transform  -------------------------------
    transformer = IconEuTransformer()  # Corrected variable name
    transformer.get_transform()
