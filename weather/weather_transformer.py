import bz2
import os

import pandas as pd
import geopandas as gpd
from abc import ABC, abstractmethod
from datetime import datetime, timezone

import pygrib

from pass_logging import logger
from pass_utils import make_parallel


class Transformer(ABC):
    @abstractmethod
    def get_transform(self):
        """transform method varies depending on weather source"""
        ...


class IconEuTransformer(Transformer):

    def get_transform(self, input_directory, output_file):
        """
        Przetwarza wszystkie pliki `.grib2` z katalogu wejściowego, scala dane i zapisuje do jednego pliku GeoJSON.

        :param input_directory: Katalog wejściowy z plikami GRIB2.
        :param output_file: Ścieżka do pliku wyjściowego (jeden scalony plik CSV).
        """
        all_data = []  # Lista na dane z wszystkich plików .grib2

        # Przetwarzanie wszystkich plików .grib2 w katalogu wejściowym
        files_to_process = [
            os.path.join(input_directory, filename)
            for filename in os.listdir(input_directory) if filename.endswith(".grib2")
        ]

        print(f"Przetwarzanie {len(files_to_process)} plików GRIB2...")

        def process_single_file(file_path):
            """
            Przetwarza pojedynczy plik GRIB2 i zwraca jego dane jako DataFrame.

            :param file_path: Ścieżka do pliku GRIB2.
            :return: DataFrame z danymi z pliku.
            """
            try:
                with pygrib.open(file_path) as grb:
                    data_frames = []  # Lista na dane z wiadomości w pliku

                    for message in grb:
                        print(f"Przetwarzanie pliku: {file_path}, Parametr={message.parameterName}")

                        # Pobieranie siatek: szerokość, długość i wartość
                        lats, lons = message.latlons()
                        values = message.values

                        # Tworzenie DataFrame
                        df = pd.DataFrame({
                            'latitude': lats.flatten(),
                            'longitude': lons.flatten(),
                            f"{message.parameterName} ({message.units})": values.flatten()
                        })

                        data_frames.append(df)

                    # Scalenie wszystkich DataFrame'ów z jednego pliku
                    if data_frames:
                        combined_df = pd.concat(data_frames, axis=1)
                        return combined_df

            except Exception as e:
                print(f"Błąd podczas przetwarzania pliku {file_path}: {e}")
                return None

        # Przetwarzanie plików równolegle
        results = make_parallel(process_single_file, files_to_process)

        # Dodanie wszystkich przetworzonych DataFrame'ów do listy
        for result in results:
            if result is not None:
                all_data.append(result)

        # Scalanie przetworzonych danych z wszystkich plików w jeden DataFrame
        if all_data:
            final_df = pd.concat(all_data, axis=1)  # Scala dane jako wiersze opisujące te same obiekty
            final_df = final_df.loc[:,
                       ~final_df.columns.duplicated()]  # Usuwanie duplikatów kolumn (np. latitude, longitude)

            # Tworzenie obiektu GeoDataFrame
            gdf = gpd.GeoDataFrame(
                final_df,
                geometry=gpd.points_from_xy(final_df.longitude, final_df.latitude),
                crs="EPSG:4326"

            )
            gdf.to_file(output_file.replace(".csv", ".fgb"), driver="FlatGeobuf")

            print(f"Scalono wszystkie dane i zapisano do pliku: {output_file.replace('.csv', '.fgb')}")
        else:
            print("Nie przetworzono żadnych danych!")


if __name__ == "__main__":
    # --------------------------- DOWNLOAD -------------------------------
    from weather_downloader import IconEuApiDownloader
    from weather.weather_extractor import IconEuExtractor

    date = datetime.now(timezone.utc).strftime("%Y%m%d")  # Current date (YYYYMMDD format)
    forecast_hour = "03"  # Forecast hour ("00", "06", "12", "18" are typical values)
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
    OUTPUT_FOLDER = "./processed_files"
    print("Przetwarzanie plików...")
    output_file = os.path.join(OUTPUT_FOLDER, "final_output.csv")
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)  # Tworzy folder wyjściowy, jeśli nie istnieje

    tranformer = IconEuTransformer()
    tranformer.get_transform(output_directory, output_file)
    print("Przetwarzanie zakończone i wynik zapisany do jednego pliku!")

    # --------------------------- LOAD  -------------------------------
