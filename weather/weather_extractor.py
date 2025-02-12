import bz2
import os

import pandas as pd

from abc import ABC, abstractmethod
from datetime import datetime, timezone

import pygrib

from pass_logging import logger
from pass_utils import make_parallel

class Extractor(ABC):
    @abstractmethod
    def extract(self, input_data: list) -> pd.DataFrame:
        """Extract method varies depending on weather source"""
        ...


class IconEuExtractor(Extractor):
    """Extractor for ICON-EU GRIB2 files."""

    def unpack_file(self, file_path, output_directory):
        """
        Rozpakowuje pojedynczy plik .bz2 i zapisuje wynikowy plik w katalogu docelowym.

        :param file_path: Ścieżka do pliku .bz2.
        :param output_directory: Katalog, gdzie ma zostać zapisany rozpakowany plik.
        """
        output_file_path = os.path.join(output_directory, os.path.basename(file_path)[:-4])  # Usuń .bz2 z nazwy pliku

        try:
            with bz2.BZ2File(file_path, 'rb') as compressed_file:
                with open(output_file_path, 'wb') as decompressed_file:
                    decompressed_file.write(compressed_file.read())
            print(f"Rozpakowano: {output_file_path}")
        except Exception as e:
            print(f"Błąd podczas rozpakowywania pliku {file_path}: {e}")



    def extract(self, input_file: list) -> pd.DataFrame:
        file_path, output_file = input_file

        try:
            # Use pygrib to parse `.grib2` files
            with pygrib.open(file_path) as grb:
                data_frames = []

                for message in grb:
                    lats, lons = message.latlons()
                    values = message.values

                    # Store result as DataFrame
                    df = pd.DataFrame({
                        'latitude': lats.flatten(),
                        'longitude': lons.flatten(),
                        f"{message.parameterName} ({message.units})": values.flatten()
                    })

                    data_frames.append(df)

                if data_frames:
                    return pd.concat(data_frames, axis=1)
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")

        return pd.DataFrame()  # Return empty DataFrame on failure


if __name__ == "__main__":
    # --------------------------- DOWNLOAD -------------------------------
    from weather_downloader import IconEuApiDownloader
    # Configurations
    date = datetime.now(timezone.utc).strftime("%Y%m%d")  # Current date (YYYYMMDD format)
    forecast_hour = "03"  # Forecast hour ("00", "06", "12", "18" are typical values)
    output_directory = "./downloaded_files"

    # Use the downloader
    downloader = IconEuApiDownloader()
    downloaded_files = downloader.get_data((output_directory, forecast_hour), date)

    logger.info(f"Downloaded files:{downloaded_files}")

    # --------------------------- EXTRACT  -------------------------------
    extractor = IconEuExtractor()
    print(f"Rozpakowywanie {len(downloaded_files)} plików .bz2...")
    make_parallel(extractor.unpack_file, downloaded_files, output_directory=output_directory)
    print("Rozpakowywanie zakończone!")
