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
    def extract(self, file_path, output_directory) -> pd.DataFrame:
        """Extract method varies depending on weather source"""
        ...


class IconEuExtractor(Extractor):
    """Extractor for ICON-EU GRIB2 files."""

    def extract(self, file_path, output_directory):
        """Extracts a single file and returns its decompressed path."""
        output_file_path = os.path.join(output_directory,
                                        os.path.basename(file_path)[:-4])  # Remove .bz2 from file name

        if not os.path.exists(file_path):
            logger.error(f"Plik {file_path} nie istnieje.")
            return None

        try:
            with bz2.BZ2File(file_path, 'rb') as compressed_file, open(output_file_path, 'wb') as decompressed_file:
                decompressed_file.write(compressed_file.read())
            logger.info(f"Rozpakowano: {output_file_path}")
            return output_file_path  # Return the path of successfully decompressed file
        except Exception as e:
            logger.error(f"Błąd podczas rozpakowywania pliku {file_path}: {e}", exc_info=True)
            return None  # Return None in case of an error


if __name__ == "__main__":
    # --------------------------- DOWNLOAD -------------------------------
    from weather_downloader import IconEuApiDownloader

    # Configurations
    date = datetime.now(timezone.utc).strftime("%Y%m%d")  # Current date (YYYYMMDD format)
    forecast_hour = "03"  # Forecast hour ("00", "06", "12", "18" are typical values)
    output_directory = "./downloaded_files"

    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    # Use the downloader
    downloader = IconEuApiDownloader()
    downloaded_files: list = downloader.get_data((output_directory, forecast_hour), date)

    logger.info(f"Downloaded files: {downloaded_files}")

    # --------------------------- EXTRACT  -------------------------------
    extractor = IconEuExtractor()
    logger.info(f"Rozpakowywanie {len(downloaded_files)} plików .bz2...")

    # Process files in parallel and remove None values
    decompressed_files = [
        result for result in make_parallel(extractor.extract, downloaded_files, output_directory=output_directory)
        if result is not None
    ]

    logger.info("Rozpakowywanie zakończone!")
    logger.info(f"Rozpakowane pliki: {decompressed_files}")
