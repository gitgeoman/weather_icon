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
    def extract(self) -> pd.DataFrame:
        """Extract method varies depending on weather source"""
        ...


class IconEuExtractor(Extractor):
    """Extractor for ICON-EU GRIB2 files."""

    output_folder: str = "./downloaded_files"

    def extract(self):
        file_paths: list = [os.path.join(self.output_folder, filename)
                            for filename in os.listdir(self.output_folder) if filename.endswith(".bz2")]
        logger.info(f"Rozpakowywanie {len(file_paths)} plików .bz2...")
        make_parallel(self.extract_single_file, items=file_paths, )
        logger.info("Rozpakowywanie zakończone!")

    def extract_single_file(self, file_path):
        """Extracts a single file and returns its decompressed path."""
        output_file_path = os.path.join(self.output_folder, os.path.basename(file_path)[:-4])

        try:
            with bz2.BZ2File(file_path, 'rb') as compressed_file, open(output_file_path, 'wb') as decompressed_file:
                decompressed_file.write(compressed_file.read())
            logger.info(f"Rozpakowano: {output_file_path}")
            return output_file_path
        except Exception as e:
            logger.error(f"Błąd podczas rozpakowywania pliku {file_path}: {e}", exc_info=True)
            return None


if __name__ == "__main__":
    # --------------------------- EXTRACT  -------------------------------
    extractor = IconEuExtractor()
    extractor.extract()
