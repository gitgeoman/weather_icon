import bz2
import os
from abc import ABC, abstractmethod

from pass_logging import logger
from pass_utils import make_parallel


class Extractor(ABC):
    @abstractmethod
    def extract(self) -> None:
        """Extract method varies depending on weather source"""
        ...


class IconEuExtractor(Extractor):
    """Extractor for ICON-EU GRIB2 files."""

    def __init__(self, config):
        self.output_folder: str = config["DOWNLOAD_FOLDER_ICON"]

    def extract(self):

        file_paths: list[str] = [os.path.join(self.output_folder, filename)
                            for filename in os.listdir(self.output_folder) if filename.endswith(".bz2")]
        if not file_paths:
            logger.warning("Brak plików .bz2 w katalogu.")
            return
        make_parallel(self.extract_single_file, items=file_paths, )
        logger.info("Rozpakowywanie zakończone!")

    def extract_single_file(self, file_path: str) -> str | None:
        """Extracts a single file and returns its decompressed path."""

        output_file_path = os.path.join(self.output_folder, os.path.splitext(os.path.basename(file_path))[0])

        if os.path.exists(output_file_path):
            logger.info(f"Plik już rozpakowany: {output_file_path}")
            return output_file_path

        try:
            with bz2.BZ2File(file_path, 'rb') as compressed_file, open(output_file_path, 'wb') as decompressed_file:
                decompressed_file.write(compressed_file.read())
            logger.info(f"Rozpakowano: {output_file_path}")
            return output_file_path

        except (IOError, OSError, bz2.BZ2File) as e:
            logger.error(f"Błąd podczas rozpakowywania pliku {file_path}: {e}", exc_info=True)
            return None


if __name__ == "__main__":
    pass
