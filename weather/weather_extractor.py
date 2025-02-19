import bz2
import os
import pandas as pd

from datetime import datetime
from abc import ABC, abstractmethod

from pass_logging import logger
from pass_utils import make_parallel


class Extractor(ABC):
    @abstractmethod
    def run(self) -> None:
        """Extract method varies depending on weather source"""
        ...


class OpenWeatherApiExtractorToday(Extractor):
    def __init__(self, config):
        self.config = config  # Store the config as an instance variable
        self.tmp_df = self.config['TMP_DF']

    def run(self):
        id_pt, data = self.tmp_df[0]

        self.config['TMP_DF'] = pd.DataFrame([{
            'id_geom': id_pt,
            'temp': data['main']['temp'],
            'temp_max': data['main']['temp_max'],
            'temp_min': data['main']['temp_min'],
            'feels_like': data['main']['feels_like'],
            'pressure': data['main']['pressure'],
            'humidity': data['main']['humidity'],
            'clouds': data['clouds']['all'],
            'weather_id': data['weather'][0]['id'],
            'weather_type': data['weather'][0]['main'],
            'weather_desc': data['weather'][0]['description'],
            'weather_icon': data['weather'][0]['icon'],
            'visibility': data['visibility'],
            'country': data['sys']['country'],
            'city': data['name'],
            "precip_type": "rain" if 'rain' in data else ("snow" if 'snow' in data else "no"),
            "precip_value": data['rain']['1h'] if 'rain' in data else (data['snow']['1h'] if 'snow' in data else 0),
            "update_time": datetime.now(),
            **data['wind'],
            **data['coord']
        }])


class OpenWeatherApiExtractorForecast(Extractor):
    def __init__(self, config):
        self.config = config  # Store the config as an instance variable
        self.tmp_df = self.config['TMP_DF']

    def run(self):
        id_pt, data = self.tmp_df[0]
        weather_list = data["list"]
        name = data["city"]["name"]
        country = data["city"]["country"]

        self.config['TMP_DF'] = pd.concat(
            [pd.DataFrame([{
                'id_geom': id_pt,
                'clouds': t['clouds']['all'],
                'visibility': t.get('visibility', 0),
                'country': country,
                'city': name,
                "precip_type": "rain" if 'rain' in t else ("snow" if 'snow' in t else "no"),
                "precip_value": t['rain']['3h'] if 'rain' in t else (t['snow']['3h'] if 'snow' in t else 0),
                "precip_probab": t["pop"],
                "update_time": datetime.fromtimestamp(t["dt"]),
                **t['main'],
                **t['wind'],
                **data['city']['coord'],
                **t['weather'][0],
            }]) for t in weather_list],
            ignore_index=True)


class IconEuExtractor(Extractor):
    """Extractor for ICON-EU GRIB2 files."""

    def __init__(self, config):
        self.output_folder: str = config["DOWNLOAD_FOLDER_ICON"]

    def run(self):

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

        except (IOError, OSError) as e:  # Corrected exception
            logger.error(f"Błąd podczas rozpakowywania pliku {file_path}: {e}", exc_info=True)
            return None


if __name__ == "__main__":
    pass
