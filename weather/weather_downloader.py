import os
from abc import ABC, abstractmethod

from datetime import datetime, timezone
import pandas as pd
import requests

from pass_logging import logger
from pass_utils import make_parallel


class Downloader(ABC):
    @abstractmethod
    def get_data(self) -> list:
        """Download method varies depending on weather source"""
        ...


class IconEuApiDownloader(Downloader):
    """Downloader for ICON-EU weather model data."""
    # config
    DATE = datetime.now(timezone.utc).strftime("%Y%m%d")
    LEVELS_T_SO = [0, ]  # 2, 5, 6, 18, 54, 162]
    LEVELS_W_SO = [0, ]  # 1, 3, 9, 27, 81, 243]
    FORECAST_HOUR: str = "00"  # Forecast hour ("00", "06", "12", "18" are typical values)
    FORECAST_HOURS = ["000", "003", "006"] # TODO comment after tests
    # FORECAST_HOURS = ["000", "003", "006", "009", "012", "015", "018", "021", "024", "027", "030", "033", "036", "039",
    # "042", "048"] # TODO uncomment after tests
    BASE_URL = "https://opendata.dwd.de/weather/nwp/icon-eu/grib"
    DOWNLOAD_FOLDER_ICON = "./file_paths"

    def get_data(self) -> list:

        # Generate ICON-EU file URLs
        links: list = self.generate_icon_links(
            date=self.DATE,
            hour=self.FORECAST_HOUR,
            levels_t_so=self.LEVELS_T_SO,
            levels_w_so=self.LEVELS_W_SO,
            forecast_hours=self.FORECAST_HOURS,
            base_url=self.BASE_URL,
        )

        # Create output directory if it doesn't exist
        os.makedirs(self.DOWNLOAD_FOLDER_ICON, exist_ok=True)

        # Download files in parallel
        logger.info(f"Downloading {len(links)} files...")
        make_parallel(
            func=self.get_single_file,  # Uses method from provided code
            items=links,
        )
        logger.info("Downloading completed!")


    def get_single_file(self, url):
        """
        Pobiera plik z danego URL i zapisuje go w folderze docelowym.
        Pominięcie pobierania, jeśli plik już istnieje.

        :param url: URL pliku do pobrania.
        :param download_folder: Ścieżka do folderu, gdzie plik ma zostać zapisany.
        """
        try:
            filename = url.split("/")[-1]
            file_path = os.path.join(self.DOWNLOAD_FOLDER_ICON, filename)

            os.makedirs(self.DOWNLOAD_FOLDER_ICON, exist_ok=True)

            if os.path.exists(file_path):
                logger.info(f"Plik już istnieje, pomijam pobieranie: {filename}")
                return

            response = requests.get(url, stream=True)
            response.raise_for_status()  # Wyjątek w razie błędu HTTP

            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            logger.info(f"Pobrano: {filename}")

        except requests.RequestException as e:
            logger.info(f"Nie można pobrać pliku z {url}: {e}")

    @staticmethod
    def generate_icon_links(date, hour, levels_t_so, levels_w_so, forecast_hours, base_url):
        links = []

        # - `"t_so"`: Reprezentuje dane dotyczące temperatury profilu gleby.
        # - `"w_so"`: Dane dotyczące wilgotności profilu gleby.
        # - `"t_2m"`: Temperatura na wysokości 2 metrów nad powierzchnią ziemi.
        # - `"tot_prec"`: Całkowita suma opadów.

        # Słownik mapujący typ danych na odpowiednie poziomy i format linków
        file_patterns = {
            "t_so": {"levels": levels_t_so, "path": "soil-level", "suffix": "T_SO"},
            "w_so": {"levels": levels_w_so, "path": "soil-level", "suffix": "W_SO"},
            "t_2m": {"levels": [None], "path": "single-level", "suffix": "T_2M"},
            "tot_prec": {"levels": [None], "path": "single-level", "suffix": "TOT_PREC"}
        }

        for file_type, details in file_patterns.items():
            for fff in forecast_hours:
                for level in details["levels"]:
                    level_part = f"_{level}" if level is not None else ""
                    link = (
                        f"{base_url}/{hour}/{file_type}/icon-eu_europe_regular-lat-lon_"
                        f"{details['path']}_{date}{hour}_{fff}{level_part}_{details['suffix']}.grib2.bz2"
                    )
                    links.append(link)

        return links


if __name__ == "__main__":
    # Use the downloader
    downloader = IconEuApiDownloader()
    downloaded_files = downloader.get_data()
