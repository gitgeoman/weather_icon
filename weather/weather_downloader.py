import os

import requests
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from pass_logging import logger
from pass_utils import make_parallel, RandomKeyPicker, get_centroids, connect_to_db

load_dotenv('../.env')

db_name: str = os.getenv('DB_NAME')
db_user: str = os.getenv('DB_USER')
db_password: str = os.getenv('DB_PASSWORD')
db_port: str = os.getenv('DB_PORT')
db_host: str = os.getenv('DB_HOST')


def load_api_keys(prefix: str = "WEATHER_API_KEY_", count: int = 12) -> list:
    return [os.getenv(f"{prefix}{i}") for i in range(1, count + 1)]

WEATHER_API_KEYS = load_api_keys()



class Downloader(ABC):
    @abstractmethod
    def run(self) -> list:
        """Download method varies depending on weather source"""
        ...


class OpenWeatherApiDownloader(Downloader):
    db_connection = connect_to_db(db_name, db_user, db_password, db_port, db_host)

    def __init__(self, config):
        self.picker = RandomKeyPicker(config["API_KEYS"])
        self.url_elem: str = config["URL_ELEM"]
        self.tmp_df = config["TMP_DF"]

    def run(self):
        self.tmp_df.extend(
            make_parallel(
                func=self.get_single_coords,
                items=[(row.id, row.geom) for index, row in get_centroids(self.db_connection).head(2).iterrows()],
                # TODO UNLOCK FOR EVERY ITEM
                url_elem=self.url_elem
            )
        )

    def get_single_coords(self, id_geom, url_elem):
        id_pt, geom = id_geom
        url: str = f'https://api.openweathermap.org/data/2.5/{url_elem}?lat={geom.y}&lon={geom.x}&appid={self.picker.pick_random_key()}&units=metric'

        try:
            # logger.info(f"...requesting data {url}")
            return [id_pt, requests.get(url).json()]
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)


class IconEuApiDownloader(Downloader):
    """Downloader for ICON-EU weather model data.
        # - `"t_so"`: Reprezentuje dane dotyczące temperatury profilu gleby.
        # - `"w_so"`: Dane dotyczące wilgotności profilu gleby.
        # - `"t_2m"`: Temperatura na wysokości 2 metrów nad powierzchnią ziemi.
        # - `"tot_prec"`: Całkowita suma opadów.
    """

    def __init__(self, config):
        self.day = config["DATE"]
        self.LEVELS_T_SO = config["LEVELS_T_SO"]
        self.LEVELS_W_SO = config["LEVELS_W_SO"]
        self.FORECAST_HOUR = config["FORECAST_HOUR"]
        self.FORECAST_HOURS = config["FORECAST_HOURS"]
        self.BASE_URL = config["BASE_URL"]
        self.DOWNLOAD_FOLDER_ICON = config["DOWNLOAD_FOLDER_ICON"]

    def run(self):

        links: list = self.generate_icon_links(
            date=self.day,
            hour=self.FORECAST_HOUR,
            levels_t_so=self.LEVELS_T_SO,
            levels_w_so=self.LEVELS_W_SO,
            forecast_hours=self.FORECAST_HOURS,
            base_url=self.BASE_URL,
        )

        os.makedirs(f'{self.DOWNLOAD_FOLDER_ICON}/{self.day}', exist_ok=True)

        logger.info(f"Downloading {len(links)} files...")
        make_parallel(
            func=self.get_single_file,  # Uses method from provided code
            items=links,
        )
        logger.info("Downloading completed!")

    def get_single_file(self, url):
        try:
            filename = url.split("/")[-1]
            file_path = os.path.join(f'{self.DOWNLOAD_FOLDER_ICON}/{self.day}', filename)

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
    pass
