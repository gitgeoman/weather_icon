import os
import geopandas as gpd

from abc import ABC, abstractmethod
from dotenv import load_dotenv

from pass_logging import logger
from pass_utils import connect_to_db

load_dotenv('../.env')

db_name: str = os.getenv('DB_NAME')
db_user: str = os.getenv('DB_USER')
db_password: str = os.getenv('DB_PASSWORD')
db_port: str = os.getenv('DB_PORT')
db_host: str = os.getenv('DB_HOST')


class Uploader(ABC):
    @abstractmethod
    def run(self) -> None:
        """upload method for db"""
        ...


class OpenWeatherApiUploader(Uploader):
    def __init__(self, config):
        self.config = config # ć inaczej błąd referencji (zrywa referencje) !!
        self.table_name = config['URL_ELEM']
        self.db_connection = connect_to_db(db_name, db_user, db_password, db_port, db_host)

    def run(self) -> None:
        self.config['TMP_DF'].to_sql(
            name=f'OW_{self.table_name}',
            schema='weather_ow',
            con=self.db_connection,
            if_exists='append',
            index=False
        )


class IconEUDBUploader(Uploader):
    def __init__(self, config):
        self.table_name = config['TABLE_NAME']
        self.output_folder = config["DOWNLOAD_FOLDER_ICON"]
        self.temp_folder = f'{config["TMP_FOLDER"]}/{config["DATE"]}'

    """Uploader for database."""

    def run(self) -> None:
        """Uploads data to database."""

        for file in [
            os.path.join(self.temp_folder, filename)
            for filename in os.listdir(self.temp_folder)
            if filename.endswith(".fgb")
        ]:
            gdf = gpd.read_file(file)
            gdf.insert(0, 'pt_id', range(1, len(gdf) + 1))
            engine = connect_to_db(db_name, db_user, db_password, db_port, db_host)
            logger.info(gdf.head())
            gdf.to_postgis(
                name=f"ICON_{self.table_name}",
                schema="weather_icon",
                con=engine,
                if_exists="append",
                index=False
            )

            logger.info(f"Data successfully saved to database: {file}")


if __name__ == "__main__":
    pass
