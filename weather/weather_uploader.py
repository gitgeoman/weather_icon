import os

import geopandas as gpd
import sqlalchemy
from sqlalchemy import create_engine
from abc import ABC, abstractmethod

from pass_logging import logger

from dotenv import load_dotenv
import os

from pass_utils import connect_to_db

load_dotenv('../.env')

db_name: str = os.getenv('DB_NAME')
db_user: str = os.getenv('DB_USER')
db_password: str = os.getenv('DB_PASSWORD')
db_port: str = os.getenv('DB_PORT')
db_host: str = os.getenv('DB_HOST')


class Uploader(ABC):
    @abstractmethod
    def upload_data(self) -> None:
        """upload method for output"""
        ...


class OpenWeatherApiUploader(Uploader):
    def __init__(self, config):
        self.tmp_df = config["TMP_DF"]
        self.db_connection = connect_to_db(db_name, db_user, db_password, db_port, db_host)

    def upload_data(self) -> None:
        self.tmp_df.to_sql(name='weather_OW', schema='weather_ow', con=self.db_connection, if_exists='append',
                           index=False)


class IconEUDBUploader(Uploader):
    def __init__(self, config):
        self.output_folder = config["DOWNLOAD_FOLDER_ICON"]
        self.temp_folder = config["TMP_FOLDER"]

    """Uploader for database."""

    def upload_data(self) -> None:
        """Uploads data to database."""

        for file in [
            os.path.join(self.temp_folder, filename)
            for filename in os.listdir(self.temp_folder)
            if filename.endswith(".fgb")
        ]:
            gdf = gpd.read_file(file)
            engine = connect_to_db(db_name, db_user, db_password, db_port, db_host)
            logger.info(gdf.head())
            gdf.to_postgis(f"weather_data_v2", schema="weather_icon", con=engine, if_exists="append",
                           index=False)

            logger.info(f"Data successfully saved to database: {file}")


if __name__ == "__main__":
    pass
