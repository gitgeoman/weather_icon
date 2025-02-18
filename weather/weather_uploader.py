import os

import geopandas as gpd
import sqlalchemy
from sqlalchemy import create_engine, Table, MetaData, Column
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape, to_shape
from shapely.geometry import mapping
from abc import ABC, abstractmethod
from shapely.geometry import Point

from pass_logging import logger

db_name: str = ''
db_user: str = ''
db_password: str = ''
db_port: str = ''
db_host: str = ""


def connect_to_db(db_name: str, db_user: str, db_password: str, db_port: str, db_host: str) -> sqlalchemy.Engine:
    return create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")


class Uploader(ABC):
    @abstractmethod
    def upload_data(self) -> None:
        """upload method for output"""
        ...


class IconEUDBUploader(Uploader):
    temp_folder = "./tmp"

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
            print(gdf.head())
            gdf.to_postgis(f"weather_data_v2", schema="weather_icon", con=engine, if_exists="append",
                           index=False)

            logger.info(f"Data successfully saved to database: {file}")


if __name__ == "__main__":
    uploader = IconEUDBUploader()
    uploader.upload_data()
