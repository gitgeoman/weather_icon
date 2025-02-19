from __future__ import absolute_import, unicode_literals
import concurrent.futures
import os
import sqlalchemy
import random
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData, select, and_  # text
from geoalchemy2 import functions
from shapely import wkt

from typing import Callable
from pass_logging import logger


def make_parallel(
        func: Callable, items: list, workers: int = os.cpu_count() * 2, **kwargs: object
) -> list:
    """przetwarzanie listy  z ThreadPoolExecutor"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(func, i, **kwargs) for i in items]
        return [f.result() for f in concurrent.futures.as_completed(futures)]


def connect_to_db(db_name: str, db_user: str, db_password: str, db_port: str, db_host: str) -> sqlalchemy.Engine:
    return create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")


class RandomKeyPicker:
    def __init__(self, keys):
        self.keys = keys
        self.counts = {key: 0 for key in keys}

    def pick_random_key(self) -> str:
        min_count = min(self.counts.values())
        least_chosen_key = [key for key, count in self.counts.items() if count == min_count]

        chosen_key = random.choice(least_chosen_key)
        self.counts[chosen_key] += 1

        return chosen_key



def get_centroids(
        db_connection: sqlalchemy.engine.base.Engine,
        grid_table: str = "poland_5km",
        grid_table_schema: str = 'weather'
) -> gpd.GeoDataFrame:
    logger.info(f"...getting centroids from grid table {grid_table} starts")
    metadata = MetaData()
    db_grid_table = Table(grid_table, metadata, schema=grid_table_schema,
                          autoload_with=db_connection)

    connection = db_connection.connect()

    db_aoi_table = Table('wojewodztwa', metadata, schema=grid_table_schema,
                         autoload_with=db_connection)

    query = select(
        db_grid_table.c.id,
        functions.ST_AsText(functions.ST_Centroid(db_grid_table.c.geom)).label('geom')
    ).where(and_(
        db_aoi_table.c.id.in_([12, 4]),
    )
    ).select_from(
        db_grid_table.join(db_aoi_table, functions.ST_Intersects(db_grid_table.c.geom, db_aoi_table.c.geom))
    )

    result = connection.execute(query)
    connection.close()

    df = pd.DataFrame(result, columns=result.keys())
    df['geom'] = df['geom'].apply(wkt.loads)

    logger.info(f"...getting centroids from grid ends")
    return gpd.GeoDataFrame(df, geometry='geom')
