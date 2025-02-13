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


def connect_to_db(db_name: str, db_user: str, db_password: str, db_host: str) -> sqlalchemy.Engine:
    return create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}")
