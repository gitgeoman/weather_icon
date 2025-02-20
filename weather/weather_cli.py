import os
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import text

from pass_utils import connect_to_db
from weather.weather_factories import FactoryWeatherICONPolandToday, FactoryWeatherICONPolandForecast, \
    FactoryWeatherOWSUBREGIONToday, FactoryWeatherOWSUBREGIONForecast


load_dotenv('../.env')

db_name: str = os.getenv('DB_NAME')
db_user: str = os.getenv('DB_USER')
db_password: str = os.getenv('DB_PASSWORD')
db_port: str = os.getenv('DB_PORT')
db_host: str = os.getenv('DB_HOST')




def handle_icon_weather_today() -> None:
    handler = FactoryWeatherICONPolandToday()
    handler.process()


def handle_icon_weather_forecast() -> None:
    handler = FactoryWeatherICONPolandForecast()
    #TODO CLEANING TABLE
    engine = connect_to_db(db_name, db_user, db_password, db_port, db_host)
    with engine.connect() as conn:
        conn.execute(text(
        f"TRUNCATE TABLE weather_icon.icon_forecast;"))
        conn.commit()
    handler.process()


def handle_ow_weather_today() -> None:
    handler = FactoryWeatherOWSUBREGIONToday()
    handler.process()


def handle_ow_weather_forecast() -> None:
    handler = FactoryWeatherOWSUBREGIONForecast()
    engine = connect_to_db(db_name, db_user, db_password, db_port, db_host)
    with engine.connect() as conn:
        conn.execute(text(
        f"TRUNCATE TABLE weather_ow.ow_forecast;"))
        conn.commit()
    handler.process()


if __name__ == "__main__":
    handle_ow_weather_forecast()
