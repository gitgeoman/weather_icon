import os
from datetime import datetime

import pygrib
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
import time

# Ścieżka do folderu z plikami GRIB2
output_folder = "./downloaded_files"

downloaded_files: list = [
    os.path.join(output_folder, filename)
    for filename in os.listdir(output_folder)
    if filename.endswith(".grib2")
]


# Funkcja do ekstrakcji danych z pliku GRIB2
def extract_grib_data(file_path):
    """
    Ekstraktuje dane z pliku GRIB2 dla każdego parametru i zwraca Pandas DataFrame
    z kolumnami: 'latitude', 'longitude' oraz kolumnę dla parametrów (np. temperatura, wiatr).
    """
    data_records = []

    # Otwórz plik GRIB
    with pygrib.open(file_path) as grbs:
        # Iteruj przez wszystkie wiadomości (messages) w pliku GRIB
        for grb in grbs:
            # Pobierz dane geograficzne i wartości
            lats, lons = grb.latlons()  # Pobierz szerokość i długość geograficzną
            values = grb.values  # Pobierz wartości parametrów (np. temperatury, wiatru)

            # Pobierz nazwę parametru (np. "temperature", "wind speed" itd.)
            parameter_name = grb.parameterName

            # Konwertuj dane geograficzne + dane parametru do rekordów
            for lat, lon, value in zip(lats.flatten(), lons.flatten(), values.flatten()):
                data_records.append({
                    "latitude": lat,
                    "longitude": lon,
                    parameter_name: value,  # dynamicznie dodajemy parametr jako nazwę kolumny
                    'update_on': datetime.strptime(
                        f"{grb.validityDate:08d}{grb.validityTime:04d}", "%Y%m%d%H%M"
                    )
                })

    # Konwertuj listę rekordów do Pandas DataFrame
    return pd.DataFrame(data_records)


# Połącz dane ze wszystkich plików GRIB2

task_start_time = time.time()
FORECAST_HOURS = ["000", "003",
                  "006"]  # , "009", "012", "015", "018", "021", "024", "027", "030", "033", "036", "039", "042", "048"]
for hour in FORECAST_HOURS:
    all_dataframes = []
    hour_start_time = time.time()
    for file_path in downloaded_files:
        if hour in file_path:

            print(f"Przetwarzanie pliku GRIB2: {file_path}")
            try:
                # Ekstraktuj dane z pliku GRIB do DataFrame
                df = extract_grib_data(file_path)
                all_dataframes.append(df)
            except Exception as e:
                print(f"Błąd przetwarzania pliku {file_path}: {e}")

    # Połącz wszystkie DataFrame w jeden
    combined_dataframe = pd.concat(all_dataframes, ignore_index=True)

    # Usuń duplikaty w przypadku łączenia tych samych współrzędnych z różnymi parametrami
    combined_dataframe = combined_dataframe.groupby(['latitude', 'longitude'], as_index=False).first()

    gdf = gpd.GeoDataFrame(
        combined_dataframe,
        geometry=[
            Point(lon, lat) for lon, lat in zip(combined_dataframe["longitude"], combined_dataframe["latitude"])
        ],
        crs="EPSG:4326"  # Standardowy układ współrzędnych WGS84
    )

    print(gdf.head())


    output_file = f"combined_grib_data_{hour}.fgb"
    gdf.to_file(output_file, driver="flatgeobuf")
    print(f"Połączone dane zapisano do pliku: {output_file}")
    hour_end_time = time.time()
    print(f"Time taken for forecast hour {hour}: {hour_end_time - hour_start_time:.2f} seconds\n")

task_end_time = time.time()
print(f"Total task duration: {task_end_time - task_start_time:.2f} seconds")
