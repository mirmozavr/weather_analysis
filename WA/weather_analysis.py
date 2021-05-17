import zipfile
from functools import partial
from multiprocessing import Pool, cpu_count

import pandas as pd
from geopy.geocoders import Nominatim

pd.set_option("isplay.max_rows", None)
pd.set_option("display.max_columns", 10)
pd.set_option("display.width", 1600)

BASE = r"D:\Programms\WA\WA\data"
corrupted_data = []
city_center = {}


def main():
    #  preprocess data
    with zipfile.ZipFile(BASE + "\\hotels.zip") as myzip:
        files = [item.filename for item in myzip.infolist()]
        df = pd.concat(
            [pd.read_csv(myzip.open(file)) for file in files[:1]]
        )  # shortened

    # preprocess dataset
    df.drop(["Id"], axis=1, inplace=True)
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df = df[(abs(df["Latitude"]) < 90) & (abs(df["Longitude"]) < 180)]

    df = df[:10]  # !!!!!!!! shortened
    df["Address"] = df["Latitude"].astype("str") + ", " + df["Longitude"].astype("str")

    result = run_pool_of_address_workers(df)
    df["Address"] = [item[0] for item in result]
    df["Country"] = [item[1] for item in result]
    df["City"] = [item[2] for item in result]


#  prep geolocator
geolocator = Nominatim(user_agent="nvm")
reverse_coords = partial(geolocator.reverse, language="en", timeout=5)


def run_pool_of_address_workers(df):
    with Pool(processes=cpu_count()) as pool:
        return pool.map(address_worker, zip(df["Address"], df["City"]))


def address_worker(data: tuple):
    location = reverse_coords(data[0])
    country_code = location.raw["address"]["country_code"].upper()

    if "city" in location.raw["address"]:
        city = location.raw["address"]["city"]
    elif "town" in location.raw["address"]:
        city = location.raw["address"]["town"]
    elif "village" in location.raw["address"]:
        city = location.raw["address"]["village"]
    else:
        city = data[2]
    return location.address, country_code, city


if __name__ == "__main__":
    main()
