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

    df = df[:10]
    df["Address"] = df["Latitude"].astype("str") + ", " + df["Longitude"].astype("str")
    df["Address"] = df["Address"].apply(address_worker)


#  prep geolocator
geolocator = Nominatim(user_agent="nvm")
reverse_coords = partial(geolocator.reverse, language="en", timeout=2)


def address_worker(coord_string: str):
    location = reverse_coords(coord_string)
    country_code = location.raw["address"]["country_code"].upper()

    if "city" in location.raw["address"]:
        city = location.raw["address"]["city"]
    elif "town" in location.raw["address"]:
        city = location.raw["address"]["town"]
    elif "village" in location.raw["address"]:
        city = location.raw["address"]["village"]
    else:
        city = None

    return location.address


# ?????????
def process(df):
    df.apply(address_worker, axis=1)


def run_pool_of_address_workers(hotels: list) -> list:
    with Pool(processes=cpu_count()) as pool:
        return pool.map(address_worker, hotels)


if __name__ == "__main__":
    main()
