import csv
import zipfile
from functools import partial
from io import TextIOWrapper
from multiprocessing import Pool, cpu_count

from geopy.geocoders import Nominatim

BASE = r"D:\Programms\WA\WA\data"
corrupted_data = []
city_center = {}


def main():
    #  preprocess data
    data = []
    with zipfile.ZipFile(BASE + "\\hotels.zip") as myzip:
        files = [item.filename for item in myzip.infolist()]
        for file in files:
            with myzip.open(file) as csvfile:
                reader = csv.reader(TextIOWrapper(csvfile))
                data.extend(
                    item
                    for item in reader
                    if (all([item[1], item[4], item[5]]) and item[0] != "Id")
                )
    validate_hotels_data(data)
    #  data is ready here
    run_pool_of_address_workers(data[:10])  # shortened dataset


def validate_hotels_data(data: list) -> None:
    for row in data:
        try:
            row[-2] = float(row[-2])
            row[-1] = float(row[-1])
        except ValueError:
            erase_corrupted_hotel(data, row)
            break
        if abs(row[-2]) >= 90 or abs(row[-1]) >= 180:
            erase_corrupted_hotel(data, row)


def erase_corrupted_hotel(data, row):
    corrupted_data.append(row)
    data.remove(row)


#  prep geolocator
geolocator = Nominatim(user_agent="nvm")
reverse_coords = partial(geolocator.reverse, language="en", timeout=2)


def address_worker(row: list) -> list:
    location = reverse_coords(f"{row[4]}, {row[5]}")
    country_code = location.raw["address"]["country_code"].upper()

    if "city" in location.raw["address"]:
        city = location.raw["address"]["city"]
    elif "town" in location.raw["address"]:
        city = location.raw["address"]["town"]
    elif "village" in location.raw["address"]:
        city = location.raw["address"]["village"]
    else:
        city = None

    if row[2] != country_code:
        row[2] = country_code
    if row[3] != city and city is not None:
        row[3] = city

    row.append(location.address)

    get_city_center(row)
    return row


def get_city_center(row):
    country = row[2]
    city = row[3]
    latitude = row[4]
    longitude = row[5]

    if country not in city_center:
        city_center[country] = {}

    if city not in city_center[country]:
        city_center[country][city] = {
            "min_lat": latitude,
            "max_lat": latitude,
            "min_lon": longitude,
            "max_lon": longitude,
        }
    else:
        city_center[country][city]["min_lat"] = min(
            city_center[country][city]["min_lat"], latitude
        )
        city_center[country][city]["max_lat"] = max(
            city_center[country][city]["max_lat"], latitude
        )
        city_center[country][city]["min_lon"] = min(
            city_center[country][city]["min_lon"], longitude
        )
        city_center[country][city]["max_lon"] = max(
            city_center[country][city]["max_lon"], longitude
        )


def run_pool_of_address_workers(hotels: list) -> list:
    with Pool(processes=cpu_count()) as pool:
        return pool.map(address_worker, hotels)


if __name__ == "__main__":
    main()
