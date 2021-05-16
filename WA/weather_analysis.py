import csv
import zipfile
from functools import partial
from io import TextIOWrapper
from multiprocessing import Pool, cpu_count

from geopy.geocoders import Nominatim

BASE = r"D:\Programms\WA\WA\data"
corrupted_data = []


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
reverse_coords = partial(geolocator.reverse, language="en", timeout=5)


def address_worker(row: list) -> list:
    location = reverse_coords(f"{row[-2]}, {row[-1]}")
    if not row[2]:
        row[2] = location.raw["address"]["country_code"].upper()
    if not row[3]:
        if "city" in location.raw["address"]:
            row[3] = location.raw["address"]["city"]
        elif "town" in location.raw["address"]:
            row[3] = location.raw["address"]["town"]
    row.append(location.address)
    return row


def run_pool_of_address_workers(hotels: list) -> list:
    with Pool(processes=cpu_count()) as pool:
        return pool.map(address_worker, hotels)


if __name__ == "__main__":
    main()
