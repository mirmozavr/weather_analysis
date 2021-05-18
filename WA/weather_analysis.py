import asyncio
import zipfile
from datetime import datetime, timedelta
from functools import partial
from multiprocessing import Pool, cpu_count

import aiohttp
import pandas as pd
from geopy.geocoders import Nominatim
from keys import API_OW

pd.set_option("isplay.max_rows", None)
pd.set_option("display.max_columns", 10)
pd.set_option("display.width", 1600)

base_folder = r"D:\Programms\WA\WA\data"

ow_url_forecast = "http://api.openweathermap.org/data/2.5/forecast"
ow_url_historical = "http://api.openweathermap.org/data/2.5/onecall/timemachine"

#  prep geolocator
geolocator = Nominatim(user_agent="nvm")
reverse_coords = partial(geolocator.reverse, language="en", timeout=5)


def main():
    df = prepare_data(base_folder)

    # fill address and fix incorrect country code and city
    # result = run_pool_of_address_workers(df)
    # df["Address"] = [item[0] for item in result]
    # df["Country"] = [item[1] for item in result]
    # df["City"] = [item[2] for item in result]

    city_centres = calc_city_centres(df)

    weather = asyncio.run(get_weather(city_centres))
    # for _ in weather:
    #     print(_)
    weather_df = pd.DataFrame(weather)
    print(weather_df)


def prepare_data(base: str) -> pd.DataFrame:
    #  preprocess data
    with zipfile.ZipFile(base + "\\hotels.zip") as myzip:
        files = [item.filename for item in myzip.infolist()]
        df = pd.concat(
            [pd.read_csv(myzip.open(file)) for file in files[:1]]  # !!!!!!!! shortened
        )  # shortened

    # preprocess dataset
    df.drop(["Id"], axis=1, inplace=True)
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df = df[(abs(df["Latitude"]) < 90) & (abs(df["Longitude"]) < 180)]

    # df = df[:10]  # !!!!!!!! shortened
    df["Address"] = df["Latitude"].astype("str") + ", " + df["Longitude"].astype("str")
    return df


def run_pool_of_address_workers(df: pd.DataFrame):
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


def calc_city_centres(df: pd.DataFrame) -> pd.DataFrame:
    # find city center coordinates
    city_group = df.groupby(["Country", "City"])
    min_lat_and_lon = city_group.min()
    min_lat_and_lon.rename(
        columns={"Latitude": "min_lat", "Longitude": "min_lon"}, inplace=True
    )
    max_lat_and_lon = city_group.max()
    max_lat_and_lon.rename(
        columns={"Latitude": "max_lat", "Longitude": "max_lon"}, inplace=True
    )
    city_centres = pd.concat(
        [
            min_lat_and_lon.loc[:, ["min_lat", "min_lon"]],
            max_lat_and_lon.loc[:, ["max_lat", "max_lon"]],
        ],
        axis=1,
    )
    city_centres["center_lat"] = (
        city_centres["min_lat"] + city_centres["max_lat"]
    ) * 0.5
    city_centres["center_lon"] = (
        city_centres["min_lon"] + city_centres["max_lon"]
    ) * 0.5
    return city_centres


async def get_weather(city_centres):
    weather = []
    async with aiohttp.ClientSession() as session:
        for row in city_centres[:3].itertuples():  # !!shortened
            # print(row.Index, row.center_lat, row.center_lon)
            weather += await get_forecast(session, row)
            weather += await get_historical_weather(session, row)
            # print("***" * 10)
        return weather


async def get_forecast(session: aiohttp.ClientSession, row: "pd.core.frame.Pandas"):
    async with session.get(
        ow_url_forecast,
        params=[
            ("lat", row.center_lat),
            ("lon", row.center_lon),
            ("appid", API_OW),
            ("units", "metric"),
        ],
    ) as resp:
        city_weather = []
        forecast = await resp.json()
        for day in (forecast["list"][index] for index in (0, 8, 16, 24, 32, 39)):
            city_weather.append(
                {
                    "city": row.Index,
                    "day": day["dt"],
                    "temp": day["main"]["temp"],
                    "temp_min": day["main"]["temp_min"],
                    "temp_max": day["main"]["temp_max"],
                }
            )
        return city_weather


async def get_historical_weather(
    session: aiohttp.ClientSession, row: "pd.core.frame.Pandas"
):
    date_stamps = [
        int(datetime.timestamp(datetime.today() - timedelta(days=i)))
        for i in range(1, 6)
    ]
    city_weather = []
    for date in date_stamps:
        async with session.get(
            ow_url_historical,
            params=[
                ("lat", row.center_lat),
                ("lon", row.center_lon),
                ("dt", date),
                ("appid", API_OW),
                ("units", "metric"),
            ],
        ) as resp:
            forecast = await resp.json()
            temp = forecast["current"]["temp"]
            temp_min = min(item["temp"] for item in forecast["hourly"])
            temp_max = max(item["temp"] for item in forecast["hourly"])
            city_weather.append(
                {
                    "city": row.Index,
                    "day": forecast["current"]["dt"],
                    "temp": temp,
                    "temp_min": temp_min,
                    "temp_max": temp_max,
                }
            )
    return city_weather


if __name__ == "__main__":
    main()
