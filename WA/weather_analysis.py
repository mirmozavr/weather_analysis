import asyncio
import os
import zipfile
from datetime import datetime, timedelta
from functools import partial
from multiprocessing import Pool
from typing import List, Tuple

import aiohttp
import click
import pandas as pd
from geopy.geocoders import Nominatim
from keys import API_OW
from matplotlib import pyplot as plt

pd.set_option("isplay.max_rows", None)
pd.set_option("display.max_columns", 10)
pd.set_option("display.width", 1600)


ow_url_forecast = "http://api.openweathermap.org/data/2.5/forecast"
ow_url_historical = "http://api.openweathermap.org/data/2.5/onecall/timemachine"

#  prep geolocator
geolocator = Nominatim(user_agent="nvm")
reverse_coords = partial(geolocator.reverse, language="en", timeout=5)


@click.command()
@click.option(
    "--input-folder",
    "-if",
    default=lambda: os.getcwd(),
    help="Enter a path to 'Hotels.zip'. Current working directory is used by default",
)
@click.option(
    "--output-folder",
    "-of",
    default=lambda: os.getcwd(),
    help="Enter a path to the output data. Current working directory is used by default",
)
@click.option(
    "--processes",
    "-p",
    type=int,
    default=lambda: os.cpu_count(),
    help="Number of processes to run",
)
def main(input_folder, output_folder, processes):
    r"""Weather analysis.

    The purpose of this programm is to process provided data (`hotels.zip`),
    clear corrupted data, add full addresses using `geopy` module,
    calculate coordinates for city centres and gather weather data
    for 11 days period from openweathermap.org,
    calculate: hottest day and city, coldest day and city,
    city with largest max temperature change, city and day with largest
    min and max temperature change, draw plots for max and min temperatures for
    every city centre.
    All gathered and calculated data will be saved at the output folder and will
    have following structure: `{output_folder}\{country}\{city}\`


    """
    df = prepare_data(input_folder)
    df.drop(["Id", "index"], axis=1, inplace=True)
    df = df[:10]  # !!!!!!!! shortened

    # fill address and fix incorrect country code and city
    result = run_pool_of_address_workers(df, processes)
    df["Address"] = [item[0] for item in result]
    df["Country"] = [item[1] for item in result]
    df["City"] = [item[2] for item in result]

    export_address_data(df, output_folder)

    city_centres = calc_city_centres(df)

    weather = asyncio.run(get_weather(city_centres))

    analysis_tasks(weather, output_folder)

    save_plots(weather, output_folder)


def prepare_data(base: str) -> pd.DataFrame:
    #  read zip
    with zipfile.ZipFile(base + "\\hotels.zip") as myzip:
        files = [item.filename for item in myzip.infolist()]
        df = pd.concat(
            [pd.read_csv(myzip.open(file)) for file in files[:1]]  # !!!!!!!! shortened
        )

    # preprocess dataset
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df = df[(abs(df["Latitude"]) < 90) & (abs(df["Longitude"]) < 180)]

    df["Address"] = df["Latitude"].astype("str") + ", " + df["Longitude"].astype("str")
    df.reset_index(inplace=True)
    return df


def run_pool_of_address_workers(df: pd.DataFrame, processes):
    with Pool(processes=processes) as pool:
        return pool.map(address_worker, zip(df["Address"], df["City"]))


def address_worker(data: Tuple) -> Tuple:
    location = reverse_coords(data[0])
    country_code = location.raw["address"]["country_code"].upper()

    if "city" in location.raw["address"]:
        city = location.raw["address"]["city"]
    elif "town" in location.raw["address"]:
        city = location.raw["address"]["town"]
    elif "village" in location.raw["address"]:
        city = location.raw["address"]["village"]
    else:
        city = data[1]
    return location.address, country_code, city


def export_address_data(df: pd.DataFrame, output_folder: str) -> None:
    grouped = df.groupby(["Country", "City"])
    chunk_size = 100
    for label, group in grouped:
        country, city = label[0], label[1]
        path = f"{output_folder}\\{country}\\{city}"
        os.makedirs(path, exist_ok=True)

        list_of_chunks = (
            group.iloc[i : i + chunk_size] for i in range(0, len(group), chunk_size)
        )
        for num, chunk in enumerate(list_of_chunks):
            file_name = f"{path}\\{country}_{city}_hotels_p{num:03d}.csv"
            chunk.to_csv(
                path_or_buf=file_name,
                columns=["Name", "Country", "City", "Address", "Latitude", "Longitude"],
            )


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


async def get_weather(city_centres: pd.DataFrame) -> pd.DataFrame:
    weather = []
    async with aiohttp.ClientSession() as session:
        for row in city_centres[:3].itertuples():  # !!shortened
            weather += await get_historical_weather(session, row)
            weather += await get_forecast(session, row)
        return pd.DataFrame(weather)


async def get_forecast(
    session: aiohttp.ClientSession, row: "pd.core.frame.Pandas"
) -> List:
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
        for item in (forecast["list"][index] for index in (0, 8, 16, 24, 32, 39)):
            city_weather.append(
                {
                    "city": row.Index,
                    "day": datetime.fromtimestamp(item["dt"]).date(),
                    "temp": item["main"]["temp"],
                    "temp_min": item["main"]["temp_min"],
                    "temp_max": item["main"]["temp_max"],
                }
            )
        return city_weather


async def get_historical_weather(
    session: aiohttp.ClientSession, row: "pd.core.frame.Pandas"
) -> List:
    date_stamps = [
        int(datetime.timestamp(datetime.today() - timedelta(days=i)))
        for i in range(5, 0, -1)
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
                    "day": datetime.fromtimestamp(forecast["current"]["dt"]).date(),
                    "temp": temp,
                    "temp_min": temp_min,
                    "temp_max": temp_max,
                }
            )
    return city_weather


def analysis_tasks(weather_df: pd.DataFrame, output_folder: str):
    # city/day with max and min temp
    temp_column = weather_df["temp"]
    min_temp_index = temp_column.idxmin()
    max_temp_index = temp_column.idxmax()
    weather_df.loc[min_temp_index].to_csv(
        path_or_buf=(output_folder + r"\coldest_city_and_day.csv")
    )
    weather_df.loc[max_temp_index].to_csv(
        path_or_buf=(output_folder + r"\hottest_city_and_day.csv")
    )

    # city with max temp change during the day
    weather_df["day_temp_delta"] = weather_df["temp_max"] - weather_df["temp_min"]
    max_change_of_day_temp_index = weather_df["day_temp_delta"].idxmax()
    weather_df.loc[max_change_of_day_temp_index].to_csv(
        path_or_buf=(output_folder + r"\biggest_daily_temp_change_city_and_day.csv")
    )

    # city with biggest max temp change
    grouped = weather_df.groupby(["city"])

    max_temps = grouped.agg(
        max_temp_low=("temp_max", "min"),
        max_temp_high=("temp_max", "max"),
    )
    max_temps["max_temp_delta"] = max_temps["max_temp_high"] - max_temps["max_temp_low"]
    max_temps.reset_index(inplace=True)
    max_temp_delta_index = max_temps["max_temp_delta"].idxmax()
    max_temps.loc[max_temp_delta_index].to_csv(
        path_or_buf=(output_folder + r"\biggest_max_temp_change_city_and_day.csv")
    )


def save_plots(weather: pd.DataFrame, output_folder: str) -> None:
    grouped = weather.groupby(["city"])
    for label, group in grouped:
        country, city = label[0], label[1]
        file_path = (
            f"{output_folder}\\{country}\\{city}\\{country}_{city}_temp_plot.png"
        )
        save_plot(label, group, file_path)


def save_plot(label: Tuple, group: pd.DataFrame, file_path: str) -> None:
    plt.xlabel("Day")
    plt.ylabel("Temperature")
    plt.title(f"{label}: daily high and low max temperature")
    plt.grid(True, which="both")
    days = group["day"]
    temp_min = group["temp_min"]
    temp_max = group["temp_max"]
    plt.plot(days, temp_min, label="Daily min temp.", c="blue")
    plt.plot(days, temp_max, label="Daily max temp.", c="red")
    plt.legend()
    plt.savefig(file_path)
    plt.clf()


if __name__ == "__main__":
    main()
