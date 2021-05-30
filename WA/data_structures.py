import asyncio
import zipfile
from datetime import datetime, timedelta
from functools import partial
from multiprocessing import Pool
from typing import List, Tuple

import aiohttp
import pandas as pd
from geopy.exc import GeocoderServiceError
from geopy.geocoders import Nominatim
from keys import API_OW

pd.set_option("isplay.max_rows", None)
pd.set_option("display.max_columns", 10)
pd.set_option("display.width", 1600)

#  prep geolocator
geolocator = Nominatim(user_agent="nvm")
reverse_coords = partial(geolocator.reverse, language="en", timeout=5)

ow_url_forecast = "http://api.openweathermap.org/data/2.5/forecast"
ow_url_historical = "http://api.openweathermap.org/data/2.5/onecall/timemachine"


class Hotels:
    """Data structure with information about hotels.

    Data will be gathered from archived csv files from provided folder.

    Attributes:
        df (pd.DataFrame): Dataframe, formed from provided data.
    """

    def __init__(self, path: str):
        """Form main dataframe.

        Args:
            path (str): The path to `hotels.zip` file.
        """
        self.df = prepare_data(path)

    def fill_address(self, processes: int) -> None:
        """Fill addresses in given dataframe.

        Incorrect country and city data will be fixed at the process.

        Args:
            processes (int): Number of processes to run.
        """
        try:
            result = run_pool_of_address_workers(self.df, processes)
            self.df["Address"] = [item[0] for item in result]
            self.df["Country"] = [item[1] for item in result]
            self.df["City"] = [item[2] for item in result]
        except GeocoderServiceError:
            quit()

    def __str__(self):
        return str(self.df)


def prepare_data(base: str) -> pd.DataFrame:
    """Form a dataframe from csv data in `hotels.zip` file at given folder.

    Lines without one of the coordinates and
    lines with invalid coordinates (not numeric, latitude more than 90 or less
    than -90, longitude more than 180 or less than -180),
    will be skipped.

    Args:
        base (str): The path to `hotels.zip` file.

    Returns:
        Dataframe with valid coordinates.
    """
    #  read zip
    try:
        with zipfile.ZipFile(base + "\\hotels.zip") as myzip:
            files = [
                item.filename
                for item in myzip.infolist()
                if item.filename.endswith(".csv")
            ]
            if not files:
                quit()
            df = pd.concat([pd.read_csv(myzip.open(file)) for file in files])
    except FileNotFoundError:
        quit()

    # preprocess dataset
    try:
        df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
        df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
        df = df[(abs(df["Latitude"]) < 90) & (abs(df["Longitude"]) < 180)]

        df["Address"] = (
            df["Latitude"].astype("str") + ", " + df["Longitude"].astype("str")
        )
        df.reset_index(inplace=True)
        df.drop(["Id", "index"], axis=1, inplace=True)
    except KeyError:
        quit()
    return df  # [300:310]  # SHORTENED!!!!


def run_pool_of_address_workers(df: pd.DataFrame, processes: int) -> List:
    """Run address_worker method in the multiprocessing pool.

    Form a list of correct addresses, country codes and cities for given dataframe
    in multiprocessing mode.

    Args:
        df (pd.DataFrame): Dataframe to process.
        processes (int): Number of processes to run.

    Returns:
        List of tuples with address, county code and city for every line in dataframe.
    """
    with Pool(processes=processes) as pool:
        return pool.map(address_worker, zip(df["Address"], df["City"]))


def address_worker(data: Tuple) -> Tuple:
    """Get the address, country code and city for given coordinates.

    Args:
        data (Tuple): Tuple of coordinates concatenated as strings and original `City`

    Returns:
        Tuple with valid address, country code and city for given coordinates.
    """
    coordinates_as_string = data[0]
    original_city = data[1]
    location = reverse_coords(coordinates_as_string)
    country_code = location.raw["address"]["country_code"].upper()

    if "city" in location.raw["address"]:
        city = location.raw["address"]["city"]
    elif "town" in location.raw["address"]:
        city = location.raw["address"]["town"]
    elif "village" in location.raw["address"]:
        city = location.raw["address"]["village"]
    else:
        city = original_city
    return location.address, country_code, city


class CityCentres:
    """Data structure with information about every city centre.

    Data will be calculated from Hotels class object.

    Attributes:
        df (pd.DataFrame): Dataframe, formed from provided object.
    """

    def __init__(self, hotels: Hotels):
        """Form main dataframe with city centres from Hotels class object.

        Args:
            hotels (Hotels): Hotels class object.
        """
        self.df = calc_city_centres(hotels)

    def __str__(self):
        return str(self.df)


def calc_city_centres(hotels: Hotels) -> pd.DataFrame:
    """Form the dataframe with calculated coordinates for city centre.

    City centre coordinates are average of one maximum and one minimum latitude and longitude
    for the hotels in this city.

    Args:
        hotels (Hotels): Hotels class object.

    Returns:
        Dataframe grouped by Country and City with coordinates of city centre.
    """
    # find max and min city center coordinates
    city_group = hotels.df.groupby(["Country", "City"])
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
    # calculate average of max and min city centre coordinates
    city_centres["center_lat"] = (
        city_centres["min_lat"] + city_centres["max_lat"]
    ) * 0.5
    city_centres["center_lon"] = (
        city_centres["min_lon"] + city_centres["max_lon"]
    ) * 0.5
    return city_centres


class Weather:
    """Data structure with weather information for every city centre.

    Data will be gathered from CityCentres class object and `openweathermap.org`

    Attributes:
        df (pd.DataFrame): Dataframe with city and weather information.
    """

    def __init__(self, city_centres: CityCentres):
        """Form main dataframe with weather information for every city centre.

        Args:
            city_centres (CityCentres): CityCentres class object.
        """
        self.df = asyncio.run(get_weather(city_centres))

    def __str__(self):
        return str(self.df)


async def get_weather(city_centres: CityCentres) -> pd.DataFrame:
    """Collect 11 days weather data for every city centre.

    Weather data will be asynchronously gathered from `openweathermap.org`

    Args:
        city_centres (CityCentres): CityCentres class object

    Returns:
        Dataframe with city, day and temperature data.
    """
    tasks = []
    async with aiohttp.ClientSession() as session:
        for row in city_centres.df.itertuples():
            tasks.append(asyncio.create_task(get_historical_weather(session, row)))
            tasks.append(asyncio.create_task(get_forecast(session, row)))
        result = await asyncio.gather(*tasks)
        weather = [row for item in result for row in item]
        return pd.DataFrame(weather)


async def get_forecast(
    session: aiohttp.ClientSession, row: "pd.core.frame.Pandas"
) -> List:
    """Collect current weather and 5 days weather forecast.

    Args:
        session (aiohttp.ClientSession): Shared aiohttp.ClientSession.
        row (pd.core.frame.Pandas): Line from dataframe.

    Returns:
        List of dicts with city, day, and current, min, and max temperature.
    """
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
    """Collect 5 days historical weather data.

    By the limitations from `openweathermap.org` 5 separate requests have to be done.
    https://openweathermap.org/api/one-call-api#history

    Args:
        session (aiohttp.ClientSession): Shared aiohttp.ClientSession.
        row (pd.core.frame.Pandas): Line from dataframe.

    Returns:
        List of dicts with city, day, and current, min, and max temperature.
    """
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
