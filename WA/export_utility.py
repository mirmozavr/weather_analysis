import os
from typing import Tuple

import pandas as pd
from data_structures import Hotels, Weather
from matplotlib import pyplot as plt


def export_address_data(hotels: Hotels, output_folder: str) -> None:
    r"""Write hotels data from Hotels class object.

    Hotels data will be grouped by city and written to `CSV` files with 100 records or less.
    Files will be structured as followed `output_folder\country\city\`.

    Args:
        hotels (Hotels): Hotels class object.
        output_folder (str): The path to desired folder for data export.
    """
    grouped = hotels.df.groupby(["Country", "City"])
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


def save_plots(weather: Weather, output_folder: str) -> None:
    """Save min and max temperature diagram for every city in Weather class object.

    Args:
        weather (Weather): Weather class object.
        output_folder (str): The path to desired folder for data export.
    """
    grouped = weather.df.groupby(["city"])
    for label, group in grouped:
        country, city = label[0], label[1]
        file_path = (
            f"{output_folder}\\{country}\\{city}\\{country}_{city}_temp_plot.png"
        )
        save_plot(label, group, file_path)


def save_plot(label: Tuple, group: "pd.DataFrame", file_path: str) -> None:
    r"""Form and save the weather diagram for single city.

    Args:
        label (tuple): Country and city.
        group (pd.DataFrame): Dataframe with weather data for 11 days for single city.
        file_path (str): The path formed as `output_folder\country\city\country_city_temp_plot.png`
    """
    plt.xlabel("Day")
    plt.ylabel("Temperature")
    plt.title(f"{label}: daily min and max temperature")
    plt.grid(True, which="both")
    days = group["day"]
    temp_min = group["temp_min"]
    temp_max = group["temp_max"]
    plt.plot(days, temp_min, label="Daily min temp.", c="blue")
    plt.plot(days, temp_max, label="Daily max temp.", c="red")
    plt.legend()
    plt.savefig(file_path)
    plt.clf()
