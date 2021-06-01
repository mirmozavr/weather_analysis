import os

import click
from analysis_methods import analysis_tasks
from data_structures import CityCentres, Hotels, Weather
from export_utility import export_address_data, save_plots


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
    have following structure: `output_folder\country\city\`
    """
    hotels = Hotels(input_folder)

    hotels.fill_address(processes)

    export_address_data(hotels, output_folder)

    city_centres = CityCentres(hotels)

    weather = Weather(city_centres)

    analysis_tasks(weather, output_folder)

    save_plots(weather, output_folder)


if __name__ == "__main__":
    main()
