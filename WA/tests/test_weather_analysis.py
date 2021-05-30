import os

import pandas as pd

from WA.analysis_methods import (
    get_city_and_day_with_max_temp,
    get_city_and_day_with_min_temp,
    get_city_with_biggest_max_temp_change,
    get_max_daily_temp_change,
)
from WA.data_structures import CityCentres, Hotels

path = os.path.dirname(__file__)


def test_hotels_class_filters_data():
    hotels = Hotels(path + r"\test_hotels_class")
    correct_hotels = {
        "Name": ["Hotel Spa Villa Olimpica Suites"],
        "Country": ["ES"],
        "City": ["Barcelona"],
        "Latitude": [41.3971434],
        "Longitude": [2.1921947],
        "Address": ["41.3971434, 2.1921947"],
    }
    correct_hotels_df = pd.DataFrame(correct_hotels)
    assert hotels.df.equals(correct_hotels_df)


def test_calc_city_centres():
    hotels = Hotels(path + r"\test_calc_city_centres")
    centres = CityCentres(hotels)
    correct_centre_coordinates = {
        "min_lat": {("UK", "Sallisaw"): 30.0, ("US", "Coalville"): 20.0},
        "min_lon": {("UK", "Sallisaw"): -160.0, ("US", "Coalville"): -120.0},
        "max_lat": {("UK", "Sallisaw"): 50.0, ("US", "Coalville"): 80.0},
        "max_lon": {("UK", "Sallisaw"): -40.0, ("US", "Coalville"): -60.0},
        "center_lat": {("UK", "Sallisaw"): 40.0, ("US", "Coalville"): 50.0},
        "center_lon": {("UK", "Sallisaw"): -100.0, ("US", "Coalville"): -90.0},
    }
    correct_centre_coordinates_df = pd.DataFrame(correct_centre_coordinates)
    assert centres.df.equals(correct_centre_coordinates_df)


test_data = {
    "Unnamed: 0": [0, 1, 2, 3, 4, 5, 6, 7, 8],
    "city": [
        "('AT', 'Vienna')",
        "('AT', 'Vienna')",
        "('AT', 'Vienna')",
        "('FR', 'Paris')",
        "('FR', 'Paris')",
        "('FR', 'Paris')",
        "('IT', 'Milan')",
        "('IT', 'Milan')",
        "('IT', 'Milan')",
    ],
    "day": [
        "2021-05-25",
        "2021-05-26",
        "2021-05-27",
        "2021-05-25",
        "2021-05-26",
        "2021-05-27",
        "2021-05-25",
        "2021-05-26",
        "2021-05-27",
    ],
    "temp": [12.99, 18.66, 13.5, 13.12, 12.36, 15.08, 18.09, 17.6, 20.61],
    "temp_min": [6.58, 5.02, 11.13, 7.36, 9.47, 6.69, 12.2, 11.16, 12.05],
    "temp_max": [13.24, 20.75, 15.97, 14.84, 14.84, 19.53, 22.89, 22.24, 25.07],
}
weather_df = pd.DataFrame(test_data)


def test_get_city_and_day_with_max_temp():
    correct_series = pd.Series(
        {
            "Unnamed: 0": 8,
            "city": "('IT', 'Milan')",
            "day": "2021-05-27",
            "temp": 20.61,
            "temp_min": 12.05,
            "temp_max": 25.07,
        }
    )
    assert get_city_and_day_with_max_temp(weather_df).equals(correct_series)


def test_get_city_and_day_with_min_temp():
    correct_series = pd.Series(
        {
            "Unnamed: 0": 4,
            "city": "('FR', 'Paris')",
            "day": "2021-05-26",
            "temp": 12.36,
            "temp_min": 9.47,
            "temp_max": 14.84,
        }
    )

    assert get_city_and_day_with_min_temp(weather_df).equals(correct_series)


def test_get_max_daily_temp_change():
    correct_series = pd.Series(
        {
            "Unnamed: 0": 1,
            "city": "('AT', 'Vienna')",
            "day": "2021-05-26",
            "temp": 18.66,
            "temp_min": 5.02,
            "temp_max": 20.75,
            "day_temp_delta": 15.73,
        }
    )
    assert get_max_daily_temp_change(weather_df).equals(correct_series)


def test_get_city_with_biggest_max_temp_change():
    correct_series = pd.Series(
        {
            "city": "('AT', 'Vienna')",
            "max_temp_low": 13.24,
            "max_temp_high": 20.75,
            "max_temp_delta": 7.51,
        }
    )
    assert get_city_with_biggest_max_temp_change(weather_df).equals(correct_series)
