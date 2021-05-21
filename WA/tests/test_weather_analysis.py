import os

import pandas as pd

from WA.weather_analysis import calc_city_centres

path = os.path.dirname(__file__)


def test_calc_city_centres():
    df = pd.read_csv(path + "\\test_calc_city_centres.csv")
    centres_df = calc_city_centres(df)

    correct_centre_coordinates = {
        "min_lat": {("UK", "Sallisaw"): 30.0, ("US", "Coalville"): 20.0},
        "min_lon": {("UK", "Sallisaw"): -160.0, ("US", "Coalville"): -120.0},
        "max_lat": {("UK", "Sallisaw"): 50.0, ("US", "Coalville"): 80.0},
        "max_lon": {("UK", "Sallisaw"): -40.0, ("US", "Coalville"): -60.0},
        "center_lat": {("UK", "Sallisaw"): 40.0, ("US", "Coalville"): 50.0},
        "center_lon": {("UK", "Sallisaw"): -100.0, ("US", "Coalville"): -90.0},
    }
    correct_centre_coordinates_df = pd.DataFrame(correct_centre_coordinates)
    assert centres_df.equals(correct_centre_coordinates_df)
