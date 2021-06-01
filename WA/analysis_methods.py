import pandas as pd
from data_structures import Weather


def analysis_tasks(weather: Weather, output_folder: str) -> None:
    """Post processing analysis.

    Calculate:
    - city and observation day with the maximum temperature for the period under review;
    - city and day of observation with minimal temperature for the period under review;
    - city and day with a maximum difference between the maximum and minimum temperature.
    - city with maximum change in maximum temperature;

    Args:
        weather (Weather): Weather class object.
        output_folder (str): The path to desired folder for data export.
    """
    weather_df = weather.df
    # city/day with max and min temp
    get_city_and_day_with_min_temp(weather_df).to_csv(
        path_or_buf=(output_folder + r"\coldest_city_and_day.csv")
    )
    get_city_and_day_with_max_temp(weather_df).to_csv(
        path_or_buf=(output_folder + r"\hottest_city_and_day.csv")
    )

    # city with max temp change during the day
    get_max_daily_temp_change(weather_df).to_csv(
        path_or_buf=(output_folder + r"\biggest_daily_temp_change_city_and_day.csv")
    )

    # city with biggest max temp change
    get_city_with_biggest_max_temp_change(weather_df).to_csv(
        path_or_buf=(output_folder + r"\biggest_max_temp_change_city_and_day.csv")
    )


def get_city_and_day_with_max_temp(weather_df: pd.DataFrame):
    """Post processing analysis task.

    Calculates:
        - city and observation day with the maximum temperature for the period under review.

    Args:
        weather_df (pd.DataFrame) Dataframe from Weather class object.

    Returns:
        Dataframe with single city/day data.
    """
    temp_column = weather_df["temp"]
    max_temp_index = temp_column.idxmax()
    return weather_df.loc[max_temp_index]


def get_city_and_day_with_min_temp(weather_df: pd.DataFrame):
    """Post processing analysis task.

    Calculates:
        - city and day of observation with minimal temperature for the period under review.

    Args:
        weather_df (pd.DataFrame) Dataframe from Weather class object.

    Returns:
        Dataframe with single city/day data.
    """
    temp_column = weather_df["temp"]
    min_temp_index = temp_column.idxmin()
    return weather_df.loc[min_temp_index]


def get_max_daily_temp_change(weather_df: pd.DataFrame):
    """Post processing analysis task.

    Calculates:
        - city and day with a maximum difference between the maximum and minimum temperature.

    Args:
        weather_df (pd.DataFrame) Dataframe from Weather class object.

    Returns:
        Dataframe with single city/day data.
    """
    weather_df["day_temp_delta"] = weather_df["temp_max"] - weather_df["temp_min"]
    max_change_of_day_temp_index = weather_df["day_temp_delta"].idxmax()
    return weather_df.loc[max_change_of_day_temp_index]


def get_city_with_biggest_max_temp_change(weather_df: pd.DataFrame):
    """Post processing analysis task.

    Calculates:
        - city with maximum change in maximum temperature.

    Args:
        weather_df (pd.DataFrame) Dataframe from Weather class object.

    Returns:
        Dataframe with single city/day data.
    """
    grouped = weather_df.groupby(["city"])

    max_temps = grouped.agg(
        max_temp_low=("temp_max", "min"),
        max_temp_high=("temp_max", "max"),
    )
    max_temps["max_temp_delta"] = max_temps["max_temp_high"] - max_temps["max_temp_low"]
    max_temps.reset_index(inplace=True)
    max_temp_delta_index = max_temps["max_temp_delta"].idxmax()
    return max_temps.loc[max_temp_delta_index]
