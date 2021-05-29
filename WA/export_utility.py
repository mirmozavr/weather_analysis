import os

from data_structures import Hotels


def export_address_data(hotels: Hotels, output_folder: str) -> None:
    r"""Write hotels data.

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
