  ## Weather analysis.

The purpose of this programm is to process provided data, clear corrupted
data, add full addresses using `geopy` module, calculate coordinates for
city centres and gather weather data for 11 days period from
openweathermap.org, calculate:

 - city and observation day with the maximum temperature for the period under review;
 - city with maximum change in maximum temperature;
 - city and day of observation with minimal temperature for the period under review;
 - city and day with a maximum difference between the maximum and minimum temperature.

draw plots for max and min temperatures for every
  city centre. All gathered and calculated data will be saved at the output
  folder and will have following structure: `output_folder\country\city\`

###Options:
  * -if, --input-folder TEXT:   Enter a path to 'Hotels.zip'. Current working
                             directory is used by default
  * -of, --output-folder TEXT:  Enter a path to the output data. Current working
                             directory is used by default
  * -p, --processes INTEGER:    Number of processes to run

  * --help                     Show this message and exit.

### Requirements
Requirements for interpreter are in `requirements-def.txt` file.
Processed archive must be `hotels.zip`.
Archived `CSV` files must have following columns: Id, Name, Country, City, Latitude, Longitude.

### Implementation for Windows
To run script simply type command: `python weather_analysis.py` Default parameters will be used.
To specify input folder, output folder and number of processes type:
`python weather_analysis.py -if '\your\desired\input_folder' -of '\your\desired\output_folder' -p 4`

### Testing
Tests are prepared with Pytest module. To run tests type `pytest` at the command line. More information at  [docs.pytest.org](https://docs.pytest.org)
To run tests and get coverage report type `pytest --cov=WA --cov-report=html
`.
