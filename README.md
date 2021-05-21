  ## Weather analysis.

  The purpose of this programm is to process provided data, clear corrupted
  data, add full addresses using `geopy` module, calculate coordinates for
  city centres and gather weather data for 11 days period from
  openweathermap.org, calculate: hottest day and city, coldest day and city,
  city with largest max temperature change, city and day with largest min and
  max temperature change, draw plots for max and min temperatures for every
  city centre. All gathered and calculated data will be saved at the output
  folder and will have following structure:
  `{output_folder}\{country}\{city}\`

Options:
  * -if, --input-folder TEXT   Enter a path to 'Hotels.zip'. Current working
                             directory is used by default
  * -of, --output-folder TEXT  Enter a path to the output data. Current working
                             directory is used by default
  * --help                     Show this message and exit.
