the order of python scripts should be as follows:
1. merge_excel_to_csv.py : input -> downloaded excel files, output -> master-data.csv
2. validate_master_data.py : input -> master-data.csv, output -> console texts
3. enrich_with_holidays.py : input -> master-data.csv, Holiday List.xlsx (List of Holidays); output - master-data-enriched.csv
4. fetch_meteostat_weather.py : input -> commands given in file comments; output - dhaka_weather_data.csv
5. update_timestamp_timezone.py : input -> command `python update_timestamp_timezone.py dhaka_weather_data.csv`; output - same file
6. merge_weather_with_master.py: input -> command `python merge_weather_with_master.py master-data-enriched.csv dhaka_weather_data.csv`; output -> merged_master_weather.csv
7. copy the final file to the static folder and make sure the name is `master_data_with_forecasted.csv`
