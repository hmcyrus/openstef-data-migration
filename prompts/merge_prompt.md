You will merge all these excel files to one csv file. Create the new file in the current root directory. Name it `master-data.csv`

The script will read column A, B and D. 

You will save column B and column D as float.

For column A the values are of format `YYYY-MM-DD HH:mm`, while writing to new file append the second and timezone info, the final value will of format `YYYY-MM-DD HH:mm:00+06:00`

The resulting file will have three columns with name - date_time, load, forecasted_load
