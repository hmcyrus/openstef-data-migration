write a script to fetch weather data using meteostat python lib. 
we need hourly weather data for dhaka of a given date range. 
script should take the dates from cli, the input date format should be YYYY-MM-dd.
absolute minimum number of api calls should be made.

the final output should contain only the following columns
- `date_time` -> containing the hourly timestamp
- following weather data columns - temp,dwpt,rhum,prcp,wdir,wspd,pres,coco

