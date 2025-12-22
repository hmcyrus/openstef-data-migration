create a script to modify the master data 

- read the file `Holiday List.xlsx`, the sheet `List of Holidays` contains some date for holidays, using the columns A and D enrich the masterdata by adding two additional columns - `is_holiday` and `holiday_type` in each row using following rules
  - `is_holiday` will be 0 for non-holidays and 1 for holidays. Column A contains the date which should be considered as holidays.
  - `holiday_type` will have the integer value of column D.
- create another column named `national_event_type` which will have 0 in all rows
