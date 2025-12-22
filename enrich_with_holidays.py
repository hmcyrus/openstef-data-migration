"""
Script to enrich master-data.csv with holiday information from Holiday List.xlsx
"""

import pandas as pd
from datetime import datetime

def main():
    # Read the master data CSV
    print("Reading master-data.csv...")
    master_df = pd.read_csv('master-data.csv')
    
    # Convert date_time column to datetime and extract date only for comparison
    master_df['date_time'] = pd.to_datetime(master_df['date_time'])
    master_df['date_only'] = master_df['date_time'].dt.date
    
    # Read the Holiday List Excel file
    print("Reading Holiday List.xlsx...")
    holidays_df = pd.read_excel('Holiday List.xlsx', sheet_name='List of Holidays')
    
    # Extract columns A (date) and D (holiday type)
    # Column A is the first column (index 0), Column D is the fourth column (index 3)
    holidays_df = holidays_df.iloc[:, [0, 3]]
    holidays_df.columns = ['holiday_date', 'holiday_type']
    
    # Convert holiday dates to date objects for comparison
    holidays_df['holiday_date'] = pd.to_datetime(holidays_df['holiday_date']).dt.date
    
    # Remove any rows with NaN values
    holidays_df = holidays_df.dropna()
    
    # Convert holiday_type to integer
    holidays_df['holiday_type'] = holidays_df['holiday_type'].astype(int)
    
    print(f"Found {len(holidays_df)} holidays in the Holiday List")
    
    # Create a dictionary for quick lookup: date -> holiday_type
    holiday_dict = dict(zip(holidays_df['holiday_date'], holidays_df['holiday_type']))
    
    # Add the new columns to master data
    print("Enriching master data with holiday information...")
    
    # Initialize columns
    master_df['is_holiday'] = 0
    master_df['holiday_type'] = 0
    master_df['national_event_type'] = 0
    
    # Set is_holiday and holiday_type based on the holiday dictionary
    master_df['is_holiday'] = master_df['date_only'].apply(lambda x: 1 if x in holiday_dict else 0)
    master_df['holiday_type'] = master_df['date_only'].apply(lambda x: holiday_dict.get(x, 0))
    
    # Drop the temporary date_only column
    master_df = master_df.drop('date_only', axis=1)
    
    # Save the enriched data to a new CSV file
    output_file = 'master-data-enriched.csv'
    print(f"Saving enriched data to {output_file}...")
    master_df.to_csv(output_file, index=False)
    
    # Print summary statistics
    total_rows = len(master_df)
    holiday_rows = master_df['is_holiday'].sum()
    print(f"\nSummary:")
    print(f"Total rows: {total_rows}")
    print(f"Holiday rows: {holiday_rows}")
    print(f"Non-holiday rows: {total_rows - holiday_rows}")
    print(f"\nEnriched data saved to {output_file}")
    
    # Display a sample of the enriched data
    print("\nSample of enriched data:")
    print(master_df.head(10))

if __name__ == "__main__":
    main()

