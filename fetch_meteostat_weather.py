#!/usr/bin/env python3
"""
Fetch hourly weather data for Dhaka using Meteostat API.

Usage:
    python fetch_meteostat_weather.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--output OUTPUT_FILE]

Example:
    python fetch_meteostat_weather.py --start-date 2023-01-01 --end-date 2023-12-31 --output dhaka_weather.csv
"""

import argparse
import sys
from datetime import datetime
import pandas as pd
from meteostat import Point, Hourly


# Dhaka coordinates
DHAKA_LAT = 23.8103
DHAKA_LON = 90.4125
DHAKA_ALT = 8  # meters above sea level


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Fetch hourly weather data for Dhaka using Meteostat',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fetch_meteostat_weather.py --start-date 2023-01-01 --end-date 2023-01-31
  python fetch_meteostat_weather.py --start-date 2023-01-01 --end-date 2023-12-31 --output weather_2023.csv
        """
    )
    
    parser.add_argument(
        '--start-date',
        required=True,
        type=str,
        help='Start date in YYYY-MM-DD format'
    )
    
    parser.add_argument(
        '--end-date',
        required=True,
        type=str,
        help='End date in YYYY-MM-DD format'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default='dhaka_weather_data.csv',
        help='Output CSV file path (default: dhaka_weather_data.csv)'
    )
    
    return parser.parse_args()


def validate_date(date_string):
    """Validate and parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid date format: {date_string}. Expected format: YYYY-MM-DD")


def fetch_weather_data(start_date, end_date):
    """
    Fetch hourly weather data for Dhaka from Meteostat.
    
    Args:
        start_date (datetime): Start date
        end_date (datetime): End date
    
    Returns:
        pd.DataFrame: Weather data with required columns
    """
    print(f"Fetching weather data for Dhaka from {start_date.date()} to {end_date.date()}...")
    
    # Create Point for Dhaka
    location = Point(DHAKA_LAT, DHAKA_LON, DHAKA_ALT)
    
    # Fetch hourly data - single API call for the entire date range
    data = Hourly(location, start_date, end_date)
    df = data.fetch()
    
    if df.empty:
        print("Warning: No data retrieved from Meteostat", file=sys.stderr)
        return pd.DataFrame()
    
    print(f"Retrieved {len(df)} hourly records")
    
    # Reset index to make time a column
    df = df.reset_index()
    
    # Rename 'time' column to 'date_time'
    df = df.rename(columns={'time': 'date_time'})
    
    # Select and reorder columns as specified
    # Meteostat columns: temp, dwpt, rhum, prcp, wdir, wspd, pres, coco
    required_columns = ['date_time', 'temp', 'dwpt', 'rhum', 'prcp', 'wdir', 'wspd', 'pres', 'coco']
    
    # Check which columns are available
    available_columns = [col for col in required_columns if col in df.columns]
    
    if len(available_columns) < len(required_columns):
        missing = set(required_columns) - set(available_columns)
        print(f"Warning: Some columns are missing in the data: {missing}", file=sys.stderr)
    
    # Select only available required columns
    df = df[available_columns]
    
    return df


def save_to_csv(df, output_file):
    """Save DataFrame to CSV file."""
    df.to_csv(output_file, index=False)
    print(f"Data saved to: {output_file}")
    print(f"Total records: {len(df)}")
    
    # Print summary statistics
    print("\nData summary:")
    print(f"  Date range: {df['date_time'].min()} to {df['date_time'].max()}")
    print(f"  Columns: {', '.join(df.columns)}")
    
    # Check for missing values
    missing_counts = df.isnull().sum()
    if missing_counts.any():
        print("\nMissing values per column:")
        for col, count in missing_counts[missing_counts > 0].items():
            print(f"  {col}: {count} ({count/len(df)*100:.1f}%)")


def main():
    """Main execution function."""
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Validate dates
        start_date = validate_date(args.start_date)
        end_date = validate_date(args.end_date)
        
        # Check date range validity
        if start_date > end_date:
            print("Error: Start date must be before or equal to end date", file=sys.stderr)
            sys.exit(1)
        
        # Fetch weather data (single API call)
        df = fetch_weather_data(start_date, end_date)
        
        if df.empty:
            print("Error: No data to save", file=sys.stderr)
            sys.exit(1)
        
        # Save to CSV
        save_to_csv(df, args.output)
        
        print("\n✓ Weather data fetch completed successfully!")
        
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

