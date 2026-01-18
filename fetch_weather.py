#!/usr/bin/env python3
"""
Fetch hourly weather data for Dhaka from Meteostat and format with timezone.

This script fetches weather data and automatically adds the Dhaka timezone (+06:00)
to the date_time column, producing a ready-to-use weather data file.

Usage:
    python fetch_weather.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--output OUTPUT_FILE]

Example:
    python fetch_weather.py --start-date 2023-01-01 --end-date 2025-09-30 --output dhaka_weather_data.csv
"""

import argparse
import sys
import time
from datetime import datetime
from typing import Optional

import pandas as pd
from meteostat import Point, Hourly


# Dhaka coordinates
DHAKA_LAT = 23.8103
DHAKA_LON = 90.4125
DHAKA_ALT = 8  # meters above sea level
DHAKA_TIMEZONE = "+06:00"

# Retry configuration
MAX_RETRIES = 4
RETRY_BACKOFF_BASE = 4  # seconds


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Fetch hourly weather data for Dhaka from Meteostat with timezone formatting',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fetch_weather.py --start-date 2023-01-01 --end-date 2023-12-31
  python fetch_weather.py --start-date 2023-01-01 --end-date 2025-09-30 --output weather.csv
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


def validate_date(date_string: str) -> datetime:
    """Validate and parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid date format: {date_string}. Expected format: YYYY-MM-DD")


def fetch_weather_data_with_retry(location: Point, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Fetch weather data from Meteostat with retry logic.
    
    Args:
        location: Meteostat Point object for the location
        start_date: Start date
        end_date: End date
    
    Returns:
        DataFrame with weather data
    """
    last_error = None
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Fetching weather data (attempt {attempt + 1}/{MAX_RETRIES})...")
            data = Hourly(location, start_date, end_date)
            df = data.fetch()
            
            if not df.empty:
                return df
            
            # Empty result - might be temporary, retry
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF_BASE * (2 ** attempt)
                print(f"Empty result, retrying in {wait_time}s...")
                time.sleep(wait_time)
                
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF_BASE * (2 ** attempt)
                print(f"Error: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
    
    if last_error:
        raise last_error
    return pd.DataFrame()


def fetch_weather_data(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Fetch hourly weather data for Dhaka from Meteostat.
    
    Args:
        start_date: Start date
        end_date: End date
    
    Returns:
        DataFrame with weather data including formatted date_time with timezone
    """
    print(f"Fetching weather data for Dhaka")
    print(f"  Location: lat={DHAKA_LAT}, lon={DHAKA_LON}, alt={DHAKA_ALT}m")
    print(f"  Date range: {start_date.date()} to {end_date.date()}")
    
    # Create Point for Dhaka
    location = Point(DHAKA_LAT, DHAKA_LON, DHAKA_ALT)
    
    # Fetch data with retry logic
    df = fetch_weather_data_with_retry(location, start_date, end_date)
    
    if df.empty:
        print("Warning: No data retrieved from Meteostat", file=sys.stderr)
        return pd.DataFrame()
    
    print(f"Retrieved {len(df)} hourly records")
    
    # Reset index to make time a column
    df = df.reset_index()
    
    # Rename 'time' column to 'date_time'
    df = df.rename(columns={'time': 'date_time'})
    
    # Format date_time with Dhaka timezone
    # Convert to string format: YYYY-MM-DD HH:MM:SS+06:00
    df['date_time'] = df['date_time'].dt.strftime(f'%Y-%m-%d %H:%M:%S{DHAKA_TIMEZONE}')
    
    # Select and reorder columns
    required_columns = ['date_time', 'temp', 'dwpt', 'rhum', 'prcp', 'wdir', 'wspd', 'pres', 'coco']
    
    # Check which columns are available
    available_columns = [col for col in required_columns if col in df.columns]
    
    if len(available_columns) < len(required_columns):
        missing = set(required_columns) - set(available_columns)
        print(f"Warning: Some columns are missing in the data: {missing}", file=sys.stderr)
    
    # Select only available required columns
    df = df[available_columns]
    
    return df


def save_to_csv(df: pd.DataFrame, output_file: str) -> None:
    """Save DataFrame to CSV file with summary."""
    df.to_csv(output_file, index=False)
    print(f"\nData saved to: {output_file}")
    print(f"Total records: {len(df)}")
    
    # Print summary statistics
    print("\nData summary:")
    print(f"  Date range: {df['date_time'].iloc[0]} to {df['date_time'].iloc[-1]}")
    print(f"  Columns: {', '.join(df.columns)}")
    
    # Check for missing values
    missing_counts = df.isnull().sum()
    if missing_counts.any():
        print("\nMissing values per column:")
        for col, count in missing_counts[missing_counts > 0].items():
            pct = count / len(df) * 100
            print(f"  {col}: {count} ({pct:.1f}%)")
    else:
        print("\nNo missing values in the data.")


def main():
    """Main execution function."""
    print("=" * 60)
    print("DHAKA WEATHER DATA FETCHER")
    print("=" * 60)
    
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
        
        # Fetch weather data
        df = fetch_weather_data(start_date, end_date)
        
        if df.empty:
            print("Error: No data to save", file=sys.stderr)
            sys.exit(1)
        
        # Save to CSV
        save_to_csv(df, args.output)
        
        print("\n" + "=" * 60)
        print("SUCCESS: Weather data fetch completed!")
        print(f"Output file: {args.output}")
        print("=" * 60)
        
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
