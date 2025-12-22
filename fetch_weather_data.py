"""
Fetch weather data from Meteostat for a given date range.

This script fetches hourly weather data for Dhaka, Bangladesh using the Meteostat API
and saves it to a CSV file.

Usage:
    python fetch_weather_data.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD

Example:
    python fetch_weather_data.py --start-date 2023-01-01 --end-date 2023-12-31
"""

import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
from meteostat import Point, Hourly
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Dhaka coordinates
DHAKA_LAT = 23.8103
DHAKA_LON = 90.4125
DHAKA_ALT = 8  # meters above sea level

OUTPUT_FILE = "weather_data_from_meteostat.csv"


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Fetch weather data from Meteostat for a date range',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fetch_weather_data.py --start-date 2023-01-01 --end-date 2023-12-31
  python fetch_weather_data.py --start-date 2024-06-01 --end-date 2024-06-30
        """
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        required=True,
        help='Start date in YYYY-MM-DD format (e.g., 2023-01-01)'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        required=True,
        help='End date in YYYY-MM-DD format (e.g., 2023-12-31)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=OUTPUT_FILE,
        help=f'Output CSV file name (default: {OUTPUT_FILE})'
    )
    
    return parser.parse_args()


def validate_date(date_string: str) -> datetime:
    """
    Validate and parse date string in YYYY-MM-DD format.
    
    Args:
        date_string: Date string to validate
        
    Returns:
        datetime object
        
    Raises:
        ValueError: If date format is invalid
    """
    try:
        return datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError:
        raise ValueError(
            f"Invalid date format: '{date_string}'. "
            "Please use YYYY-MM-DD format (e.g., 2023-01-01)"
        )


def fetch_weather_data(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Fetch hourly weather data from Meteostat for the given date range.
    
    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        
    Returns:
        DataFrame containing hourly weather data with columns:
            - time: Timestamp
            - temp: Temperature in °C
            - dwpt: Dew point in °C
            - rhum: Relative humidity in %
            - prcp: Precipitation in mm
            - wdir: Wind direction in degrees
            - wspd: Wind speed in km/h
            - pres: Sea-level air pressure in hPa
            - coco: Weather condition code
    """
    logger.info(f"Fetching weather data for Dhaka (lat={DHAKA_LAT}, lon={DHAKA_LON})")
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
    
    # Create Point for Dhaka
    location = Point(DHAKA_LAT, DHAKA_LON, DHAKA_ALT)
    
    # Adjust end_date to include the full day
    end_date_adjusted = end_date + timedelta(days=1)
    
    # Calculate total days for progress bar
    total_days = (end_date - start_date).days + 1
    logger.info(f"Total days to fetch: {total_days}")
    
    # Fetch hourly data
    logger.info("Fetching data from Meteostat...")
    data = Hourly(location, start_date, end_date_adjusted)
    df = data.fetch()
    
    if df.empty:
        logger.warning("No weather data found for the specified date range!")
        return pd.DataFrame()
    
    # Reset index to make 'time' a column
    df = df.reset_index()
    df = df.rename(columns={'time': 'timestamp'})
    
    # Add date and hour columns for easier analysis
    df['date'] = df['timestamp'].dt.date
    df['hour'] = df['timestamp'].dt.hour
    
    # Reorder columns
    columns_order = ['timestamp', 'date', 'hour', 'temp', 'dwpt', 'rhum', 
                     'prcp', 'wdir', 'wspd', 'pres', 'coco']
    df = df[columns_order]
    
    logger.info(f"Successfully fetched {len(df)} hourly records")
    
    # Show data summary
    logger.info("\nData Summary:")
    logger.info(f"  Total records: {len(df)}")
    logger.info(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    logger.info(f"  Missing values per column:")
    for col in ['temp', 'dwpt', 'rhum', 'prcp', 'wdir', 'wspd', 'pres', 'coco']:
        missing = df[col].isna().sum()
        if missing > 0:
            logger.info(f"    {col}: {missing} ({missing/len(df)*100:.2f}%)")
    
    return df


def save_to_csv(df: pd.DataFrame, output_file: str):
    """
    Save DataFrame to CSV file.
    
    Args:
        df: DataFrame to save
        output_file: Output file path
    """
    logger.info(f"Saving data to {output_file}...")
    df.to_csv(output_file, index=False)
    logger.info(f"Successfully saved {len(df)} records to {output_file}")


def main():
    """Main function to orchestrate the weather data fetching process."""
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Validate dates
        start_date = validate_date(args.start_date)
        end_date = validate_date(args.end_date)
        
        # Validate date range
        if start_date > end_date:
            raise ValueError(
                f"Start date ({args.start_date}) cannot be after end date ({args.end_date})"
            )
        
        # Check if date range is too large (warn if > 2 years)
        days_diff = (end_date - start_date).days
        if days_diff > 730:
            logger.warning(
                f"Large date range detected ({days_diff} days). "
                "This may take a while to fetch..."
            )
        
        # Fetch weather data
        df = fetch_weather_data(start_date, end_date)
        
        if df.empty:
            logger.error("No data was fetched. Exiting.")
            return 1
        
        # Save to CSV
        save_to_csv(df, args.output)
        
        logger.info("\n" + "="*50)
        logger.info("Weather data fetch completed successfully!")
        logger.info(f"Output file: {args.output}")
        logger.info("="*50)
        
        return 0
        
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.exception(e)
        return 1


if __name__ == "__main__":
    exit(main())

