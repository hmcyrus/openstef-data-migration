"""Script to enrich master-data.csv with weather data from Meteostat"""
import logging
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from weather_service import get_weather_for_date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def enrich_with_weather(input_csv: str, output_csv: str):
    """
    Enrich master-data.csv with weather data from Meteostat.
    Uses minimal API calls - one call per unique date only.
    
    Args:
        input_csv: Path to input CSV file
        output_csv: Path to output CSV file with weather data
    """
    logger.info(f"Reading data from {input_csv}")
    df = pd.read_csv(input_csv)
    
    # Parse datetime column
    df['date_time'] = pd.to_datetime(df['date_time'])
    
    # Extract date and hour for efficient lookups
    df['date'] = df['date_time'].dt.date
    df['hour'] = df['date_time'].dt.hour
    
    # Get unique dates - this determines the exact number of API calls needed
    unique_dates = sorted(df['date'].unique())
    
    logger.info(f"Processing {len(df)} rows across {len(unique_dates)} unique dates")
    logger.info(f"Will make exactly {len(unique_dates)} API calls to Meteostat")
    
    # Fetch weather data for all unique dates (minimal API calls)
    weather_data_list = []
    
    for date in tqdm(unique_dates, desc="Fetching weather data"):
        date_obj = datetime.combine(date, datetime.min.time())
        weather_data = get_weather_for_date(date_obj)
        
        # Convert to DataFrame for this date's 24 hours
        for hour in range(24):
            if hour < len(weather_data):
                weather_row = weather_data[hour].copy()
                weather_row['date'] = date
                weather_row['hour'] = hour
                weather_data_list.append(weather_row)
    
    # Create weather DataFrame
    weather_df = pd.DataFrame(weather_data_list)
    
    logger.info("Merging weather data with master data (vectorized operation)")
    
    # Merge weather data with main dataframe using vectorized operation
    # This is much faster than iterating through rows
    df = df.merge(
        weather_df,
        on=['date', 'hour'],
        how='left'
    )
    
    # Fill any missing values with zeros
    weather_columns = ['temp', 'dwpt', 'rhum', 'prcp', 'wdir', 'wspd', 'pres', 'coco']
    df[weather_columns] = df[weather_columns].fillna(0.0)
    
    # Convert coco to int type
    df['coco'] = df['coco'].astype(int)
    
    # Drop temporary columns used for merging
    df = df.drop(['date', 'hour'], axis=1)
    
    # Format datetime column to the required format: YYYY-MM-DD HH:mm:00+06:00
    df['date_time'] = df['date_time'].dt.strftime('%Y-%m-%d %H:%M:00+06:00')
    
    # Save enriched data
    logger.info(f"Saving enriched data to {output_csv}")
    df.to_csv(output_csv, index=False)
    
    logger.info(f"Successfully enriched {len(df)} rows with weather data")
    
    # Display sample of enriched data
    logger.info("\nSample of enriched data:")
    print(df.head(10))
    
    # Display statistics
    logger.info("\nWeather data statistics:")
    print(df[weather_columns].describe())


if __name__ == "__main__":
    input_file = "master-data.csv"
    output_file = "master-data-with-weather.csv"
    
    try:
        enrich_with_weather(input_file, output_file)
        logger.info("Weather enrichment completed successfully!")
    except Exception as e:
        logger.error(f"Error during weather enrichment: {str(e)}")
        logger.exception(e)
        raise

