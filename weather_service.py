"""Weather Service for fetching weather data from Meteostat"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
from meteostat import Point, Hourly

logger = logging.getLogger(__name__)


class WeatherService:
    """Service for fetching weather data from Meteostat"""
    
    # Dhaka coordinates
    DHAKA_LAT = 23.8103
    DHAKA_LON = 90.4125
    DHAKA_ALT = 8  # meters above sea level
    
    def __init__(self):
        """Initialize the weather service with Dhaka location"""
        self.location = Point(self.DHAKA_LAT, self.DHAKA_LON, self.DHAKA_ALT)
        logger.info(f"Weather service initialized for Dhaka (lat={self.DHAKA_LAT}, lon={self.DHAKA_LON})")
    
    def get_hourly_weather_data(self, date: datetime) -> List[Dict[str, float]]:
        """
        Fetch 24-hour weather data from Meteostat for Dhaka for a specific date.
        
        Args:
            date: The target date (datetime object)
            
        Returns:
            List of dictionaries containing hourly weather data for 24 hours.
            Each dictionary contains:
                - temp: Temperature in °C
                - dwpt: Dew point in °C
                - rhum: Relative humidity in %
                - prcp: Precipitation in mm
                - wdir: Wind direction in degrees
                - wspd: Wind speed in km/h
                - pres: Sea-level air pressure in hPa
                - coco: Weather condition code
        """
        try:
            # Ensure we're working with a date at midnight
            start_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
            # End date is the next day at midnight (to get full 24 hours)
            end_date = start_date + timedelta(days=1)
            
            logger.info(f"Fetching weather data for Dhaka from {start_date} to {end_date}")
            
            # Fetch hourly data from Meteostat
            data = Hourly(self.location, start_date, end_date)
            df = data.fetch()
            
            if df.empty:
                logger.warning(f"No weather data found for date {date.date()}. Using default values.")
                return self._get_default_weather_data()
            
            # Convert DataFrame to list of dictionaries
            weather_data = []
            for hour in range(24):
                hour_timestamp = start_date + timedelta(hours=hour)
                
                # Try to find data for this specific hour
                if hour_timestamp in df.index:
                    row = df.loc[hour_timestamp]
                    weather_data.append({
                        'temp': float(row['temp']) if pd.notna(row['temp']) else 0.0,
                        'dwpt': float(row['dwpt']) if pd.notna(row['dwpt']) else 0.0,
                        'rhum': float(row['rhum']) if pd.notna(row['rhum']) else 0.0,
                        'prcp': float(row['prcp']) if pd.notna(row['prcp']) else 0.0,
                        'wdir': float(row['wdir']) if pd.notna(row['wdir']) else 0.0,
                        'wspd': float(row['wspd']) if pd.notna(row['wspd']) else 0.0,
                        'pres': float(row['pres']) if pd.notna(row['pres']) else 0.0,
                        'coco': int(row['coco']) if pd.notna(row['coco']) else 0
                    })
                else:
                    # If data for this hour is missing, use zeros
                    logger.warning(f"Missing weather data for hour {hour} on {date.date()}")
                    weather_data.append(self._get_default_hour_weather())
            
            logger.info(f"Successfully fetched weather data for {len(weather_data)} hours")
            return weather_data
            
        except Exception as e:
            logger.error(f"Error fetching weather data for date {date.date()}: {str(e)}")
            logger.exception(e)
            return self._get_default_weather_data()
    
    def _get_default_hour_weather(self) -> Dict[str, float]:
        """Return default weather values for a single hour"""
        return {
            'temp': 0.0,
            'dwpt': 0.0,
            'rhum': 0.0,
            'prcp': 0.0,
            'wdir': 0.0,
            'wspd': 0.0,
            'pres': 0.0,
            'coco': 0
        }
    
    def _get_default_weather_data(self) -> List[Dict[str, float]]:
        """Return default weather values for 24 hours"""
        return [self._get_default_hour_weather() for _ in range(24)]


# Create a singleton instance
weather_service = WeatherService()


def get_weather_for_date(date: datetime) -> List[Dict[str, float]]:
    """
    Convenience function to get weather data for a specific date.
    
    Args:
        date: The target date (datetime object)
        
    Returns:
        List of dictionaries containing hourly weather data for 24 hours
    """
    return weather_service.get_hourly_weather_data(date)

