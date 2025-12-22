#!/usr/bin/env python3
"""
Merge master-data.csv and dhaka_weather_data.csv on date_time column.
Uses only built-in Python csv module (no pandas).
"""

import csv
import sys
import argparse
from collections import OrderedDict


def read_csv_to_dict(filepath):
    """
    Read a CSV file and return a dictionary with date_time as key.
    
    Args:
        filepath: Path to the CSV file
        
    Returns:
        tuple: (headers, data_dict)
            - headers: list of column names (excluding date_time)
            - data_dict: dict mapping date_time to row data (excluding date_time)
    """
    data_dict = OrderedDict()
    headers = []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        all_headers = next(reader)  # Read header row
        
        # Store headers except the first one (date_time)
        headers = all_headers[1:]
        
        # Read data rows
        for row in reader:
            if row:  # Skip empty rows
                date_time = row[0]
                values = row[1:]
                data_dict[date_time] = values
    
    return headers, data_dict


def merge_csv_files(master_file, weather_file, output_file):
    """
    Merge two CSV files on date_time column.
    
    Args:
        master_file: Path to master-data.csv
        weather_file: Path to dhaka_weather_data.csv
        output_file: Path to output merged CSV file
    """
    # Expected column order (excluding date_time which is always first)
    EXPECTED_COLUMNS = [
        'date_time', 'load', 'is_holiday', 'holiday_type', 'national_event_type',
        'temp', 'dwpt', 'rhum', 'prcp', 'wdir', 'wspd', 'pres', 'coco', 'forecasted_load'
    ]
    
    print(f"Reading {master_file}...")
    master_headers, master_data = read_csv_to_dict(master_file)
    
    print(f"Reading {weather_file}...")
    weather_headers, weather_data = read_csv_to_dict(weather_file)
    
    # Combine all available columns (excluding date_time)
    all_columns = master_headers + weather_headers
    
    # Check for column mismatches
    print(f"\nValidating columns...")
    expected_columns_set = set(EXPECTED_COLUMNS[1:])  # Exclude date_time for comparison
    actual_columns_set = set(all_columns)
    
    missing_columns = expected_columns_set - actual_columns_set
    extra_columns = actual_columns_set - expected_columns_set
    
    has_mismatch = False
    
    if missing_columns:
        print(f"⚠️  WARNING: Missing expected columns: {', '.join(sorted(missing_columns))}")
        has_mismatch = True
    
    if extra_columns:
        print(f"⚠️  WARNING: Extra columns found (not in expected list): {', '.join(sorted(extra_columns))}")
        has_mismatch = True
    
    if not has_mismatch:
        print(f"✓ All expected columns found!")
    
    # Create column index mappings for reordering
    master_col_index = {col: idx for idx, col in enumerate(master_headers)}
    weather_col_index = {col: idx for idx, col in enumerate(weather_headers)}
    
    # Build output headers in the expected order
    output_headers = []
    column_source = []  # Track where each column comes from
    
    for col in EXPECTED_COLUMNS:
        if col == 'date_time':
            output_headers.append(col)
            column_source.append(('both', None))
        elif col in master_col_index:
            output_headers.append(col)
            column_source.append(('master', master_col_index[col]))
        elif col in weather_col_index:
            output_headers.append(col)
            column_source.append(('weather', weather_col_index[col]))
        else:
            # Column expected but not found - add it anyway with empty values
            output_headers.append(col)
            column_source.append(('missing', None))
    
    print(f"\nMerging data...")
    
    # Get all unique timestamps from both files
    all_timestamps = sorted(set(master_data.keys()) | set(weather_data.keys()))
    
    # Write merged data
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(output_headers)
        
        rows_written = 0
        for timestamp in all_timestamps:
            # Get data for this timestamp
            master_values = master_data.get(timestamp, [''] * len(master_headers))
            weather_values = weather_data.get(timestamp, [''] * len(weather_headers))
            
            # Build row in the expected column order
            merged_row = []
            for col, (source, index) in zip(output_headers, column_source):
                if source == 'both':
                    merged_row.append(timestamp)
                elif source == 'master':
                    merged_row.append(master_values[index] if master_values else '')
                elif source == 'weather':
                    merged_row.append(weather_values[index] if weather_values else '')
                else:  # missing
                    merged_row.append('')
            
            writer.writerow(merged_row)
            rows_written += 1
    
    print(f"\nMerge complete!")
    print(f"Total rows written: {rows_written}")
    print(f"Output file: {output_file}")
    print(f"\nColumns in output file ({len(output_headers)}): {', '.join(output_headers)}")
    
    if has_mismatch:
        print(f"\n⚠️  NOTE: Column mismatches were detected. Please review the warnings above.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Merge two CSV files on date_time column using raw Python csv module.'
    )
    parser.add_argument(
        'master_file',
        help='Path to the master data CSV file'
    )
    parser.add_argument(
        'weather_file',
        help='Path to the weather data CSV file'
    )
    parser.add_argument(
        '-o', '--output',
        default='merged_master_weather.csv',
        help='Path to the output merged CSV file (default: merged_master_weather.csv)'
    )
    
    args = parser.parse_args()
    
    merge_csv_files(args.master_file, args.weather_file, args.output)

