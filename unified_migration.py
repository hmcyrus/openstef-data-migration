#!/usr/bin/env python3
"""
Unified Migration Script for DPDC Load Data

This script performs the complete data migration pipeline:
1. Merge Excel files from DPDC Load directory into master-data.csv
2. Enrich with holiday information from Holiday List.xlsx
3. Merge with weather data from dhaka_weather_data.csv
4. Finalize output to static/master_data_with_forecasted.csv

Prerequisites:
- Run fetch_weather.py first to create dhaka_weather_data.csv
- Ensure Holiday List.xlsx is available

Usage:
    python unified_migration.py [options]

Examples:
    python unified_migration.py
    python unified_migration.py --dry-run
    python unified_migration.py --force
    python unified_migration.py --output-dir ./output
"""

import argparse
import csv
import os
import shutil
import sys
import tempfile
import warnings
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Suppress openpyxl warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class Config:
    """Configuration for the migration pipeline."""
    # Input paths
    excel_dir: str = "DPDC Load"
    holiday_file: str = "Holiday List.xlsx"
    weather_file: str = "dhaka_weather_data.csv"
    
    # Intermediate output files
    master_data_file: str = "master-data.csv"
    enriched_data_file: str = "master-data-enriched.csv"
    merged_data_file: str = "merged_master_weather.csv"
    
    # Final output
    output_dir: str = "static"
    final_output_file: str = "master_data_with_forecasted.csv"
    
    # Options
    force: bool = False
    dry_run: bool = False


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def log(message: str, level: str = "INFO") -> None:
    """Print a timestamped log message."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def log_step(step_num: int, total_steps: int, description: str) -> None:
    """Print a step header."""
    print()
    print("=" * 70)
    print(f"STEP {step_num}/{total_steps}: {description}")
    print("=" * 70)


def file_exists(path: str) -> bool:
    """Check if a file exists."""
    return os.path.isfile(path)


def dir_exists(path: str) -> bool:
    """Check if a directory exists."""
    return os.path.isdir(path)


def atomic_write_csv(df: pd.DataFrame, output_path: str) -> None:
    """Write DataFrame to CSV atomically (write to temp, then rename)."""
    # Create temp file in same directory for atomic rename
    output_dir = os.path.dirname(output_path) or "."
    fd, temp_path = tempfile.mkstemp(suffix='.csv', dir=output_dir)
    try:
        os.close(fd)
        df.to_csv(temp_path, index=False)
        shutil.move(temp_path, output_path)
    except Exception:
        # Clean up temp file if something went wrong
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise


def atomic_write_csv_raw(rows: List[List[str]], headers: List[str], output_path: str) -> None:
    """Write rows to CSV atomically using raw csv module."""
    output_dir = os.path.dirname(output_path) or "."
    fd, temp_path = tempfile.mkstemp(suffix='.csv', dir=output_dir)
    try:
        os.close(fd)
        with open(temp_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        shutil.move(temp_path, output_path)
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise


def count_excel_files(excel_dir: str) -> int:
    """Count Excel files in directory (excluding 'all_data' files)."""
    excel_files = list(Path(excel_dir).rglob('*.xlsx'))
    return len([f for f in excel_files if 'all_data' not in f.name.lower()])


# =============================================================================
# STEP 1: MERGE EXCEL FILES TO CSV
# =============================================================================

def process_excel_file(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Process a single Excel file and extract columns A, B, D.
    Returns a DataFrame with date_time, load, and forecasted_load.
    """
    try:
        # Read the Excel file
        df = pd.read_excel(file_path, usecols=[0, 1, 3])  # Columns A, B, D
        
        # Rename columns
        df.columns = ['date_time', 'load', 'forecasted_load']
        
        # Convert load and forecasted_load to float
        df['load'] = pd.to_numeric(df['load'], errors='coerce')
        df['forecasted_load'] = pd.to_numeric(df['forecasted_load'], errors='coerce')
        
        # Process date_time column
        df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce')
        
        # Drop rows where date_time is NaT (invalid dates)
        df = df.dropna(subset=['date_time'])
        
        # Filter to keep only hourly data (minute == 0)
        df = df[df['date_time'].dt.minute == 0]
        
        # Format date_time as string with timezone
        df['date_time'] = df['date_time'].dt.strftime('%Y-%m-%d %H:%M:00+06:00')
        
        return df
    
    except Exception as e:
        log(f"Error processing {file_path}: {str(e)}", "WARNING")
        return None


def step_1_merge_excel_to_csv(config: Config) -> bool:
    """
    Step 1: Merge all Excel files into master-data.csv.
    
    Returns:
        True if successful, False otherwise.
    """
    output_file = config.master_data_file
    
    # Check if output already exists
    if file_exists(output_file) and not config.force:
        log(f"Output file {output_file} already exists. Skipping step. (use --force to rebuild)")
        return True
    
    if config.dry_run:
        file_count = count_excel_files(config.excel_dir)
        log(f"[DRY RUN] Would process {file_count} Excel files from {config.excel_dir}")
        log(f"[DRY RUN] Would create {output_file}")
        return True
    
    # Find all Excel files
    excel_files = list(Path(config.excel_dir).rglob('*.xlsx'))
    excel_files = [f for f in excel_files if 'all_data' not in f.name.lower()]
    
    if not excel_files:
        log(f"No Excel files found in {config.excel_dir}", "ERROR")
        return False
    
    log(f"Found {len(excel_files)} Excel files to process")
    
    # Process each file with progress
    all_data = []
    for idx, file_path in enumerate(excel_files, 1):
        if idx % 50 == 0 or idx == len(excel_files):
            log(f"Processing file {idx}/{len(excel_files)}...")
        
        df = process_excel_file(file_path)
        if df is not None and not df.empty:
            all_data.append(df)
    
    if not all_data:
        log("No data extracted from Excel files", "ERROR")
        return False
    
    # Concatenate all dataframes
    log(f"Merging {len(all_data)} dataframes...")
    merged_df = pd.concat(all_data, ignore_index=True)
    
    # Sort by date_time
    log("Sorting by date_time...")
    merged_df = merged_df.sort_values('date_time')
    
    # Remove duplicates
    log("Removing duplicates...")
    original_count = len(merged_df)
    merged_df = merged_df.drop_duplicates(subset=['date_time'], keep='first')
    duplicates_removed = original_count - len(merged_df)
    
    if duplicates_removed > 0:
        log(f"Removed {duplicates_removed} duplicate rows")
    
    # Save to CSV atomically
    log(f"Saving to {output_file}...")
    atomic_write_csv(merged_df, output_file)
    
    # Summary
    log(f"Created {output_file}")
    log(f"  Total rows: {len(merged_df):,}")
    log(f"  Date range: {merged_df['date_time'].min()} to {merged_df['date_time'].max()}")
    log(f"  File size: {os.path.getsize(output_file) / (1024*1024):.2f} MB")
    
    return True


# =============================================================================
# STEP 2: ENRICH WITH HOLIDAYS
# =============================================================================

def step_2_enrich_with_holidays(config: Config) -> bool:
    """
    Step 2: Enrich master data with holiday information.
    
    Returns:
        True if successful, False otherwise.
    """
    input_file = config.master_data_file
    holiday_file = config.holiday_file
    output_file = config.enriched_data_file
    
    # Check if output already exists
    if file_exists(output_file) and not config.force:
        log(f"Output file {output_file} already exists. Skipping step. (use --force to rebuild)")
        return True
    
    if config.dry_run:
        log(f"[DRY RUN] Would read {input_file}")
        log(f"[DRY RUN] Would read holidays from {holiday_file}")
        log(f"[DRY RUN] Would create {output_file}")
        return True
    
    # Read the master data CSV
    log(f"Reading {input_file}...")
    master_df = pd.read_csv(input_file)
    
    # Convert date_time column to datetime and extract date only for comparison
    master_df['date_time'] = pd.to_datetime(master_df['date_time'])
    master_df['date_only'] = master_df['date_time'].dt.date
    
    # Read the Holiday List Excel file
    log(f"Reading {holiday_file}...")
    holidays_df = pd.read_excel(holiday_file, sheet_name='List of Holidays')
    
    # Extract columns A (date) and D (holiday type)
    holidays_df = holidays_df.iloc[:, [0, 3]]
    holidays_df.columns = ['holiday_date', 'holiday_type']
    
    # Convert holiday dates to date objects for comparison
    holidays_df['holiday_date'] = pd.to_datetime(holidays_df['holiday_date']).dt.date
    
    # Remove any rows with NaN values
    holidays_df = holidays_df.dropna()
    
    # Convert holiday_type to integer
    holidays_df['holiday_type'] = holidays_df['holiday_type'].astype(int)
    
    log(f"Found {len(holidays_df)} holidays in the Holiday List")
    
    # Create a dictionary for quick lookup: date -> holiday_type
    holiday_dict = dict(zip(holidays_df['holiday_date'], holidays_df['holiday_type']))
    
    # Add the new columns to master data
    log("Enriching master data with holiday information...")
    
    # Initialize columns
    master_df['is_holiday'] = 0
    master_df['holiday_type'] = 0
    master_df['national_event_type'] = 0
    
    # Set is_holiday and holiday_type based on the holiday dictionary
    master_df['is_holiday'] = master_df['date_only'].apply(lambda x: 1 if x in holiday_dict else 0)
    master_df['holiday_type'] = master_df['date_only'].apply(lambda x: holiday_dict.get(x, 0))
    
    # Drop the temporary date_only column
    master_df = master_df.drop('date_only', axis=1)
    
    # Restore date_time format with timezone
    master_df['date_time'] = master_df['date_time'].dt.strftime('%Y-%m-%d %H:%M:%S+06:00')
    
    # Save the enriched data
    log(f"Saving to {output_file}...")
    atomic_write_csv(master_df, output_file)
    
    # Summary
    total_rows = len(master_df)
    holiday_rows = master_df['is_holiday'].sum()
    log(f"Created {output_file}")
    log(f"  Total rows: {total_rows:,}")
    log(f"  Holiday rows: {holiday_rows:,}")
    log(f"  Non-holiday rows: {total_rows - holiday_rows:,}")
    
    return True


# =============================================================================
# STEP 3: MERGE WEATHER WITH MASTER
# =============================================================================

def read_csv_to_dict(filepath: str) -> Tuple[List[str], OrderedDict]:
    """
    Read a CSV file and return a dictionary with date_time as key.
    
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


def step_3_merge_weather_with_master(config: Config) -> bool:
    """
    Step 3: Merge enriched master data with weather data.
    
    Returns:
        True if successful, False otherwise.
    """
    master_file = config.enriched_data_file
    weather_file = config.weather_file
    output_file = config.merged_data_file
    
    # Check if output already exists
    if file_exists(output_file) and not config.force:
        log(f"Output file {output_file} already exists. Skipping step. (use --force to rebuild)")
        return True
    
    if config.dry_run:
        log(f"[DRY RUN] Would read {master_file}")
        log(f"[DRY RUN] Would read {weather_file}")
        log(f"[DRY RUN] Would create {output_file}")
        return True
    
    # Expected column order
    EXPECTED_COLUMNS = [
        'date_time', 'load', 'is_holiday', 'holiday_type', 'national_event_type',
        'temp', 'dwpt', 'rhum', 'prcp', 'wdir', 'wspd', 'pres', 'coco', 'forecasted_load'
    ]
    
    log(f"Reading {master_file}...")
    master_headers, master_data = read_csv_to_dict(master_file)
    
    log(f"Reading {weather_file}...")
    weather_headers, weather_data = read_csv_to_dict(weather_file)
    
    # Combine all available columns (excluding date_time)
    all_columns = master_headers + weather_headers
    
    # Check for column mismatches
    log("Validating columns...")
    expected_columns_set = set(EXPECTED_COLUMNS[1:])  # Exclude date_time
    actual_columns_set = set(all_columns)
    
    missing_columns = expected_columns_set - actual_columns_set
    extra_columns = actual_columns_set - expected_columns_set
    
    if missing_columns:
        log(f"Missing expected columns: {', '.join(sorted(missing_columns))}", "WARNING")
    
    if extra_columns:
        log(f"Extra columns found: {', '.join(sorted(extra_columns))}", "WARNING")
    
    if not missing_columns and not extra_columns:
        log("All expected columns found!")
    
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
    
    log("Merging data...")
    
    # Get all unique timestamps from both files
    all_timestamps = sorted(set(master_data.keys()) | set(weather_data.keys()))
    
    # Build merged rows
    merged_rows = []
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
        
        merged_rows.append(merged_row)
    
    # Save merged data atomically
    log(f"Saving to {output_file}...")
    atomic_write_csv_raw(merged_rows, output_headers, output_file)
    
    # Summary
    log(f"Created {output_file}")
    log(f"  Total rows: {len(merged_rows):,}")
    log(f"  Columns: {len(output_headers)}")
    
    return True


# =============================================================================
# STEP 4: FINALIZE OUTPUT
# =============================================================================

def step_4_finalize_output(config: Config) -> bool:
    """
    Step 4: Copy merged data to final output location.
    
    Returns:
        True if successful, False otherwise.
    """
    input_file = config.merged_data_file
    output_dir = config.output_dir
    output_file = os.path.join(output_dir, config.final_output_file)
    
    if config.dry_run:
        log(f"[DRY RUN] Would create directory {output_dir} (if not exists)")
        log(f"[DRY RUN] Would copy {input_file} to {output_file}")
        return True
    
    # Create output directory if it doesn't exist
    if not dir_exists(output_dir):
        log(f"Creating output directory: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
    
    # Copy file to final location
    log(f"Copying to final location: {output_file}")
    shutil.copy2(input_file, output_file)
    
    # Summary
    file_size = os.path.getsize(output_file) / (1024 * 1024)
    log(f"Created {output_file}")
    log(f"  File size: {file_size:.2f} MB")
    
    return True


# =============================================================================
# PRE-FLIGHT CHECKS
# =============================================================================

def preflight_checks(config: Config) -> bool:
    """
    Verify all required inputs exist before starting the pipeline.
    
    Returns:
        True if all checks pass, False otherwise.
    """
    print()
    print("=" * 70)
    print("PRE-FLIGHT CHECKS")
    print("=" * 70)
    
    all_passed = True
    
    # Check Excel directory
    if dir_exists(config.excel_dir):
        file_count = count_excel_files(config.excel_dir)
        if file_count > 0:
            log(f"[OK] Excel directory: {config.excel_dir} ({file_count} files)")
        else:
            log(f"[FAIL] Excel directory exists but contains no .xlsx files: {config.excel_dir}", "ERROR")
            all_passed = False
    else:
        log(f"[FAIL] Excel directory not found: {config.excel_dir}", "ERROR")
        all_passed = False
    
    # Check Holiday file
    if file_exists(config.holiday_file):
        log(f"[OK] Holiday file: {config.holiday_file}")
    else:
        log(f"[FAIL] Holiday file not found: {config.holiday_file}", "ERROR")
        all_passed = False
    
    # Check Weather file
    if file_exists(config.weather_file):
        log(f"[OK] Weather file: {config.weather_file}")
    else:
        log(f"[FAIL] Weather file not found: {config.weather_file}", "ERROR")
        log("       Run fetch_weather.py first to create this file.", "ERROR")
        all_passed = False
    
    print()
    return all_passed


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def run_pipeline(config: Config) -> bool:
    """
    Run the complete migration pipeline.
    
    Returns:
        True if successful, False otherwise.
    """
    print()
    print("=" * 70)
    print("UNIFIED MIGRATION PIPELINE")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if config.dry_run:
        print("MODE: DRY RUN (no files will be created)")
    if config.force:
        print("MODE: FORCE (rebuilding all files)")
    
    # Pre-flight checks
    if not preflight_checks(config):
        log("Pre-flight checks failed. Aborting.", "ERROR")
        return False
    
    # Define steps
    steps = [
        (1, "Merge Excel files to master-data.csv", step_1_merge_excel_to_csv),
        (2, "Enrich with holiday information", step_2_enrich_with_holidays),
        (3, "Merge weather data with master", step_3_merge_weather_with_master),
        (4, "Finalize output", step_4_finalize_output),
    ]
    
    total_steps = len(steps)
    
    # Execute steps
    for step_num, description, step_func in steps:
        log_step(step_num, total_steps, description)
        
        try:
            success = step_func(config)
            if not success:
                log(f"Step {step_num} failed. Aborting pipeline.", "ERROR")
                return False
        except Exception as e:
            log(f"Step {step_num} raised an exception: {e}", "ERROR")
            return False
    
    # Final summary
    print()
    print("=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not config.dry_run:
        final_output = os.path.join(config.output_dir, config.final_output_file)
        print(f"\nFinal output: {final_output}")
        
        print("\nIntermediate files created:")
        for f in [config.master_data_file, config.enriched_data_file, config.merged_data_file]:
            if file_exists(f):
                size = os.path.getsize(f) / (1024 * 1024)
                print(f"  - {f} ({size:.2f} MB)")
    
    return True


# =============================================================================
# CLI
# =============================================================================

def parse_arguments() -> Config:
    """Parse command line arguments and return Config."""
    parser = argparse.ArgumentParser(
        description='Unified Migration Script for DPDC Load Data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python unified_migration.py                    # Run full pipeline
  python unified_migration.py --dry-run          # Preview without making changes
  python unified_migration.py --force            # Rebuild all files
  python unified_migration.py --output-dir out   # Custom output directory
        """
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would happen without making changes'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force rebuild all intermediate files'
    )
    
    parser.add_argument(
        '--excel-dir',
        type=str,
        default='DPDC Load',
        help='Directory containing Excel files (default: DPDC Load)'
    )
    
    parser.add_argument(
        '--holiday-file',
        type=str,
        default='Holiday List.xlsx',
        help='Path to Holiday List Excel file (default: Holiday List.xlsx)'
    )
    
    parser.add_argument(
        '--weather-file',
        type=str,
        default='dhaka_weather_data.csv',
        help='Path to weather data CSV file (default: dhaka_weather_data.csv)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='static',
        help='Output directory for final file (default: static)'
    )
    
    args = parser.parse_args()
    
    return Config(
        excel_dir=args.excel_dir,
        holiday_file=args.holiday_file,
        weather_file=args.weather_file,
        output_dir=args.output_dir,
        force=args.force,
        dry_run=args.dry_run,
    )


def main():
    """Main entry point."""
    try:
        config = parse_arguments()
        success = run_pipeline(config)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        log(f"Unexpected error: {e}", "ERROR")
        sys.exit(1)


if __name__ == '__main__':
    main()
