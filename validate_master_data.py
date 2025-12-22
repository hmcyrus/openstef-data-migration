"""
Script to validate master-data.csv for timestamp integrity.

Checks:
1. If data is present for all hours from the given time range
2. If there are any duplicate timestamps
3. If there are any timestamps outside the expected range
"""

import pandas as pd
from datetime import datetime, timedelta
import pytz

def validate_master_data(csv_file):
    """
    Validate the master-data.csv file for timestamp integrity.
    
    Args:
        csv_file: Path to the CSV file to validate
    """
    print("=" * 80)
    print("MASTER DATA VALIDATION REPORT")
    print("=" * 80)
    print(f"\nReading file: {csv_file}")
    
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Parse the datetime column
    df['date_time'] = pd.to_datetime(df['date_time'])
    
    print(f"Total rows: {len(df)}")
    print(f"First timestamp: {df['date_time'].iloc[0]}")
    print(f"Last timestamp: {df['date_time'].iloc[-1]}")
    
    # Get the time range
    start_time = df['date_time'].iloc[0]
    end_time = df['date_time'].iloc[-1]
    
    print("\n" + "=" * 80)
    print("CHECK 1: DUPLICATE TIMESTAMPS")
    print("=" * 80)
    
    # Check for duplicates
    duplicates = df[df.duplicated(subset=['date_time'], keep=False)]
    
    if len(duplicates) > 0:
        print(f"❌ FOUND {len(duplicates)} duplicate timestamp entries!")
        print("\nDuplicate timestamps:")
        duplicate_times = duplicates['date_time'].unique()
        for dt in sorted(duplicate_times):
            count = len(duplicates[duplicates['date_time'] == dt])
            print(f"  - {dt}: appears {count} times")
            # Show the duplicate rows
            dup_rows = df[df['date_time'] == dt]
            print(f"    Row indices: {dup_rows.index.tolist()}")
    else:
        print("✓ No duplicate timestamps found")
    
    print("\n" + "=" * 80)
    print("CHECK 2: TIMESTAMPS OUTSIDE EXPECTED RANGE")
    print("=" * 80)
    
    # Check for timestamps outside the range
    outside_range = df[(df['date_time'] < start_time) | (df['date_time'] > end_time)]
    
    if len(outside_range) > 0:
        print(f"❌ FOUND {len(outside_range)} timestamps outside the range!")
        print(f"Expected range: {start_time} to {end_time}")
        print("\nOut-of-range timestamps:")
        for idx, row in outside_range.iterrows():
            print(f"  - Row {idx}: {row['date_time']}")
    else:
        print(f"✓ All timestamps are within the expected range")
        print(f"  Range: {start_time} to {end_time}")
    
    print("\n" + "=" * 80)
    print("CHECK 3: MISSING HOURS IN TIME RANGE")
    print("=" * 80)
    
    # Create a complete hourly range
    expected_range = pd.date_range(start=start_time, end=end_time, freq='H')
    
    print(f"Expected number of hours: {len(expected_range)}")
    print(f"Actual number of timestamps: {len(df)}")
    
    # Find missing timestamps
    actual_times = set(df['date_time'])
    expected_times = set(expected_range)
    missing_times = sorted(expected_times - actual_times)
    
    if len(missing_times) > 0:
        print(f"\n❌ FOUND {len(missing_times)} missing hours!")
        print("\nMissing timestamps (showing first 50):")
        for dt in missing_times[:50]:
            print(f"  - {dt}")
        if len(missing_times) > 50:
            print(f"  ... and {len(missing_times) - 50} more")
    else:
        print("✓ All hours are present in the time range")
    
    # Check for extra timestamps (timestamps in data but not in expected hourly range)
    extra_times = sorted(actual_times - expected_times)
    
    if len(extra_times) > 0:
        print(f"\n⚠ FOUND {len(extra_times)} extra timestamps (not on hourly boundaries)!")
        print("\nExtra timestamps (showing first 50):")
        for dt in extra_times[:50]:
            # Find the row index
            row_idx = df[df['date_time'] == dt].index[0]
            print(f"  - Row {row_idx}: {dt}")
        if len(extra_times) > 50:
            print(f"  ... and {len(extra_times) - 50} more")
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    issues_found = 0
    
    if len(duplicates) > 0:
        print(f"❌ Duplicates: {len(duplicates)} entries")
        issues_found += 1
    else:
        print("✓ Duplicates: None")
    
    if len(outside_range) > 0:
        print(f"❌ Out of range: {len(outside_range)} entries")
        issues_found += 1
    else:
        print("✓ Out of range: None")
    
    if len(missing_times) > 0:
        print(f"❌ Missing hours: {len(missing_times)} entries")
        issues_found += 1
    else:
        print("✓ Missing hours: None")
    
    if len(extra_times) > 0:
        print(f"⚠ Extra timestamps: {len(extra_times)} entries")
        issues_found += 1
    else:
        print("✓ Extra timestamps: None")
    
    print("\n" + "=" * 80)
    if issues_found == 0:
        print("✓✓✓ ALL CHECKS PASSED! Data integrity is good.")
    else:
        print(f"❌ FOUND {issues_found} ISSUE(S) - Please review the report above.")
    print("=" * 80)

if __name__ == "__main__":
    validate_master_data("master-data.csv")

