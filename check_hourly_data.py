"""
Script to check for duplicate or missing hours in hourly CSV data.
"""
import pandas as pd
from datetime import timedelta


def check_hourly_data(csv_file):
    """
    Check for duplicate or missing hours in the CSV file.
    
    Args:
        csv_file: Path to the CSV file with hourly data
    """
    print(f"Reading data from {csv_file}...")
    
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Get the timestamp column (first column)
    timestamp_col = df.columns[0]
    print(f"Timestamp column: {timestamp_col}")
    
    # Convert to datetime
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    
    # Sort by timestamp to ensure proper order
    df = df.sort_values(timestamp_col).reset_index(drop=True)
    
    # Get start and end timestamps
    start_time = df[timestamp_col].iloc[0]
    end_time = df[timestamp_col].iloc[-1]
    
    print(f"\nData range:")
    print(f"  Start: {start_time}")
    print(f"  End:   {end_time}")
    print(f"  Total rows in file: {len(df)}")
    
    # Calculate expected number of hours
    time_diff = end_time - start_time
    expected_hours = int(time_diff.total_seconds() / 3600) + 1  # +1 to include both start and end
    print(f"  Expected hours: {expected_hours}")
    
    # Check for duplicates
    print("\n" + "="*60)
    print("CHECKING FOR DUPLICATES")
    print("="*60)
    
    duplicates = df[df.duplicated(subset=[timestamp_col], keep=False)]
    
    if len(duplicates) > 0:
        print(f"⚠️  Found {len(duplicates)} duplicate timestamps!")
        print("\nDuplicate timestamps:")
        duplicate_times = df[timestamp_col][df.duplicated(subset=[timestamp_col], keep=False)].unique()
        for ts in sorted(duplicate_times):
            count = (df[timestamp_col] == ts).sum()
            print(f"  {ts} - appears {count} times")
            # Show the duplicate rows
            dup_rows = df[df[timestamp_col] == ts]
            print(f"    Row indices: {dup_rows.index.tolist()}")
    else:
        print("✓ No duplicate timestamps found!")
    
    # Check for missing hours
    print("\n" + "="*60)
    print("CHECKING FOR MISSING HOURS")
    print("="*60)
    
    # Create a complete hourly range
    expected_range = pd.date_range(start=start_time, end=end_time, freq='h')
    
    # Find missing timestamps
    actual_timestamps = set(df[timestamp_col])
    expected_timestamps = set(expected_range)
    missing_timestamps = expected_timestamps - actual_timestamps
    
    if len(missing_timestamps) > 0:
        print(f"⚠️  Found {len(missing_timestamps)} missing hours!")
        print("\nMissing timestamps:")
        for ts in sorted(missing_timestamps):
            print(f"  {ts}")
    else:
        print("✓ No missing hours found!")
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total rows in file:      {len(df)}")
    print(f"Expected hours:          {expected_hours}")
    print(f"Unique timestamps:       {df[timestamp_col].nunique()}")
    print(f"Duplicate timestamps:    {len(duplicates)}")
    print(f"Missing hours:           {len(missing_timestamps)}")
    
    if len(duplicates) == 0 and len(missing_timestamps) == 0:
        print("\n✓ Data integrity check PASSED! No issues found.")
    else:
        print("\n⚠️  Data integrity check FAILED! Issues found above.")
    
    return {
        'total_rows': len(df),
        'expected_hours': expected_hours,
        'unique_timestamps': df[timestamp_col].nunique(),
        'duplicates': len(duplicates),
        'missing_hours': len(missing_timestamps),
        'duplicate_list': sorted(duplicate_times) if len(duplicates) > 0 else [],
        'missing_list': sorted(missing_timestamps) if len(missing_timestamps) > 0 else []
    }


if __name__ == "__main__":
    csv_file = "master-data.csv"
    results = check_hourly_data(csv_file)

