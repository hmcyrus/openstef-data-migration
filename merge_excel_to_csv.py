import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

def process_excel_file(file_path):
    """
    Process a single Excel file and extract columns A, B, D
    Returns a DataFrame with date_time, load, and forecasted_load
    """
    try:
        # Read the Excel file
        df = pd.read_excel(file_path, usecols=[0, 1, 3])  # Columns A, B, D (0-indexed: 0, 1, 3)
        
        # Rename columns
        df.columns = ['date_time', 'load', 'forecasted_load']
        
        # Convert load and forecasted_load to float
        df['load'] = pd.to_numeric(df['load'], errors='coerce')
        df['forecasted_load'] = pd.to_numeric(df['forecasted_load'], errors='coerce')
        
        # Process date_time column
        # Convert to datetime and then format with seconds and timezone
        df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce')
        
        # Drop rows where date_time is NaT (invalid dates)
        df = df.dropna(subset=['date_time'])
        
        # Filter to keep only hourly data (minute == 0)
        df = df[df['date_time'].dt.minute == 0]
        
        # Format date_time as string
        df['date_time'] = df['date_time'].dt.strftime('%Y-%m-%d %H:%M:00+06:00')
        
        return df
    
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return None

def merge_all_excel_files(root_dir):
    """
    Recursively find all Excel files in the directory and merge them
    """
    all_data = []
    
    # Find all Excel files recursively
    excel_files = list(Path(root_dir).rglob('*.xlsx'))
    
    # Exclude the consolidated file if it exists
    excel_files = [f for f in excel_files if 'all_data' not in f.name.lower()]
    
    print(f"Found {len(excel_files)} Excel files to process...")
    
    # Process each file
    for idx, file_path in enumerate(excel_files, 1):
        if idx % 50 == 0:
            print(f"Processing file {idx}/{len(excel_files)}...")
        
        df = process_excel_file(file_path)
        if df is not None and not df.empty:
            all_data.append(df)
    
    # Concatenate all dataframes
    if all_data:
        print(f"\nMerging {len(all_data)} dataframes...")
        merged_df = pd.concat(all_data, ignore_index=True)
        
        # Sort by date_time
        print("Sorting by date_time...")
        merged_df = merged_df.sort_values('date_time')
        
        # Remove duplicates if any
        print("Removing duplicates...")
        merged_df = merged_df.drop_duplicates(subset=['date_time'], keep='first')
        
        return merged_df
    else:
        print("No data to merge!")
        return None

def main():
    print("Starting Excel to CSV merge process...")
    print("=" * 60)
    
    # Define the root directory
    root_dir = "DPDC Load"
    
    # Check if directory exists
    if not os.path.exists(root_dir):
        print(f"Error: Directory '{root_dir}' not found!")
        return
    
    # Merge all Excel files
    merged_df = merge_all_excel_files(root_dir)
    
    if merged_df is not None:
        # Save to CSV
        output_file = "master-data.csv"
        print(f"\nSaving to {output_file}...")
        merged_df.to_csv(output_file, index=False)
        
        print("=" * 60)
        print(f"✓ Successfully created {output_file}")
        print(f"  Total rows: {len(merged_df):,}")
        print(f"  Columns: {', '.join(merged_df.columns)}")
        print(f"  Date range: {merged_df['date_time'].min()} to {merged_df['date_time'].max()}")
        print(f"  File size: {os.path.getsize(output_file) / (1024*1024):.2f} MB")
        print("=" * 60)
    else:
        print("Failed to create merged file.")

if __name__ == "__main__":
    main()

