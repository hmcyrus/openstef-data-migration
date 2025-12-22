#!/usr/bin/env python3
"""
Script to append timezone offset (+06:00) to the first column of a CSV file.
Uses raw Python text parsing without pandas.
"""

import sys


def update_csv_first_column(input_file, output_file=None):
    """
    Append '+06:00' to the first column of each row in a CSV file.
    
    Args:
        input_file: Path to the input CSV file
        output_file: Path to the output CSV file (if None, overwrites input file)
    """
    # Read all lines from the input file
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Process each line
    updated_lines = []
    for i, line in enumerate(lines):
        # Remove trailing newline for processing
        line = line.rstrip('\n\r')
        
        # Skip header row (first line)
        if i == 0:
            updated_lines.append(line + '\n')
            continue
        
        # Find the first comma
        comma_index = line.find(',')
        
        if comma_index != -1:
            # Split at the first comma
            first_column = line[:comma_index]
            rest_of_line = line[comma_index:]
            
            # Append +06:00 to the first column
            updated_first_column = first_column + '+06:00'
            
            # Reconstruct the line
            updated_line = updated_first_column + rest_of_line
        else:
            # No comma found (shouldn't happen in valid CSV, but handle it)
            updated_line = line + '+06:00'
        
        # Add back the newline
        updated_lines.append(updated_line + '\n')
    
    # Determine output file path
    if output_file is None:
        output_file = input_file
    
    # Write the updated lines to the output file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.writelines(updated_lines)
    
    print(f"Successfully updated {len(updated_lines)} lines.")
    print(f"Output written to: {output_file}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python update_timestamp_timezone.py <input_csv_file> [output_csv_file]")
        print("\nIf output_csv_file is not provided, the input file will be overwritten.")
        sys.exit(1)
    
    input_csv = sys.argv[1]
    output_csv = sys.argv[2] if len(sys.argv) > 2 else None
    
    update_csv_first_column(input_csv, output_csv)

