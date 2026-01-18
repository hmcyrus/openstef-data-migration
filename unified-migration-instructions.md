# Unified Migration Instructions

This document describes how to run the data migration pipeline using the unified scripts.

## Prerequisites

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Ensure the following input files/directories are available:
   - `DPDC Load/` - Directory containing Excel files (daily load data)
   - `Holiday List.xlsx` - Excel file with holiday information (sheet: "List of Holidays")

## Step 1: Fetch Weather Data

Run this separately before the main migration. Only needs to be re-run when the date range changes.

```bash
python fetch_weather.py --start-date 2023-01-01 --end-date 2025-09-30
```

**Options:**
| Option | Description | Default |
|--------|-------------|---------|
| `--start-date` | Start date (YYYY-MM-DD) | Required |
| `--end-date` | End date (YYYY-MM-DD) | Required |
| `--output` | Output file path | `dhaka_weather_data.csv` |

**Output:** `dhaka_weather_data.csv` with columns:
- `date_time` (with +06:00 timezone)
- `temp`, `dwpt`, `rhum`, `prcp`, `wdir`, `wspd`, `pres`, `coco`

## Step 2: Run Migration Pipeline

```bash
python unified_migration.py
```

**Options:**
| Option | Description | Default |
|--------|-------------|---------|
| `--dry-run` | Preview without making changes | Off |
| `--force` | Rebuild all files (ignore existing) | Off |
| `--excel-dir` | Excel files directory | `DPDC Load` |
| `--holiday-file` | Holiday list file | `Holiday List.xlsx` |
| `--weather-file` | Weather data file | `dhaka_weather_data.csv` |
| `--output-dir` | Final output directory | `static` |

**Pipeline Steps:**
1. Merge Excel files → `master-data.csv`
2. Enrich with holidays → `master-data-enriched.csv`
3. Merge with weather → `merged_master_weather.csv`
4. Finalize → `static/master_data_with_forecasted.csv`

## Quick Start

```bash
# First time setup
pip install -r requirements.txt

# Fetch weather data (once)
python fetch_weather.py --start-date 2023-01-01 --end-date 2025-09-30

# Run migration
python unified_migration.py
```

## Common Commands

```bash
# Preview what will happen
python unified_migration.py --dry-run

# Force rebuild everything
python unified_migration.py --force

# Custom paths
python unified_migration.py \
    --excel-dir "DPDC Load" \
    --holiday-file "Holiday List.xlsx" \
    --weather-file "dhaka_weather_data.csv" \
    --output-dir "./static"
```

## Output Files

| File | Description |
|------|-------------|
| `master-data.csv` | Merged Excel data (intermediate) |
| `master-data-enriched.csv` | With holiday info (intermediate) |
| `merged_master_weather.csv` | With weather data (intermediate) |
| `static/master_data_with_forecasted.csv` | **Final output** |

## Validation (Optional)

To validate the master data separately:
```bash
python validate_master_data.py
```

## Notes

- The pipeline skips steps if output files already exist (use `--force` to override)
- Weather data fetch uses Meteostat API and requires internet connection
- All timestamps use Dhaka timezone (+06:00)
