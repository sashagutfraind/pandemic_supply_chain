# AWS Supply Chain Project Structure

## Overview

This project provides automated data generation and ingestion for AWS Supply Chain, simulating a pandemic supply chain scenario with a 3-tier distribution network.

## Core Files

### Data Generation
- **`asc_generate.py`** - Pure data generation module
  - Contains all 15 CDM dataset generation functions
  - Can be imported as a module or run standalone
  - No AWS dependencies
  - Usage: `python asc_generate.py` or `from asc_generate import generate_all_data`

### Data Ingestion

- **`asc_2_lake_builder.py`** - Data Integration Flow approach (RECOMMENDED)
  - Orchestrates data generation, S3 upload, and flow management
  - Imports generation functions from `asc_generate.py`
  - Creates Data Integration Flows BEFORE uploading data (auto-triggers ingestion)
  - Works with all 15 datasets
  - Usage: `python asc_2_lake_builder.py --all`

- **`asc_4_send_events.py`** - API ingestion approach (LIMITED)
  - Uses `send_data_integration_event` API
  - Imports generation functions from `asc_generate.py`
  - Works with 10/15 datasets (compatible with event API)
  - Usage: `python asc_4_send_events.py --all`

### Supporting Tools

- **`asc_1_deploy_helper.py`** - Instance deployment helper
  - Creates Identity Center users and permission sets
  - Provides manual setup instructions
  - Usage: `python asc_1_deploy_helper.py`

- **`asc_3_diagnostics.py`** - Diagnostics and validation
  - Validates local data
  - Checks Data Lake status (47 datasets)
  - Monitors Data Integration Flows and executions
  - Monitors integration events
  - Usage: `python asc_3_diagnostics.py --all`

- **`validate_datasets.py`** - CDM compliance validator
  - Validates all 15 datasets for CDM compliance
  - Checks required fields, forbidden fields, null values
  - Usage: `python validate_datasets.py`

## File Dependencies

```
asc_generate.py (standalone data generation)
    ↑ imported by
    ├── asc_2_lake_builder.py (Data Integration Flows)
    └── asc_4_send_events.py (API ingestion)

asc_1_deploy_helper.py (standalone deployment)
asc_3_diagnostics.py (standalone diagnostics)
validate_datasets.py (standalone validation)

analysis-coverage/ (standalone coverage analysis)
    ├── coverage_1_gazeteer_zips.py (Census Gazetteer data)
    ├── coverage_2_download_acs.py (ACS population data)
    ├── coverage_3_current_stocks.py (AWS Supply Chain inventory)
    └── coverage_4_compute_access.py (Access metrics & visualizations)
```

## Configuration

- **`asc_instance_config.json`** - Instance configuration
  - Instance ID and name
  - Admin user credentials
  - S3 bucket name
  - AWS region

## Generated Data

- **`output-data/`** - Generated CSV files 

## Coverage Analysis Subproject

- **`analysis-coverage/`** - Antiviral coverage analysis pipeline
  - **`coverage_1_gazeteer_zips.py`** - Retrieves 850 WA ZIP codes from Census Gazetteer
  - **`coverage_2_download_acs.py`** - Fetches population data from ACS API (all ZIP codes, no sampling)
  - **`coverage_3_current_stocks.py`** - Queries inventory from AWS Supply Chain Data Lake
  - **`coverage_4_compute_access.py`** - Computes access metrics and generates visualizations
  - **`ABOUT_COVERAGE.md`** - Detailed methodology and findings documentation
  - **`coverage-output/`** - Analysis outputs:
    - `cov_1_zip_centroids.csv` - ZIP code coordinates
    - `cov_2_zip_population.csv` - Population data (850 ZIP codes)
    - `cov_2_census_raw.csv` - Raw census API response (cached)
    - `cov_3_current_stocks.csv` - Inventory by site
    - `cov_4_access_by_zip.csv` - Per-ZIP access metrics
    - `cov_4_access_by_site.csv` - Per-site aggregated metrics
    - `cov_4_stock_levels.png` - Visualization (requires matplotlib)
    - `cov_4_population_vs_doses.png` - Visualization (requires matplotlib)

## Documentation

### Setup Guides
- **`DATA_LAKE_SETUP_GUIDE.md`** - Step-by-step setup instructions
- **`DEPLOYMENT_GUIDE.md`** - Instance deployment guide
- **`DATA_INTEGRATION_SETUP.md`** - Manual flow creation guide

### Reference Documentation
- **`IMPLEMENTATION_SUMMARY.md`** - Complete implementation overview
- **`EXTENDED_DATA_MODEL.md`** - Data model documentation
- **`INGESTION_STATUS.md`** - API ingestion status
- **`DIAGNOSTICS_GUIDE.md`** - Diagnostics tool guide

### Project Documentation
- **`PROJECT_STRUCTURE.md`** - This file
- **`FILE_COMPARISON.md`** - File differences explained
- **`FINAL_STATUS.md`** - Current status and next steps
- **`README.md`** - Project overview

## Workflow

### 1. Deploy Instance
```bash
python asc_1_deploy_helper.py
```

### 2. Generate and Upload Data
```bash
# Generates 15 CDM datasets, creates flows, uploads to S3
python asc_2_lake_builder.py --all
```

### 3. Monitor Data Ingestion
```bash
# Check flow status and data lake
python asc_3_diagnostics.py --all
```

### 4. (Optional) Send Events via API
```bash
# Alternative ingestion for 10/15 datasets
python asc_4_send_events.py --all
```

### 5. Run Coverage Analysis
```bash
cd analysis-coverage
uv sync
uv run coverage_1_gazeteer_zips.py
uv run coverage_2_download_acs.py
uv run coverage_3_current_stocks.py
uv run coverage_4_compute_access.py
```

## Key Features

### Data Generation (`asc_generate.py`)
- 15 CDM-compliant datasets
- 3-tier distribution network (hub → DCs → local sites)
- Pandemic spike simulation (300% demand increase)
- Realistic inventory levels and lead times
- Transshipment scenarios

### Data Integration (`asc_2_lake_builder.py`)
- Automated S3 upload
- Automatic Data Integration Flow creation
- Flow execution monitoring
- Works with all 15 datasets
- Auto-triggers ingestion when data is uploaded

### API Ingestion (`asc_4_send_events.py`)
- Real-time event streaming
- CDM transformation
- Works with 10/15 datasets
- Limited by API schemas

### Diagnostics (`asc_3_diagnostics.py`)
- Local data validation
- Data Lake status checking (47 datasets)
- Data Integration Flow monitoring
- Integration event tracking
- CloudTrail API tracking
- CloudWatch logs checking

### Coverage Analysis (`analysis-coverage/`)
- Geographic accessibility analysis
- Population-weighted distance metrics
- Inventory coverage ratios (doses per 1,000 people)
- Identifies best-stocked and understocked sites
- Visualizations of coverage gaps
- Integrates with AWS Supply Chain Data Lake

