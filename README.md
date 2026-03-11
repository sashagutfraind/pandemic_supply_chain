# AWS Supply Chain - Antiviral Distribution Optimization

Simulates the "Bullwhip Effect" during pandemic scenarios using AWS Supply Chain to optimize antiviral drug distribution across US ZIP codes.

## Quick Start

```bash
# 0. Launch instance 
python asc_1_deploy_helper

# 1. Generate and upload data
python asc_2_lake_builder.py --all

# 2. Check flow status
python asc_2_lake_builder.py --list-flows

# 3. Validate datasets
python validate_datasets.py
```

## Core Scripts

- `asc_generate.py` - Generate 15 CDM-compliant datasets (369 records)
- `asc_2_lake_builder.py` - Upload to S3 and manage Data Integration Flows
- `asc_send_data.py` - Alternative API-based ingestion (limited to 10/15 datasets)
- `asc_1_deploy_helper.py` - Instance deployment automation
- `asc_3_diagnostics.py` - Diagnostics and monitoring
- `validate_datasets.py` - CDM compliance validation

## Project Structure

```
antivirals_supply_chain/
├── asc_*.py                    # Main scripts
├── validate_datasets.py        # Validation
├── asc_instance_config.json    # Configuration
├── output-data/                # Generated datasets
├── helpers/                    # Helper scripts and documentation
│   ├── *.py                    # Utility scripts
│   └── *.md                    # Detailed documentation
└── README.md                   # This file
```

## Scenario

- New variant causes 300% spike in antiviral demand
- Manufacturers face 14-day lead times
- Local clinics need drugs within 24 hours
- Goal: Optimize distribution to maximize population access

## Supply Chain Network

3-tier distribution:
- 1 Regional Hub (Seattle): 2,800 units
- 2 Distribution Centers (Spokane, Tacoma): 1,050 units each
- 4 Local Sites: 240-700 units each

## Documentation

See `helpers/` directory for detailed guides:
- `FINAL_STATUS.md` - Current project status
- `DATA_LAKE_SETUP_GUIDE.md` - Step-by-step setup
- `DEPLOYMENT_GUIDE.md` - Instance deployment
- `TEST_RESULTS.md` - Validation results

## Instance Configuration

- Instance: ascnew1 (`d873032f-739a-41ac-9d86-f57c5e0c2779`)
- Region: us-east-1
- S3 Bucket: `aws-supply-chain-data-d873032f-739a-41ac-9d86-f57c5e0c2779`
- Datasets: 15 CDM-compliant (369 records)

## Next Steps

1. Manually trigger flows in AWS Supply Chain console
2. Verify data ingestion with `--list-flows`
3. Check Insights for network visualization
4. Build Bedrock Agent for transshipment optimization
