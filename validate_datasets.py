#!/usr/bin/env python3
"""
Dataset Validation Script

Validates all generated datasets for CDM compliance before upload.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple


OUTPUT_DIR = Path("output-data")

# CDM required fields for each dataset
CDM_REQUIRED_FIELDS = {
    'company': ['id', 'description'],
    'geography': ['id', 'description'],
    'calendar': ['calendar_id', 'date'],
    'product_hierarchy': ['id', 'description'],
    'product': ['id', 'description'],
    'trading_partner': ['id', 'description', 'tpartner_type'],
    'site': ['id', 'description'],
    'transportation_lane': ['id', 'from_site_id', 'to_site_id'],
    'inv_policy': ['id', 'site_id', 'product_id'],
    'forecast': ['snapshot_date', 'site_id', 'product_id'],
    'inv_level': ['product_id', 'site_id', 'on_hand_inventory', 'snapshot_date'],
    'inbound_order': ['id', 'tpartner_id'],
    'inbound_order_line': ['id', 'order_id', 'product_id'],
    'shipment': ['id', 'product_id'],
    'outbound_order_line': ['id', 'product_id']
}

# Fields that should NOT be present (non-CDM)
FORBIDDEN_FIELDS = {
    'site': ['state', 'custom_total_pop', 'custom_median_income', 'custom_poverty_rate'],
    'product': ['category', 'unit_of_measure', 'manufacturer'],
    'trading_partner': ['name', 'partner_type', 'state']
}


def validate_dataset(dataset_name: str, filepath: Path) -> Tuple[bool, List[str]]:
    """Validate a single dataset."""
    errors = []
    
    try:
        df = pd.read_csv(filepath)
        
        # Check required fields
        required = CDM_REQUIRED_FIELDS.get(dataset_name, [])
        for field in required:
            if field not in df.columns:
                errors.append(f"Missing required field: {field}")
        
        # Check forbidden fields
        forbidden = FORBIDDEN_FIELDS.get(dataset_name, [])
        for field in forbidden:
            if field in df.columns:
                errors.append(f"Forbidden field present: {field}")
        
        # Check for empty dataset
        if len(df) == 0:
            errors.append("Dataset is empty")
        
        # Check for null values in required fields
        for field in required:
            if field in df.columns and df[field].isnull().any():
                null_count = df[field].isnull().sum()
                errors.append(f"Field '{field}' has {null_count} null values")
        
        return len(errors) == 0, errors
        
    except Exception as e:
        return False, [f"Error reading file: {str(e)}"]


def main():
    print("=" * 70)
    print("AWS Supply Chain Dataset Validation")
    print("=" * 70)
    print()
    
    datasets = [
        'company', 'geography', 'calendar',
        'product_hierarchy', 'product',
        'trading_partner', 'site', 'transportation_lane',
        'inv_policy', 'forecast',
        'inv_level', 'inbound_order', 'inbound_order_line',
        'shipment', 'outbound_order_line'
    ]
    
    total_datasets = len(datasets)
    valid_datasets = 0
    
    for dataset_name in datasets:
        filepath = OUTPUT_DIR / f'wa_{dataset_name}.csv'
        
        if not filepath.exists():
            print(f"❌ {dataset_name}: File not found")
            continue
        
        is_valid, errors = validate_dataset(dataset_name, filepath)
        
        if is_valid:
            df = pd.read_csv(filepath)
            print(f"✅ {dataset_name}: {len(df)} records, all checks passed")
            valid_datasets += 1
        else:
            print(f"❌ {dataset_name}: Validation failed")
            for error in errors:
                print(f"   - {error}")
    
    print()
    print("=" * 70)
    print(f"Validation Summary: {valid_datasets}/{total_datasets} datasets valid")
    print("=" * 70)
    
    if valid_datasets == total_datasets:
        print("\n✅ All datasets are CDM-compliant and ready for upload!")
        return 0
    else:
        print(f"\n⚠️  {total_datasets - valid_datasets} dataset(s) need attention")
        return 1


if __name__ == '__main__':
    exit(main())
