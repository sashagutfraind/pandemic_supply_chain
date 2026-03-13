#!/usr/bin/env python3
"""
Pull current stock levels from AWS Supply Chain for all sites.

NOTE: This script reads inventory data from the AWS Supply Chain Data Lake (S3 bucket).
- 1/ this assumes a single flow per table, 2/ this does not reflect the data uploaded through send_events APIs
- future implementation could use Insights events (https://docs.aws.amazon.com/aws-supply-chain/latest/adminguide/Insights.html)

"""

import boto3
import json
import pandas as pd
from pathlib import Path
from botocore.exceptions import ClientError

OUTPUT_DIR = Path(__file__).parent / "coverage-output"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)


def load_config() -> dict:
    """Load instance configuration."""
    config_path = Path(__file__).parent.parent / 'asc_instance_config.json'
    with open(config_path, 'r') as f:
        return json.load(f)


def get_current_stocks(instance_id: str, region: str) -> pd.DataFrame:
    """Query current inventory levels from AWS Supply Chain Data Lake (S3)."""
    print("Fetching current stock levels from AWS Supply Chain Data Lake...")
    
    client = boto3.client('supplychain', region_name=region)
    s3_client = boto3.client('s3', region_name=region)
    
    # Compute bucket name
    bucket_name = f"aws-supply-chain-data-{instance_id}"
    
    try:
        # Verify datasets exist in Data Lake (with pagination)
        print("  Checking Data Lake datasets...")
        datasets = []
        paginator = client.get_paginator('list_data_lake_datasets')
        page_iterator = paginator.paginate(
            instanceId=instance_id,
            namespace='asc'
        )
        
        for page in page_iterator:
            datasets.extend(page.get('datasets', []))
        
        dataset_names = [d['name'] for d in datasets]
        
        print(f"  Found {len(datasets)} datasets in Data Lake")
        
        if 'inv_level' not in dataset_names:
            print("  ✗ inv_level dataset not found in Data Lake")
            return pd.DataFrame()
        
        if 'site' not in dataset_names:
            print("  ✗ site dataset not found in Data Lake")
            return pd.DataFrame()
        
        print(f"  ✓ inv_level dataset found")
        print(f"  ✓ site dataset found")
        
        # Read data from S3 bucket (where Data Lake stores the data)
        print(f"\n  Reading from S3 bucket: {bucket_name}")
        
        # List objects in inv_level prefix
        inv_prefix = 'othersources/wa_inv_level/'
        inv_objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=inv_prefix)
        
        if 'Contents' not in inv_objects:
            print(f"  ✗ No files found in {inv_prefix}")
            return pd.DataFrame()
        
        # Find the CSV file (skip _SUCCESS markers)
        inv_file = None
        for obj in inv_objects['Contents']:
            if obj['Key'].endswith('.csv'):
                inv_file = obj['Key']
                break
        
        if not inv_file:
            print(f"  ✗ No CSV file found in {inv_prefix}")
            return pd.DataFrame()
        
        # Download and read inv_level
        inv_obj = s3_client.get_object(Bucket=bucket_name, Key=inv_file)
        inv_df = pd.read_csv(inv_obj['Body'])
        print(f"  Loaded {len(inv_df)} inventory records from Data Lake (S3)")
        
        # List objects in site prefix
        site_prefix = 'othersources/wa_site/'
        site_objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=site_prefix)
        
        if 'Contents' not in site_objects:
            print(f"  ✗ No files found in {site_prefix}")
            return pd.DataFrame()
        
        # Find the CSV file
        site_file = None
        for obj in site_objects['Contents']:
            if obj['Key'].endswith('.csv'):
                site_file = obj['Key']
                break
        
        if not site_file:
            print(f"  ✗ No CSV file found in {site_prefix}")
            return pd.DataFrame()
        
        # Download and read site
        site_obj = s3_client.get_object(Bucket=bucket_name, Key=site_file)
        site_df = pd.read_csv(site_obj['Body'])
        print(f"  Loaded {len(site_df)} site records from Data Lake (S3)")
        
        # Merge to get site details
        stocks = inv_df.merge(
            site_df[['id', 'description', 'latitude', 'longitude', 'city', 'state_prov']],
            left_on='site_id',
            right_on='id',
            how='left'
        )
        
        # Extract ZIP code from site_id
        stocks['zip_code'] = stocks['site_id'].astype(str)
        
        # Aggregate by site
        site_stocks = stocks.groupby(['site_id', 'zip_code', 'latitude', 'longitude', 'city', 'state_prov']).agg({
            'on_hand_inventory': 'sum',
            'product_id': 'count'
        }).reset_index()
        
        site_stocks.rename(columns={
            'on_hand_inventory': 'total_doses',
            'product_id': 'product_count'
        }, inplace=True)
        
        print(f"  Aggregated to {len(site_stocks)} sites")
        
        return site_stocks
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"  Error accessing AWS Supply Chain: {error_code} - {e}")
        return pd.DataFrame()


def main():
    print("="*70)
    print("Coverage Analysis - Step 3: Current Stocks")
    print("="*70)
    print()
    
    config = load_config()
    instance_id = config['instance_id']
    region = config['aws_region']
    
    # Get current stocks
    stocks_df = get_current_stocks(instance_id, region)
    
    if not stocks_df.empty:
        # Save to CSV
        output_file = OUTPUT_DIR / 'cov_3_current_stocks.csv'
        stocks_df.to_csv(output_file, index=False)
        print(f"\n✓ Saved current stocks to: {output_file}")
        
        # Display summary
        print(f"\nSummary:")
        print(f"  Total sites: {len(stocks_df)}")
        print(f"  Total doses: {stocks_df['total_doses'].sum():,.0f}")
        print(f"  Average doses per site: {stocks_df['total_doses'].mean():,.0f}")
        print(f"  Min doses: {stocks_df['total_doses'].min():,.0f}")
        print(f"  Max doses: {stocks_df['total_doses'].max():,.0f}")
    else:
        print("\n✗ No stock data available")
        return 1
    
    print("\n" + "="*70)
    print("Complete")
    print("="*70)
    
    return 0


if __name__ == '__main__':
    exit(main())
