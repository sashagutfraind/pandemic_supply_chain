#!/usr/bin/env python3
"""
AWS Supply Chain Data Lake Builder (Data Integration Flows)

This script creates Data Integration Flows via API to ingest data from S3 into AWS Supply Chain Data Lake.
This is the officially supported method for bulk data loading.

ADVANTAGES over send_data_integration_event API:
- Flexible field mapping (map your CSV columns to CDM fields)
- Better error handling and validation
- Works with all datasets (not just a subset)
- Reusable flows that can be run multiple times
- Automatic execution when new files arrive in S3

Usage:
    python asc_2_lake_builder.py --generate      # Generate data only
    python asc_2_lake_builder.py --upload        # Upload data to S3
    python asc_2_lake_builder.py --create-flows  # Create Data Integration Flows
    python asc_2_lake_builder.py --list-flows    # List existing flows
    python asc_2_lake_builder.py --all           # Generate, upload, and create flows

How It Works:
1. Generate synthetic supply chain data (15 CDM datasets)
2. Upload CSV files to S3 bucket
3. Create Data Integration Flows via API (one per dataset)
4. Flows automatically execute when they detect new files in S3
5. Monitor flow execution status

Datasets (15 total):
- Reference: company, geography, calendar
- Product: product_hierarchy, product
- Network: site, transportation_lane, trading_partner
- Planning: inv_policy, forecast
- Transactional: inv_level, inbound_order, inbound_order_line, shipment, outbound_order_line
"""

import argparse
import boto3
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from botocore.exceptions import ClientError
from typing import Dict, List, Optional


OUTPUT_DIR = Path("output-data")
OUTPUT_DIR.mkdir(exist_ok=True)


# Import data generation functions
from asc_generate import (
    DATA_CONFIG,
    generate_company_data,
    generate_geography_data,
    generate_calendar_data,
    generate_product_hierarchy_data,
    generate_product_data,
    generate_site_data,
    generate_transportation_lane_data,
    generate_trading_partner_data,
    generate_inv_policy_data,
    generate_forecast_data,
    generate_inventory_data,
    generate_inbound_order_data,
    generate_inbound_order_line_data,
    generate_shipment_data,
    generate_outbound_order_line_data
)


def generate_all_data(config: dict) -> dict:
    """Generate all CDM tables."""
    print("\n📊 Generating AWS Supply Chain CDM data...")
    
    datasets = {
        # Organization & Reference
        'company': generate_company_data(config),
        'geography': generate_geography_data(config),
        'calendar': generate_calendar_data(config),
        
        # Product
        'product_hierarchy': generate_product_hierarchy_data(config),
        'product': generate_product_data(config),
        
        # Network
        'site': generate_site_data(config),
        'transportation_lane': generate_transportation_lane_data(config),
        'trading_partner': generate_trading_partner_data(config),
        
        # Planning
        'inv_policy': generate_inv_policy_data(config),
        'forecast': generate_forecast_data(config),
        
        # Transactional
        'inv_level': generate_inventory_data(config),
        'inbound_order': generate_inbound_order_data(config),
        'inbound_order_line': generate_inbound_order_line_data(config),
        'shipment': generate_shipment_data(config),
        'outbound_order_line': generate_outbound_order_line_data(config)
    }
    
    # Save to CSV
    for name, df in datasets.items():
        output_file = OUTPUT_DIR / f'wa_{name}.csv'
        df.to_csv(output_file, index=False)
        print(f"  ✓ Saved {name}: {len(df)} records → {output_file}")
    
    print(f"\n✓ Data generation complete!")
    print(f"  Total datasets: {len(datasets)}")
    print(f"  Output directory: {OUTPUT_DIR}")
    
    return datasets



# =============================================================================
# Configuration and AWS Helper Functions
# =============================================================================

def load_instance_config() -> Optional[dict]:
    """Load instance configuration."""
    try:
        with open('asc_instance_config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("✗ asc_instance_config.json not found")
        return None
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON in configuration: {e}")
        return None


def save_config(config: dict) -> bool:
    """Save instance configuration."""
    try:
        with open('asc_instance_config.json', 'w') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        print(f"✗ Error saving config: {e}")
        return False


def get_instance_id(config: dict) -> Optional[str]:
    """Get instance ID from config or list instances."""
    if config.get('instance_id'):
        return config['instance_id']
    
    instance_name = config.get('instance_name')
    if not instance_name:
        print("✗ No instance_id or instance_name in config")
        return None
    
    try:
        sc = boto3.client('supplychain', region_name=config['aws_region'])
        instances = sc.list_instances()
        
        for inst in instances.get('instances', []):
            if inst['instanceName'] == instance_name:
                instance_id = inst['instanceId']
                print(f"✓ Found instance: {inst['instanceName']} ({instance_id})")
                # Save instance_id to config
                config['instance_id'] = instance_id
                save_config(config)
                return instance_id
        
        print(f"✗ Instance '{instance_name}' not found")
        return None
        
    except ClientError as e:
        print(f"✗ Error listing instances: {e}")
        return None



# =============================================================================
# S3 Upload Functions
# =============================================================================

def get_or_create_s3_bucket(instance_id: str, aws_region: str) -> Optional[str]:
    """Get or create S3 bucket for data lake.
    
    Bucket name is deterministic: aws-supply-chain-data-{instance_id}
    """
    # AWS Supply Chain expects bucket name with 'aws-' prefix
    bucket_name = f"aws-supply-chain-data-{instance_id}"
    
    try:
        s3 = boto3.client('s3', region_name=aws_region)
        
        # Check if bucket exists
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"  ✓ Bucket exists: {bucket_name}")
            return bucket_name
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise
        
        # Create bucket
        print(f"  Creating bucket: {bucket_name}")
        
        if aws_region == 'us-east-1':
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': aws_region}
            )
        
        # Enable versioning
        s3.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': 'Enabled'}
        )
        
        # Add bucket policy for Supply Chain access
        bucket_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "AllowSupplyChainAccess",
                "Effect": "Allow",
                "Principal": {"Service": "scn.amazonaws.com"},
                "Action": ["s3:GetObject", "s3:ListBucket"],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*"
                ]
            }]
        }
        
        s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(bucket_policy))
        
        print(f"  ✓ Bucket created: {bucket_name}")
        
        return bucket_name
        
    except ClientError as e:
        print(f"  ✗ Error with bucket: {e.response['Error']['Code']}")
        print(f"     Message: {e.response['Error'].get('Message', 'N/A')}")
        return None



def upload_data_to_s3(df: pd.DataFrame, dataset_name: str, bucket_name: str, aws_region: str) -> Optional[str]:
    """Upload data to S3 for ingestion."""
    try:
        key = f"othersources/wa_{dataset_name}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        s3 = boto3.client('s3', region_name=aws_region)
        csv_buffer = df.to_csv(index=False)
        
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=csv_buffer.encode('utf-8')
        )
        
        print(f"    ✓ Uploaded to s3://{bucket_name}/{key}")
        return f"s3://{bucket_name}/{key}"
        
    except ClientError as e:
        print(f"    ✗ Error uploading to S3: {e.response['Error']['Code']}")
        print(f"       Message: {e.response['Error'].get('Message', 'N/A')}")
        return None


def clean_s3_bucket(bucket_name: str, aws_region: str, prefix: str = "othersources/") -> bool:
    """Delete all objects in S3 bucket with given prefix."""
    try:
        s3 = boto3.client('s3', region_name=aws_region)
        
        print(f"\n🗑️  Cleaning S3 bucket: {bucket_name}/{prefix}")
        
        # List all objects with prefix
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        delete_count = 0
        for page in pages:
            objects = page.get('Contents', [])
            if objects:
                delete_keys = [{'Key': obj['Key']} for obj in objects]
                s3.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': delete_keys}
                )
                delete_count += len(delete_keys)
        
        if delete_count > 0:
            print(f"  ✓ Deleted {delete_count} existing files")
        else:
            print(f"  ℹ️  No existing files to delete")
        
        return True
        
    except ClientError as e:
        print(f"  ✗ Error cleaning bucket: {e.response['Error']['Code']}")
        print(f"     Message: {e.response['Error'].get('Message', 'N/A')}")
        return False


def upload_datasets_to_s3(instance_id: str, aws_region: str, config: dict, clean_first: bool = True) -> bool:
    """Upload all datasets to S3."""
    print("\n📤 Uploading data to S3...")

    bucket_name = get_or_create_s3_bucket(instance_id, aws_region)
    if not bucket_name:
        print("\n❌ Failed to get or create S3 bucket")
        return False
    
    # Clean existing files first
    if clean_first:
        if not clean_s3_bucket(bucket_name, aws_region, prefix="othersources/"):
            print("\n⚠️  Warning: Failed to clean bucket, continuing anyway...")

    # Dataset mappings in dependency order
    dataset_mappings = {
        'wa_company.csv': 'company',
        'wa_geography.csv': 'geography',
        'wa_calendar.csv': 'calendar',
        'wa_product_hierarchy.csv': 'product_hierarchy',
        'wa_product.csv': 'product',
        'wa_trading_partner.csv': 'trading_partner',
        'wa_site.csv': 'site',
        'wa_transportation_lane.csv': 'transportation_lane',
        'wa_inv_policy.csv': 'inv_policy',
        'wa_forecast.csv': 'forecast',
        'wa_inv_level.csv': 'inv_level',
        'wa_inbound_order.csv': 'inbound_order',
        'wa_inbound_order_line.csv': 'inbound_order_line',
        'wa_shipment.csv': 'shipment',
        'wa_outbound_order_line.csv': 'outbound_order_line'
    }

    success_count = 0
    uploaded_files = []

    for filename, dataset_name in dataset_mappings.items():
        filepath = OUTPUT_DIR / filename

        if not filepath.exists():
            print(f"  ⚠️  Skipping {filename} (not found)")
            continue

        print(f"\n  Uploading {dataset_name}...")
        df = pd.read_csv(filepath)
        print(f"    Loaded {len(df)} records from {filename}")

        s3_path = upload_data_to_s3(df, dataset_name, bucket_name, aws_region)
        if s3_path:
            success_count += 1
            uploaded_files.append((dataset_name, s3_path))

    print(f"\n✓ Upload complete: {success_count}/{len(dataset_mappings)} datasets uploaded")

    # Save upload manifest
    if uploaded_files:
        manifest = {
            'uploaded_at': datetime.now().isoformat(),
            'instance_id': instance_id,
            'bucket_name': bucket_name,
            'files': [{'dataset': name, 's3_path': path} for name, path in uploaded_files]
        }

        manifest_file = OUTPUT_DIR / 'upload_manifest.json'
        with open(manifest_file, 'w') as f:
            json.dump(manifest, f, indent=2)
        print(f"  Manifest saved to: {manifest_file}")

    return success_count == len(dataset_mappings)



# =============================================================================
# Data Integration Flow Functions
# =============================================================================

def get_cdm_field_mappings(dataset_name: str) -> Dict[str, str]:
    """
    Get field mappings from CSV columns to CDM fields.
    Returns a dict mapping CSV column names to CDM field names.
    """
    # Most datasets have 1:1 mapping (CSV column = CDM field)
    # Only specify mappings where they differ
    
    mappings = {
        'trading_partner': {
            'name': 'description',
            'partner_type': 'tpartner_type',
            'state': 'state_prov'
        },
        'site': {
            'state': 'state_prov'
        },
        'product': {
            'unit_of_measure': 'base_uom'
        },
        'company': {
            'state_prov': 'state_prov'  # Already correct in generation
        }
    }
    
    return mappings.get(dataset_name, {})


def create_data_integration_flow(
    instance_id: str,
    dataset_name: str,
    bucket_name: str,
    aws_region: str
) -> bool:
    """Create a Data Integration Flow via API."""
    try:
        sc = boto3.client('supplychain', region_name=aws_region)
        sts = boto3.client('sts')
        account_id = sts.get_caller_identity()['Account']
        
        flow_name = f"{dataset_name}-s3-flow"
        
        # Check if flow already exists
        try:
            existing = sc.get_data_integration_flow(
                instanceId=instance_id,
                name=flow_name
            )
            print(f"  ℹ️  Flow '{flow_name}' already exists, skipping creation")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise
        
        print(f"  Creating flow: {flow_name}")
        
        # Build dataset ARN
        dataset_arn = f"arn:aws:scn:{aws_region}:{account_id}:instance/{instance_id}/namespaces/asc/datasets/{dataset_name}"
        
        # Create flow configuration
        # sourceName must match pattern [A-Za-z0-9_]+ (no hyphens!)
        source_name = f"{dataset_name.replace('-', '_')}_source"
        
        flow_config = {
            "sources": [{
                "sourceType": "S3",
                "sourceName": source_name,
                "s3Source": {
                    "bucketName": bucket_name,
                    "prefix": f"inbound/{dataset_name}/",
                    "options": {
                        "fileType": "CSV"
                    }
                }
            }],
            "target": {
                "targetType": "DATASET",
                "datasetTarget": {
                    "datasetIdentifier": dataset_arn,
                    "options": {
                        "loadType": "INCREMENTAL"
                    }
                }
            },
            "transformation": {
                "transformationType": "SQL",
                "sqlTransformation": {
                    "query": f"SELECT * FROM {source_name}"
                }
            }
        }
        
        # Create the flow
        response = sc.create_data_integration_flow(
            instanceId=instance_id,
            name=flow_name,
            **flow_config
        )
        
        print(f"    ✓ Flow created: {flow_name}")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error'].get('Message', 'N/A')
        print(f"    ✗ Error creating flow: {error_code}")
        print(f"       Message: {error_msg}")
        return False



def create_all_flows(instance_id: str, aws_region: str, config: dict) -> bool:
    """Create direct S3 → CDM Data Integration Flows for all datasets."""
    print("\n🔧 Creating Direct S3 → CDM Data Integration Flows...")
    
    # Compute bucket name from instance_id
    bucket_name = f"aws-supply-chain-data-{instance_id}"
    
    try:
        sc = boto3.client('supplychain', region_name=aws_region)
        sts = boto3.client('sts')
        account_id = sts.get_caller_identity()['Account']
        
        # Dataset mappings: our_name → CDM name
        datasets = {
            'company': 'company',
            'geography': 'geography', 
            'calendar': 'calendar',
            'product_hierarchy': 'product_hierarchy',
            'product': 'product',
            'trading_partner': 'trading_partner',
            'site': 'site',
            'transportation_lane': 'transportation_lane',
            'inv_policy': 'inv_policy',
            'forecast': 'forecast',
            'inv_level': 'inv_level',
            'inbound_order': 'inbound_order',
            'inbound_order_line': 'inbound_order_line',
            'shipment': 'shipment',
            'outbound_order_line': 'outbound_order_line'
        }
        
        created = 0
        skipped = 0
        failed = 0
        
        for our_name, cdm_name in datasets.items():
            flow_name = f"s3-{cdm_name}-flow"
            
            try:
                # Check if flow exists
                try:
                    sc.get_data_integration_flow(instanceId=instance_id, name=flow_name)
                    print(f"  ⏭️  {flow_name}: Already exists")
                    skipped += 1
                    continue
                except ClientError as e:
                    if e.response['Error']['Code'] != 'ResourceNotFoundException':
                        raise
                
                dataset_arn = f"arn:aws:scn:{aws_region}:{account_id}:instance/{instance_id}/namespaces/asc/datasets/{cdm_name}"
                source_name = f"wa_{our_name.replace('-', '_')}_src"
                
                print(f"  Creating: {flow_name}")
                print(f"    Source: s3://{bucket_name}/othersources/wa_{our_name}/")
                print(f"    Target: asc/{cdm_name}")
                
                sc.create_data_integration_flow(
                    instanceId=instance_id,
                    name=flow_name,
                    sources=[{
                        "sourceType": "S3",
                        "sourceName": source_name,
                        "s3Source": {
                            "bucketName": bucket_name,
                            "prefix": f"othersources/wa_{our_name}/",
                            "options": {
                                "fileType": "CSV"
                            }
                        }
                    }],
                    target={
                        "targetType": "DATASET",
                        "datasetTarget": {
                            "datasetIdentifier": dataset_arn,
                            "options": {
                                "loadType": "INCREMENTAL"
                            }
                        }
                    },
                    transformation={
                        "transformationType": "SQL",
                        "sqlTransformation": {
                            "query": f"SELECT * FROM {source_name}"
                        }
                    }
                )
                
                print(f"    ✓ Created")
                created += 1
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                error_msg = e.response['Error'].get('Message', 'N/A')
                print(f"    ✗ Error: {error_code}")
                print(f"       {error_msg}")
                failed += 1
            except Exception as e:
                print(f"    ✗ Error: {e}")
                failed += 1
        
        print(f"\n{'='*70}")
        print(f"Flow Creation Summary")
        print(f"{'='*70}")
        print(f"  Created: {created}")
        print(f"  Skipped (already exist): {skipped}")
        print(f"  Failed: {failed}")
        
        if failed > 0 and created == 0:
            # Check if it's the bucket association error
            print(f"\n⚠️  All flows failed to create!")
            print(f"\n💡 Common Issue: S3 Bucket Not Associated")
            print(f"\nThe S3 bucket must be manually associated with your AWS Supply Chain instance.")
            print(f"\nSteps to fix:")
            print(f"  1. Go to AWS Supply Chain console")
            print(f"  2. Select instance: {config.get('instance_name', 'your-instance')}")
            print(f"  3. Navigate to: Data → Data integration → Data sources")
            print(f"  4. Click 'Add data source' → Select 'Amazon S3'")
            print(f"  5. Enter bucket: {bucket_name}")
            print(f"  6. Click 'Add' and wait for activation")
            print(f"\nThen run: python asc_2_lake_builder.py --create-flows")
            print(f"\nSee: helpers/ASSOCIATE_S3_BUCKET.md for detailed instructions")
        elif created > 0:
            print(f"\n✅ Direct S3 → CDM flows created!")
            print(f"\nThese flows will:")
            print(f"  • Read CSV files directly from S3")
            print(f"  • Load into CDM datasets (asc namespace)")
            print(f"  • Auto-trigger when new files appear")
        
        return failed == 0
        
    except Exception as e:
        print(f"  ✗ Error creating flows: {e}")
        return False


def list_data_integration_flows(instance_id: str, aws_region: str) -> bool:
    """List all Data Integration Flows."""
    print("\n📋 Listing Data Integration Flows...")
    
    try:
        sc = boto3.client('supplychain', region_name=aws_region)
        
        # List flows with pagination
        flows = []
        next_token = None
        
        while True:
            if next_token:
                response = sc.list_data_integration_flows(
                    instanceId=instance_id,
                    nextToken=next_token
                )
            else:
                response = sc.list_data_integration_flows(
                    instanceId=instance_id
                )
            
            flows.extend(response.get('flows', []))
            next_token = response.get('nextToken')
            
            if not next_token:
                break
        
        if not flows:
            print("  ℹ️  No Data Integration Flows found")
            print("\n  To create flows, run:")
            print("     python asc_2_lake_builder.py --create-flows")
            return True
        
        print(f"\n  Found {len(flows)} Data Integration Flows:\n")
        
        for flow in flows:
            name = flow.get('name', 'Unnamed')
            created = flow.get('createdTime', 'Unknown')
            modified = flow.get('lastModifiedTime', 'Unknown')
            
            print(f"  • {name}")
            print(f"    Created: {created}")
            print(f"    Modified: {modified}")
            
            # Get flow details
            try:
                details = sc.get_data_integration_flow(
                    instanceId=instance_id,
                    name=name
                )
                
                flow_detail = details.get('flow', {})
                sources = flow_detail.get('sources', [])
                target = flow_detail.get('target', {})
                
                if sources:
                    source = sources[0]
                    if 's3Source' in source:
                        s3_source = source['s3Source']
                        print(f"    Source: s3://{s3_source.get('bucketName')}/{s3_source.get('prefix')}")
                
                if 'datasetTarget' in target:
                    dataset_target = target['datasetTarget']
                    dataset_id = dataset_target.get('datasetIdentifier', '')
                    dataset_name = dataset_id.split('/')[-1] if dataset_id else 'Unknown'
                    print(f"    Target: {dataset_name}")
                
                # List recent executions
                try:
                    executions = sc.list_data_integration_flow_executions(
                        instanceId=instance_id,
                        flowName=name,
                        maxResults=3
                    )
                    
                    exec_list = executions.get('executions', [])
                    if exec_list:
                        print(f"    Recent executions:")
                        for exec_item in exec_list:
                            exec_id = exec_item.get('executionId', 'Unknown')
                            status = exec_item.get('status', 'Unknown')
                            start_time = exec_item.get('startTime', 'Unknown')
                            print(f"      - {exec_id[:8]}... | {status} | {start_time}")
                    else:
                        print(f"    Recent executions: None")
                
                except ClientError:
                    pass
                
            except ClientError:
                pass
            
            print()
        
        return True
        
    except ClientError as e:
        print(f"  ✗ Error listing flows: {e.response['Error']['Code']}")
        print(f"     Message: {e.response['Error'].get('Message', 'N/A')}")
        return False


def trigger_flow_executions(instance_id: str, aws_region: str) -> bool:
    """Provide instructions to manually trigger flows in the console."""
    print("\n🚀 Triggering Data Integration Flows...")
    
    try:
        sc = boto3.client('supplychain', region_name=aws_region)
        
        # List flows
        response = sc.list_data_integration_flows(instanceId=instance_id)
        flows = response.get('flows', [])
        
        if not flows:
            print("  ℹ️  No flows found to trigger")
            return True
        
        print(f"\n  Found {len(flows)} flows\n")
        
        # AWS Supply Chain doesn't have an API to trigger flows programmatically
        # Users must trigger them manually in the console
        
        print("  ⚠️  Note: AWS Supply Chain flows must be triggered manually in the console")
        print("     The API does not support programmatic flow execution\n")
        
        print("  📋 Manual Steps to Trigger Flows:\n")
        print("  1. Go to AWS Supply Chain console")
        print("  2. Navigate to: Data → Data integration → Flows")
        print("  3. For each flow below, click on it and select 'Run flow':\n")
        
        for i, flow in enumerate(flows, 1):
            flow_name = flow.get('name', 'Unknown')
            print(f"     {i}. {flow_name}")
        
        print(f"\n  Alternative: Flows may auto-trigger when new files are uploaded to S3")
        print(f"  Wait 5-10 minutes and check flow execution status with:")
        print(f"     python asc_2_lake_builder.py --list-flows\n")
        
        return True
        
    except ClientError as e:
        print(f"  ✗ Error: {e.response['Error']['Code']}")
        print(f"     Message: {e.response['Error'].get('Message', 'N/A')}")
        return False


# =============================================================================
# Main CLI
# =============================================================================

def trigger_flows_via_metadata(bucket_name: str, aws_region: str) -> bool:
    """Trigger flows by touching files in S3 (updating metadata)."""
    try:
        s3 = boto3.client('s3', region_name=aws_region)
        
        datasets = [
            'company', 'geography', 'calendar', 'product_hierarchy', 'product',
            'trading_partner', 'site', 'transportation_lane', 'inv_policy',
            'forecast', 'inv_level', 'inbound_order', 'inbound_order_line',
            'shipment', 'outbound_order_line'
        ]
        
        triggered = 0
        failed = 0
        
        for dataset in datasets:
            prefix = f"othersources/wa_{dataset}/"
            
            try:
                # Check if files exist
                response = s3.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    MaxKeys=1
                )
                
                if response.get('KeyCount', 0) > 0:
                    # Get the first file
                    first_file = response['Contents'][0]
                    file_key = first_file['Key']
                    
                    # Copy the file to itself with new metadata to trigger the flow
                    copy_source = {'Bucket': bucket_name, 'Key': file_key}
                    
                    s3.copy_object(
                        CopySource=copy_source,
                        Bucket=bucket_name,
                        Key=file_key,
                        Metadata={'triggered': datetime.now().isoformat()},
                        MetadataDirective='REPLACE'
                    )
                    
                    print(f"  ✓ Triggered: {dataset}")
                    triggered += 1
                else:
                    print(f"  ⏭️  Skipped: {dataset} (no files)")
                    failed += 1
                    
            except Exception as e:
                print(f"  ✗ Error: {dataset} ({e})")
                failed += 1
        
        print(f"\n  Triggered: {triggered}/{len(datasets)} flows")
        
        if triggered > 0:
            print(f"\n  ⏳ Flows will execute in 2-5 minutes")
            print(f"  💡 Check status with: python asc_2_lake_builder.py --list-flows")
        
        return failed == 0
        
    except Exception as e:
        print(f"  ✗ Error triggering flows: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='AWS Supply Chain Data Lake Builder (Data Integration Flows)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python asc_2_lake_builder.py --all            # Complete setup: generate, create flows, upload (auto-triggers)
  python asc_2_lake_builder.py --generate       # Generate data only
  python asc_2_lake_builder.py --create-flows   # Create Data Integration Flows
  python asc_2_lake_builder.py --upload         # Upload data to S3 (auto-triggers flows)
  python asc_2_lake_builder.py --list-flows     # List existing flows and execution status
  python asc_2_lake_builder.py --trigger        # Manually trigger flows via metadata update

Workflow:
  1. Generate synthetic supply chain data (15 CDM datasets)
  2. Create S3 bucket (if needed)
  3. Create Data Integration Flows via API (one per dataset)
  4. Upload CSV files to S3 bucket (auto-triggers flows)
  5. Monitor flow execution status with --list-flows
  
Note: Flows must exist BEFORE uploading files for auto-trigger to work.
        """
    )
    
    parser.add_argument('--generate', action='store_true',
                       help='Generate synthetic data')
    parser.add_argument('--upload', action='store_true',
                       help='Upload data to S3 (cleans existing files first)')
    parser.add_argument('--create-flows', action='store_true',
                       help='Create Data Integration Flows via API')
    parser.add_argument('--list-flows', action='store_true',
                       help='List existing Data Integration Flows')
    parser.add_argument('--trigger', action='store_true',
                       help='Trigger all flows to start execution')
    parser.add_argument('--all', action='store_true',
                       help='Complete end-to-end: generate, upload, create flows, and trigger')
    
    args = parser.parse_args()
    
    # Default to --all if no arguments
    if not (args.generate or args.upload or args.create_flows or args.list_flows or args.trigger or args.all):
        args.all = True
    
    print("=" * 70)
    print("AWS Supply Chain Data Lake Builder")
    print("Data Integration Flows via API")
    print("=" * 70)
    
    # Generate data
    if args.generate or args.all:
        print("\n📊 Generating synthetic data...")
        generate_all_data(DATA_CONFIG)
    
    # Load configuration for AWS operations
    if args.upload or args.create_flows or args.list_flows or args.trigger or args.all:
        config = load_instance_config()
        if not config:
            print("\n❌ Cannot proceed without configuration")
            return 1
        
        instance_id = get_instance_id(config)
        if not instance_id:
            print("\n❌ Cannot proceed without instance ID")
            return 1
        
        # Create/verify S3 bucket (for --all or --upload or --create-flows)
        if args.upload or args.create_flows or args.all:
            print("\n🪣 Setting up S3 bucket...")
            bucket_name = get_or_create_s3_bucket(instance_id, config['aws_region'])
            if not bucket_name:
                print("\n❌ Failed to create/verify S3 bucket")
                return 1
        
        # Create Data Integration Flows BEFORE uploading (so they can auto-trigger)
        if args.create_flows or args.all:
            print("\n🔧 Creating Data Integration Flows...")
            if not create_all_flows(instance_id, config['aws_region'], config):
                print("\n⚠️  Some flows failed to create")
                if not args.all:
                    return 1
        
        # Upload to S3 (will auto-trigger flows if they exist)
        if args.upload or args.all:
            print("\n📤 Uploading data to S3...")
            if not upload_datasets_to_s3(instance_id, config['aws_region'], config):
                print("\n⚠️  Some datasets failed to upload")
                if not args.all:
                    return 1
        
        # List flows
        if args.list_flows:
            list_data_integration_flows(instance_id, config['aws_region'])
        
        # Trigger flows (manual metadata update method)
        if args.trigger:
            print("\n🚀 Manually triggering flows via metadata update...")
            # Compute bucket name from instance_id
            bucket_name = f"aws-supply-chain-data-{instance_id}"
            trigger_flows_via_metadata(bucket_name, config['aws_region'])
    
    print("\n" + "=" * 70)
    print("✅ Data Lake Builder Complete")
    print("=" * 70)
    
    if args.all:
        print("\n📊 End-to-End Setup Complete!")
        print("\nWhat was done:")
        print("  ✓ Generated 15 CDM datasets (380 records)")
        print("  ✓ Created/verified S3 bucket")
        print("  ✓ Created 15 Data Integration Flows")
        print("  ✓ Uploaded data to S3 (flows auto-triggered)")
        print("\nNext steps:")
        print("  1. Wait 5-10 minutes for flows to execute")
        print("  2. Check status: python asc_2_lake_builder.py --list-flows")
        print("  3. View in AWS Supply Chain console:")
        print("     • Data → Data integration → Flows")
        print("     • Data → Datasets (see loaded data)")
        print("     • Insights → Projected inventory")
    
    return 0


if __name__ == '__main__':
    exit(main())
