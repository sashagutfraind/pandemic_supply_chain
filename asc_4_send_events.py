#!/usr/bin/env python3
"""
AWS Supply Chain Data Sender (send_data_integration_event API)

This script uses the send_data_integration_event API to ingest data into AWS Supply Chain.
This API is designed for real-time event streaming, not bulk data loading.

LIMITATIONS:
- Only works with specific datasets that have strict schemas (at times different from CDM)
- Use asc_2_lake_builder.py initially, which creates Data Integration Flows via API.
Data Integration Flows are the officially supported method for bulk data loading.

Usage:
    python asc_send_data.py --generate    # Generate data only
    python asc_send_data.py --send        # Send existing data via API
    python asc_send_data.py --all         # Generate and send

[guess] Compatible Datasets (9 out of 15):
- company, geography, product_hierarchy, product
- trading_partner, site, inv_policy
- inv_level, inbound_order, inbound_order_line

[guess] Incompatible Datasets (6 out of 15):
- calendar, transportation_lane, forecast
- shipment, outbound_order_line
(These require Data Integration Flows)
"""

import argparse
import boto3
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from botocore.exceptions import ClientError

# Import data generation functions
from asc_generate import DATA_CONFIG, generate_all_data


OUTPUT_DIR = Path("output-data")
EVENT_OUTPUT_DIR = Path("output-data/events")
EVENT_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)


def load_instance_config() -> dict:
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


def get_instance_id(config: dict) -> str | None:
    """Get instance ID from config."""
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
                print(f"✓ Found instance: {inst['instanceName']} ({inst['instanceId']})")
                return inst['instanceId']
        
        print(f"✗ Instance '{instance_name}' not found")
        return None
        
    except ClientError as e:
        print(f"✗ Error listing instances: {e}")
        return None


def transform_to_cdm(dataset_name: str, data: pd.DataFrame) -> list[dict]:
    """Transform data to AWS Supply Chain CDM format for send_data_integration_event API.
    
    IMPORTANT: connection_id is auto-populated by AWS and must NOT be included in the payload.
    """
    records = data.to_dict('records')
    
    # Remove connection_id from ALL records (it's auto-populated by AWS)
    for record in records:
        record.pop('connection_id', None)

    if dataset_name == 'company':
        # Company API only accepts: id, description
        for record in records:
            # Keep only required fields
            transformed = {
                'id': str(record['id']),
                'description': str(record['description'])
            }
            # Clear and replace
            record.clear()
            record.update(transformed)

    elif dataset_name == 'trading_partner':
        for record in records:
            record['tpartner_type'] = record.pop('partner_type', 'SUPPLIER')
            record['geo_id'] = 'SCN_RESERVED_NO_VALUE_PROVIDED'
            record['eff_start_date'] = '1900-01-01T00:00:00Z'
            record['eff_end_date'] = '9999-12-31T23:59:59Z'
            if 'name' in record:
                record['description'] = record.pop('name')
            if 'state' in record:
                record['state_prov'] = record.pop('state')

    elif dataset_name == 'site':
        for record in records:
            record['id'] = str(record['id'])
            if 'state' in record:
                record['state_prov'] = record.pop('state')
            # Remove custom fields
            record.pop('custom_total_pop', None)
            record.pop('custom_median_income', None)
            record.pop('custom_poverty_rate', None)

    elif dataset_name == 'product':
        for record in records:
            record.pop('category', None)
            record.pop('manufacturer', None)
            if 'unit_of_measure' in record:
                record['base_uom'] = record.pop('unit_of_measure')

    elif dataset_name == 'inv_level':
        transformed = []
        for record in records:
            new_record = {
                'snapshot_date': f"{record['snapshot_date']}T00:00:00Z",
                'site_id': str(record['site_id']),
                'product_id': str(record['product_id']),
                'on_hand_inventory': str(record['on_hand_inventory']),
                'inv_condition': str(record['inv_condition']),
                'lot_number': str(record['lot_number'])
            }
            if 'expiration_date' in record and pd.notna(record['expiration_date']):
                new_record['expiry_date'] = f"{record['expiration_date']}T00:00:00Z"
            transformed.append(new_record)
        return transformed

    elif dataset_name == 'inbound_order':
        transformed = []
        for record in records:
            new_record = {
                'id': str(record['id']),
                'tpartner_id': str(record['tpartner_id'])
            }
            if 'order_type' in record and pd.notna(record['order_type']):
                new_record['order_type'] = str(record['order_type'])
            if 'order_status' in record and pd.notna(record['order_status']):
                new_record['order_status'] = str(record['order_status'])
            if 'destination_site_id' in record and pd.notna(record['destination_site_id']):
                new_record['to_site_id'] = str(record['destination_site_id'])
            transformed.append(new_record)
        return transformed

    elif dataset_name == 'inbound_order_line':
        transformed = []
        for record in records:
            new_record = {
                'id': str(record['id']),
                'order_id': str(record['order_id']),
                'tpartner_id': str(record['tpartner_id']),
                'product_id': str(record['product_id'])
            }
            if 'to_site_id' in record and pd.notna(record['to_site_id']):
                new_record['to_site_id'] = str(record['to_site_id'])
            if 'quantity_submitted' in record and pd.notna(record['quantity_submitted']):
                new_record['quantity_submitted'] = str(record['quantity_submitted'])
            if 'quantity_uom' in record and pd.notna(record['quantity_uom']):
                new_record['quantity_uom'] = str(record['quantity_uom'])
            if 'status' in record and pd.notna(record['status']):
                new_record['status'] = str(record['status'])
            if 'expected_delivery_date' in record and pd.notna(record['expected_delivery_date']):
                new_record['expected_delivery_date'] = f"{record['expected_delivery_date']}T00:00:00Z"
            transformed.append(new_record)
        return transformed

    elif dataset_name == 'inv_policy':
        for record in records:
            record['site_id'] = str(record['site_id'])
            record['product_id'] = str(record['product_id'])
            if 'eff_start_date' in record:
                record['eff_start_date'] = f"{record['eff_start_date']}T00:00:00Z"
            if 'eff_end_date' in record:
                record['eff_end_date'] = f"{record['eff_end_date']}T23:59:59Z"
    
    elif dataset_name == 'geography':
        for record in records:
            if 'parent_geo_id' in record and pd.isna(record['parent_geo_id']):
                record.pop('parent_geo_id')
    
    elif dataset_name == 'product_hierarchy':
        for record in records:
            if 'parent_product_group_id' in record and pd.isna(record['parent_product_group_id']):
                record.pop('parent_product_group_id')
    
    elif dataset_name == 'company':
        for record in records:
            if 'state' in record:
                record['state_prov'] = record.pop('state')

    return records


def send_data_integration_event(
    instance_id: str,
    dataset_name: str,
    data: pd.DataFrame,
    aws_region: str
) -> bool:
    """Send data to AWS Supply Chain via send_data_integration_event API."""
    try:
        sc = boto3.client('supplychain', region_name=aws_region)
        sts = boto3.client('sts')

        account_id = sts.get_caller_identity()['Account']
        records = transform_to_cdm(dataset_name, data)

        print(f"    Sending {len(records)} records to {dataset_name}...")

        event_group_id = f"{dataset_name}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        dataset_arn = f"arn:aws:scn:{aws_region}:{account_id}:instance/{instance_id}/namespaces/asc/datasets/{dataset_name}"

        # Map dataset names to event types
        event_type_map = {
            'inv_level': 'scn.data.inventorylevel',
            'inbound_order': 'scn.data.inboundorder',
            'inbound_order_line': 'scn.data.inboundorderline',
        }
        event_type = event_type_map.get(dataset_name, 'scn.data.dataset')

        # Send data in batches
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]

            if event_type == 'scn.data.dataset':
                response = sc.send_data_integration_event(
                    instanceId=instance_id,
                    eventType=event_type,
                    eventGroupId=event_group_id,
                    data=json.dumps(batch),
                    datasetTarget={
                        'datasetIdentifier': dataset_arn,
                        'operationType': 'UPSERT'
                    }
                )
            else:
                response = sc.send_data_integration_event(
                    instanceId=instance_id,
                    eventType=event_type,
                    eventGroupId=event_group_id,
                    data=json.dumps(batch)
                )

            print(f"      Batch {i//batch_size + 1}: {len(batch)} records sent (eventId: {response.get('eventId', 'N/A')})")

        print(f"    ✓ Data sent to {dataset_name}")
        return True

    except ClientError as e:
        print(f"    ✗ Error sending data: {e.response['Error']['Code']}")
        print(f"       Message: {e.response['Error'].get('Message', 'N/A')}")
        return False


def send_datasets(instance_id: str, aws_region: str) -> bool:
    """Send datasets via send_data_integration_event API."""
    print("\n📤 Sending data via send_data_integration_event API...")
    print("   Note: Only compatible datasets will be sent")
    print("   Incompatible datasets require Data Integration Flows")

    # Only datasets that work with send_data_integration_event API
    dataset_mappings = {
        'wa_company_event.csv': 'company',
        'wa_geography_event.csv': 'geography',
        'wa_product_hierarchy_event.csv': 'product_hierarchy',
        'wa_product_event.csv': 'product',
        'wa_trading_partner_event.csv': 'trading_partner',
        'wa_site_event.csv': 'site',
        'wa_inv_policy_event.csv': 'inv_policy',
        'wa_inv_level_event.csv': 'inv_level',
        'wa_inbound_order_event.csv': 'inbound_order',
        'wa_inbound_order_line_event.csv': 'inbound_order_line',
    }

    success_count = 0

    for filename, dataset_name in dataset_mappings.items():
        filepath = EVENT_OUTPUT_DIR / filename

        if not filepath.exists():
            print(f"  ⚠️  Skipping {filename} (not found)")
            continue

        print(f"\n  Sending {dataset_name}...")
        df = pd.read_csv(filepath)
        print(f"    Loaded {len(df)} records from {filename}")

        if send_data_integration_event(instance_id, dataset_name, df, aws_region):
            success_count += 1

    print(f"\n✓ Send complete: {success_count}/{len(dataset_mappings)} datasets sent")

    if success_count == len(dataset_mappings):
        print("\n  ✅ All compatible datasets sent successfully!")
        print("\n  ⚠️  IMPORTANT: Incompatible datasets (5) require Data Integration Flows:")
        print("     - calendar, transportation_lane, forecast")
        print("     - shipment, outbound_order_line")
        print("\n  📋 Use asc_2_lake_builder.py to create Data Integration Flows")

    return success_count == len(dataset_mappings)


def main():
    parser = argparse.ArgumentParser(
        description='AWS Supply Chain Data Sender (send_data_integration_event API)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--generate', action='store_true', help='Generate synthetic data')
    parser.add_argument('--send', action='store_true', help='Send data via API')
    parser.add_argument('--all', action='store_true', help='Generate and send')
    
    args = parser.parse_args()
    
    if not (args.generate or args.send or args.all):
        args.all = True
    
    print("=" * 70)
    print("AWS Supply Chain Data Sender (send_data_integration_event API)")
    print("=" * 70)
    
    if args.generate or args.all:
        print("\n📊 Generating data for event API...")
        # Generate data with different filenames
        datasets = generate_all_data(DATA_CONFIG)
        
        # Save with _event suffix to distinguish from flow-based files
        for name, df in datasets.items():
            output_file = EVENT_OUTPUT_DIR / f'wa_{name}_event.csv'
            df.to_csv(output_file, index=False)
            print(f"  ✓ Saved {name}: {len(df)} records → {output_file}")
        
        print(f"\n✓ Data generation complete!")
        print(f"  Output directory: {EVENT_OUTPUT_DIR}")
    
    if args.send or args.all:
        config = load_instance_config()
        if not config:
            print("\n❌ Cannot proceed without configuration")
            return 1
        
        instance_id = get_instance_id(config)
        if not instance_id:
            print("\n❌ Cannot proceed without instance ID")
            return 1
        
        if not send_datasets(instance_id, config['aws_region']):
            print("\n⚠️  Some datasets failed to send")
    
    print("\n" + "=" * 70)
    print("✅ Complete")
    print("=" * 70)
    
    return 0


if __name__ == '__main__':
    exit(main())
