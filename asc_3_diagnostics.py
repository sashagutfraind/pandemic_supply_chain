#!/usr/bin/env python3
"""
AWS Supply Chain Instance Diagnostics

Comprehensive status report for AWS Supply Chain instance including:
- Instance details
- Data Integration Flow execution status
- Data Lake datasets and schemas
- ETL errors and messages
- Summary statistics

Usage:
    python asc_3_diagnostics.py
"""

import boto3
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from botocore.exceptions import ClientError


def load_config() -> dict:
    """Load instance configuration."""
    with open('asc_instance_config.json', 'r') as f:
        return json.load(f)


def get_instance_details(instance_id: str, region: str) -> Optional[dict]:
    """Get instance information."""
    print("="*70)
    print("INSTANCE DETAILS")
    print("="*70)
    
    try:
        sc = boto3.client('supplychain', region_name=region)
        response = sc.get_instance(instanceId=instance_id)
        instance = response.get('instance', {})
        
        print(f"Instance ID: {instance.get('instanceId', 'N/A')}")
        print(f"Instance Name: {instance.get('instanceName', 'N/A')}")
        print(f"Instance Description: {instance.get('instanceDescription', 'N/A')}")
        print(f"State: {instance.get('state', 'N/A')}")
        print(f"Web App DNS: {instance.get('webAppDnsDomain', 'N/A')}")
        print(f"Created: {instance.get('createdTime', 'N/A')}")
        print(f"Last Modified: {instance.get('lastModifiedTime', 'N/A')}")
        
        return instance
        
    except ClientError as e:
        print(f"Error: {e.response['Error']['Code']} - {e.response['Error'].get('Message', 'N/A')}")
        return None


def list_data_lake_namespaces(instance_id: str, region: str) -> List[dict]:
    """List all data lake namespaces."""
    print("\n" + "="*70)
    print("DATA LAKE NAMESPACES")
    print("="*70)
    
    try:
        sc = boto3.client('supplychain', region_name=region)
        
        namespaces = []
        next_token = None
        
        while True:
            if next_token:
                response = sc.list_data_lake_namespaces(
                    instanceId=instance_id,
                    nextToken=next_token
                )
            else:
                response = sc.list_data_lake_namespaces(instanceId=instance_id)
            
            namespaces.extend(response.get('namespaces', []))
            next_token = response.get('nextToken')
            
            if not next_token:
                break
        
        print(f"Total namespaces: {len(namespaces)}\n")
        
        for ns in namespaces:
            print(f"  • {ns.get('name', 'N/A')}")
            print(f"    Description: {ns.get('description', 'N/A')}")
            print(f"    Created: {ns.get('createdTime', 'N/A')}")
            print(f"    Modified: {ns.get('lastModifiedTime', 'N/A')}")
        
        return namespaces
        
    except ClientError as e:
        print(f"Error: {e.response['Error']['Code']}")
        return []


def list_datasets_in_namespace(instance_id: str, namespace: str, region: str) -> List[dict]:
    """List all datasets in a namespace with detailed information."""
    print(f"\n" + "="*70)
    print(f"DATASETS IN NAMESPACE: {namespace}")
    print("="*70)
    
    try:
        sc = boto3.client('supplychain', region_name=region)
        
        datasets = []
        next_token = None
        
        while True:
            if next_token:
                response = sc.list_data_lake_datasets(
                    instanceId=instance_id,
                    namespace=namespace,
                    nextToken=next_token
                )
            else:
                response = sc.list_data_lake_datasets(
                    instanceId=instance_id,
                    namespace=namespace
                )
            
            datasets.extend(response.get('datasets', []))
            next_token = response.get('nextToken')
            
            if not next_token:
                break
        
        print(f"Total datasets: {len(datasets)}\n")
        
        for ds in datasets:
            name = ds.get('name', 'N/A')
            created = ds.get('createdTime', 'N/A')
            modified = ds.get('lastModifiedTime', 'N/A')
            
            print(f"  • {name}")
            print(f"    Created: {created}")
            print(f"    Modified: {modified}")
            
            # Get detailed dataset info including schema
            try:
                ds_detail = sc.get_data_lake_dataset(
                    instanceId=instance_id,
                    namespace=namespace,
                    name=name
                )
                
                dataset_info = ds_detail.get('dataset', {})
                schema = dataset_info.get('schema', {})
                fields = schema.get('fields', [])
                
                print(f"    Fields: {len(fields)}")
                
                # Show primary keys if any
                primary_keys = schema.get('primaryKeys', [])
                if primary_keys:
                    pk_names = [pk.get('name') for pk in primary_keys]
                    print(f"    Primary keys: {', '.join(pk_names)}")
                
            except ClientError:
                pass
            
            print()
        
        return datasets
        
    except ClientError as e:
        print(f"Error: {e.response['Error']['Code']} - {e.response['Error'].get('Message', 'N/A')}")
        return []


def get_flow_execution_details(instance_id: str, region: str) -> Tuple[List[dict], Dict[str, List[dict]]]:
    """Get detailed flow execution information."""
    print("\n" + "="*70)
    print("DATA INTEGRATION FLOWS")
    print("="*70)
    
    try:
        sc = boto3.client('supplychain', region_name=region)
        
        # List all flows
        flows = []
        next_token = None
        
        while True:
            if next_token:
                response = sc.list_data_integration_flows(
                    instanceId=instance_id,
                    nextToken=next_token
                )
            else:
                response = sc.list_data_integration_flows(instanceId=instance_id)
            
            flows.extend(response.get('flows', []))
            next_token = response.get('nextToken')
            
            if not next_token:
                break
        
        print(f"Total flows: {len(flows)}\n")
        
        flow_executions = {}
        
        for flow in flows:
            flow_name = flow['name']
            print(f"  Flow: {flow_name}")
            print(f"    Created: {flow.get('createdTime', 'N/A')}")
            print(f"    Modified: {flow.get('lastModifiedTime', 'N/A')}")
            
            # Get flow details
            try:
                flow_detail = sc.get_data_integration_flow(
                    instanceId=instance_id,
                    name=flow_name
                )
                
                flow_info = flow_detail.get('flow', {})
                sources = flow_info.get('sources', [])
                target = flow_info.get('target', {})
                
                if sources and sources[0].get('s3Source'):
                    s3_src = sources[0]['s3Source']
                    print(f"    Source: s3://{s3_src.get('bucketName')}/{s3_src.get('prefix')}")
                
                if target.get('datasetTarget'):
                    ds_target = target['datasetTarget']
                    dataset_arn = ds_target.get('datasetIdentifier', '')
                    dataset_name = dataset_arn.split('/')[-1] if dataset_arn else 'N/A'
                    load_type = ds_target.get('options', {}).get('loadType', 'N/A')
                    print(f"    Target: {dataset_name} (loadType: {load_type})")
                
            except ClientError:
                pass
            
            # Get executions
            try:
                executions = []
                exec_next_token = None
                
                while True:
                    if exec_next_token:
                        exec_response = sc.list_data_integration_flow_executions(
                            instanceId=instance_id,
                            flowName=flow_name,
                            maxResults=10,
                            nextToken=exec_next_token
                        )
                    else:
                        exec_response = sc.list_data_integration_flow_executions(
                            instanceId=instance_id,
                            flowName=flow_name,
                            maxResults=10
                        )
                    
                    executions.extend(exec_response.get('executions', []))
                    exec_next_token = exec_response.get('nextToken')
                    
                    if not exec_next_token:
                        break
                
                flow_executions[flow_name] = executions
                
                if executions:
                    print(f"    Executions: {len(executions)}")
                    
                    # Show recent executions
                    for i, exec_item in enumerate(executions[:3]):
                        exec_id = exec_item.get('executionId', 'N/A')
                        status = exec_item.get('status', 'N/A')
                        start_time = exec_item.get('startTime', 'N/A')
                        end_time = exec_item.get('endTime', 'N/A')
                        
                        print(f"      [{i+1}] {exec_id[:16]}... | {status}")
                        print(f"          Start: {start_time}")
                        if end_time != 'N/A':
                            print(f"          End: {end_time}")
                        
                        # Get execution details for errors
                        if status == 'FAILED':
                            try:
                                exec_details = sc.get_data_integration_flow_execution(
                                    instanceId=instance_id,
                                    flowName=flow_name,
                                    executionId=exec_item['executionId']
                                )
                                
                                execution = exec_details.get('execution', {})
                                messages = execution.get('messages', [])
                                
                                if messages:
                                    print(f"          Messages:")
                                    for msg in messages[:3]:
                                        print(f"            - {msg.get('message', 'N/A')}")
                                
                                statistics = execution.get('statistics', {})
                                if statistics:
                                    print(f"          Statistics:")
                                    print(f"            Records processed: {statistics.get('recordsProcessed', 0)}")
                                    print(f"            Records failed: {statistics.get('recordsFailed', 0)}")
                            except:
                                pass
                else:
                    print(f"    Executions: 0 (not yet triggered)")
                
            except ClientError as e:
                print(f"    Error listing executions: {e.response['Error']['Code']}")
                flow_executions[flow_name] = []
            
            print()
        
        return flows, flow_executions
        
    except ClientError as e:
        print(f"Error: {e.response['Error']['Code']}")
        return [], {}


def get_execution_statistics(flow_executions: Dict[str, List[dict]]) -> dict:
    """Calculate execution statistics across all flows."""
    print("="*70)
    print("EXECUTION STATISTICS")
    print("="*70)
    
    stats = {
        'total_flows': len(flow_executions),
        'flows_with_executions': 0,
        'flows_without_executions': 0,
        'total_executions': 0,
        'succeeded': 0,
        'failed': 0,
        'running': 0,
        'other': 0
    }
    
    for flow_name, executions in flow_executions.items():
        if executions:
            stats['flows_with_executions'] += 1
            stats['total_executions'] += len(executions)
            
            for exec_item in executions:
                status = exec_item.get('status', 'UNKNOWN')
                if status == 'SUCCEEDED':
                    stats['succeeded'] += 1
                elif status == 'FAILED':
                    stats['failed'] += 1
                elif status in ['RUNNING', 'IN_PROGRESS']:
                    stats['running'] += 1
                else:
                    stats['other'] += 1
        else:
            stats['flows_without_executions'] += 1
    
    print(f"Total flows: {stats['total_flows']}")
    print(f"Flows with executions: {stats['flows_with_executions']}")
    print(f"Flows without executions: {stats['flows_without_executions']}")
    print(f"\nTotal executions: {stats['total_executions']}")
    print(f"  Succeeded: {stats['succeeded']}")
    print(f"  Failed: {stats['failed']}")
    print(f"  Running: {stats['running']}")
    print(f"  Other: {stats['other']}")
    
    return stats


def list_failed_executions(instance_id: str, flow_executions: Dict[str, List[dict]], region: str) -> None:
    """List all failed executions with error details."""
    print("\n" + "="*70)
    print("FAILED EXECUTIONS")
    print("="*70)
    
    sc = boto3.client('supplychain', region_name=region)
    
    failed_count = 0
    
    for flow_name, executions in flow_executions.items():
        for exec_item in executions:
            if exec_item.get('status') == 'FAILED':
                failed_count += 1
                exec_id = exec_item.get('executionId', 'N/A')
                
                print(f"\nFlow: {flow_name}")
                print(f"Execution ID: {exec_id}")
                print(f"Start Time: {exec_item.get('startTime', 'N/A')}")
                print(f"End Time: {exec_item.get('endTime', 'N/A')}")
                
                # Get detailed error information
                try:
                    exec_details = sc.get_data_integration_flow_execution(
                        instanceId=instance_id,
                        flowName=flow_name,
                        executionId=exec_item['executionId']
                    )
                    
                    execution = exec_details.get('execution', {})
                    
                    # Messages
                    messages = execution.get('messages', [])
                    if messages:
                        print(f"Messages:")
                        for msg in messages:
                            msg_type = msg.get('type', 'N/A')
                            msg_text = msg.get('message', 'N/A')
                            print(f"  [{msg_type}] {msg_text}")
                    
                    # Statistics
                    statistics = execution.get('statistics', {})
                    if statistics:
                        print(f"Statistics:")
                        print(f"  Records processed: {statistics.get('recordsProcessed', 0)}")
                        print(f"  Records failed: {statistics.get('recordsFailed', 0)}")
                        print(f"  Bytes processed: {statistics.get('bytesProcessed', 0)}")
                    
                except ClientError as e:
                    print(f"  Error getting details: {e.response['Error']['Code']}")
    
    if failed_count == 0:
        print("\nNo failed executions found")


def list_data_integration_events(instance_id: str, region: str) -> None:
    """List recent data integration events."""
    print("\n" + "="*70)
    print("DATA INTEGRATION EVENTS (Recent)")
    print("="*70)
    
    try:
        sc = boto3.client('supplychain', region_name=region)
        
        response = sc.list_data_integration_events(
            instanceId=instance_id,
            maxResults=20
        )
        
        events = response.get('events', [])
        
        if not events:
            print("No recent events found")
            return
        
        print(f"Showing {len(events)} most recent events:\n")
        
        for event in events:
            event_id = event.get('eventId', 'N/A')
            event_type = event.get('eventType', 'N/A')
            event_time = event.get('eventTimestamp', 'N/A')
            
            print(f"  Event ID: {event_id}")
            print(f"  Type: {event_type}")
            print(f"  Timestamp: {event_time}")
            print()
        
    except ClientError as e:
        print(f"Error: {e.response['Error']['Code']}")


def check_s3_bucket_status(instance_id: str, region: str) -> None:
    """Check S3 bucket and file status."""
    print("\n" + "="*70)
    print("S3 BUCKET STATUS")
    print("="*70)
    
    bucket_name = f"aws-supply-chain-data-{instance_id}"
    
    try:
        s3 = boto3.client('s3', region_name=region)
        
        # Check bucket exists
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"Bucket: {bucket_name}")
            print(f"Status: EXISTS")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"Bucket: {bucket_name}")
                print(f"Status: NOT FOUND")
                return
            else:
                raise
        
        # Count files by dataset
        print(f"\nFiles by dataset:")
        
        datasets = [
            'company', 'geography', 'calendar', 'product_hierarchy', 'product',
            'trading_partner', 'site', 'transportation_lane', 'inv_policy',
            'forecast', 'inv_level', 'inbound_order', 'inbound_order_line',
            'shipment', 'outbound_order_line'
        ]
        
        total_files = 0
        total_size = 0
        
        for dataset in datasets:
            prefix = f"othersources/wa_{dataset}/"
            
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
            
            files = response.get('Contents', [])
            file_count = len(files)
            dataset_size = sum(f.get('Size', 0) for f in files)
            
            total_files += file_count
            total_size += dataset_size
            
            if file_count > 0:
                latest_file = max(files, key=lambda x: x.get('LastModified', datetime.min))
                print(f"  {dataset:25s} {file_count:3d} file(s) | {dataset_size:8d} bytes | Latest: {latest_file.get('LastModified', 'N/A')}")
            else:
                print(f"  {dataset:25s} {file_count:3d} file(s)")
        
        print(f"\nTotal: {total_files} files | {total_size:,} bytes")
        
    except ClientError as e:
        print(f"Error: {e.response['Error']['Code']}")


def generate_summary(stats: dict, datasets: List[dict], flows: List[dict]) -> None:
    """Generate summary report."""
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    
    print(f"\nData Lake:")
    print(f"  Datasets: {len(datasets)}")
    
    print(f"\nData Integration:")
    print(f"  Flows: {stats['total_flows']}")
    print(f"  Total executions: {stats['total_executions']}")
    print(f"  Success rate: {stats['succeeded']}/{stats['total_executions']} ({100*stats['succeeded']/stats['total_executions']:.1f}%)" if stats['total_executions'] > 0 else "  Success rate: N/A (no executions)")
    
    print(f"\nStatus:")
    if stats['flows_without_executions'] > 0:
        print(f"  {stats['flows_without_executions']} flow(s) have not executed yet")
    if stats['failed'] > 0:
        print(f"  {stats['failed']} execution(s) failed")
    if stats['running'] > 0:
        print(f"  {stats['running']} execution(s) currently running")
    if stats['succeeded'] == stats['total_executions'] and stats['total_executions'] > 0:
        print(f"  All executions succeeded")


def main():
    print("="*70)
    print("AWS SUPPLY CHAIN DIAGNOSTICS")
    print("="*70)
    print()
    
    config = load_config()
    instance_id = config['instance_id']
    region = config['aws_region']
    
    # Instance details
    instance = get_instance_details(instance_id, region)
    
    if not instance:
        print("\nCannot proceed without instance information")
        return 1
    
    # Namespaces
    namespaces = list_data_lake_namespaces(instance_id, region)
    
    # Datasets (always check 'asc' namespace - it's the default for CDM datasets)
    datasets = list_datasets_in_namespace(instance_id, 'asc', region)
    
    # Flows and executions
    flows, flow_executions = get_flow_execution_details(instance_id, region)
    
    # Execution statistics
    stats = get_execution_statistics(flow_executions)
    
    # Failed executions
    if stats['failed'] > 0:
        list_failed_executions(instance_id, flow_executions, region)
    
    # S3 bucket status
    check_s3_bucket_status(instance_id, region)
    
    # Data integration events
    list_data_integration_events(instance_id, region)
    
    # Summary
    generate_summary(stats, datasets, flows)
    
    print("\n" + "="*70)
    print("END OF REPORT")
    print("="*70)
    
    return 0


if __name__ == '__main__':
    exit(main())
