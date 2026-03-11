#!/usr/bin/env python3
"""
AWS Supply Chain Data Generator

Generates synthetic supply chain data for AWS Supply Chain CDM.

This module contains all data generation functions for the 15 CDM datasets.
Import these functions in other scripts to generate data.

Usage as module:
    from asc_generate import DATA_CONFIG, generate_all_data
    datasets = generate_all_data(DATA_CONFIG)

Usage as script:
    python asc_generate.py
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path


# =============================================================================
# Configuration
# =============================================================================

OUTPUT_DIR = Path("output-data")
OUTPUT_DIR.mkdir(exist_ok=True)

# Data generation configuration - Extended for Multi-Tier Distribution Network
DATA_CONFIG = {
    'state_name': 'Washington',
    'state_prefixes': ('98', '99'),
    'cdm_mode': True,  # Set to True to generate CDM-compliant column names
    
    # 3-Tier Distribution Network
    # Tier 1: Regional Hub (Seattle)
    'hub_sites': {
        '98101': {'city': 'Seattle', 'type': 'REGIONAL_HUB', 'lat': 47.6062, 'lon': -122.3321, 
                  'pop': 28000, 'income': 95000, 'poverty': 12.5}
    },
    
    # Tier 2: Distribution Centers
    'dc_sites': {
        '99201': {'city': 'Spokane', 'type': 'DISTRIBUTION_CENTER', 'lat': 47.6588, 'lon': -117.4260,
                  'pop': 21000, 'income': 52000, 'poverty': 18.1},
        '98402': {'city': 'Tacoma', 'type': 'DISTRIBUTION_CENTER', 'lat': 47.2529, 'lon': -122.4443,
                  'pop': 15000, 'income': 65000, 'poverty': 14.3}
    },
    
    # Tier 3: Local Distribution Points
    'local_sites': {
        '98004': {'city': 'Bellevue', 'type': 'LOCAL_DISTRIBUTION', 'lat': 47.6101, 'lon': -122.2015,
                  'pop': 35000, 'income': 140000, 'poverty': 5.2},
        '98501': {'city': 'Olympia', 'type': 'LOCAL_DISTRIBUTION', 'lat': 47.0379, 'lon': -122.9007,
                  'pop': 25000, 'income': 78000, 'poverty': 11.0},
        '98661': {'city': 'Vancouver', 'type': 'LOCAL_DISTRIBUTION', 'lat': 45.6387, 'lon': -122.6615,
                  'pop': 18000, 'income': 68000, 'poverty': 13.5},
        '98801': {'city': 'Wenatchee', 'type': 'LOCAL_DISTRIBUTION', 'lat': 47.4235, 'lon': -120.3103,
                  'pop': 12000, 'income': 55000, 'poverty': 16.8}
    },
    
    # Vendor/Supplier information
    'vendors': [
        {'id': 'SUPPLIER-PHARMA-01', 'name': 'Generic Pharma Inc', 'city': 'Boston', 'state': 'MA',
         'lead_time_days': 14, 'reliability': 0.95, 'cost_per_unit': 18.50},
        {'id': 'SUPPLIER-PHARMA-02', 'name': 'MedSupply Corp', 'city': 'Chicago', 'state': 'IL',
         'lead_time_days': 10, 'reliability': 0.90, 'cost_per_unit': 20.00},
        {'id': 'SUPPLIER-PHARMA-03', 'name': 'PharmaQuick LLC', 'city': 'San Francisco', 'state': 'CA',
         'lead_time_days': 7, 'reliability': 0.85, 'cost_per_unit': 22.50}
    ],
    
    # External warehouses
    'external_warehouses': [
        {'id': 'WAREHOUSE-EAST', 'name': 'East Coast Distribution Center', 'city': 'Newark', 'state': 'NJ'},
        {'id': 'WAREHOUSE-WEST', 'name': 'West Coast Distribution Center', 'city': 'Oakland', 'state': 'CA'}
    ],
    
    # Demand customers (hospitals, clinics)
    'customers': [
        {'id': 'HOSPITAL-UW', 'name': 'UW Medical Center', 'city': 'Seattle', 'type': 'HOSPITAL'},
        {'id': 'HOSPITAL-HARBORVIEW', 'name': 'Harborview Medical Center', 'city': 'Seattle', 'type': 'HOSPITAL'},
        {'id': 'CLINIC-BELLEVUE', 'name': 'Bellevue Family Clinic', 'city': 'Bellevue', 'type': 'CLINIC'},
        {'id': 'HOSPITAL-SPOKANE', 'name': 'Providence Sacred Heart', 'city': 'Spokane', 'type': 'HOSPITAL'},
        {'id': 'CLINIC-TACOMA', 'name': 'Tacoma Community Health', 'city': 'Tacoma', 'type': 'CLINIC'}
    ]
}


# =============================================================================
# Data Generation Functions
# =============================================================================

def generate_product_data(config: dict) -> pd.DataFrame:
    """Generate product data (CDM: product table)."""
    print("  Generating product data...")
    
    product_df = pd.DataFrame({
        'id': ['ANTIVIR-WA-01'],
        'connection_id': ['WA-PHARMA-DIST'],  # Required in CDM
        'description': [f'Oseltamivir 75mg - {config["state_name"]} Emergency Stock'],
        'base_uom': ['EA'],  # CDM field name (was unit_of_measure)
        'product_type': ['PHARMACEUTICAL']
    })
    
    return product_df


def generate_site_data(config: dict) -> pd.DataFrame:
    """Generate site data (CDM: site table) - 3-tier distribution network."""
    print("  Generating site data...")

    # Map cities to geography IDs
    city_to_geo = {
        'Seattle': 'USA-WA-SEATTLE',
        'Bellevue': 'USA-WA-SEATTLE',
        'Spokane': 'USA-WA-SPOKANE',
        'Tacoma': 'USA-WA-SEATTLE',
        'Olympia': 'USA-WA-SEATTLE',
        'Vancouver': 'USA-WA-VANCOUVER',
        'Wenatchee': 'USA-WA-WENATCHEE'
    }

    # Combine all site tiers
    all_sites = {**config['hub_sites'], **config['dc_sites'], **config['local_sites']}
    
    sites = []
    for zip_code, site_info in all_sites.items():
        sites.append({
            'id': zip_code,
            'connection_id': 'WA-PHARMA-DIST',  # Required in CDM
            'description': f"{site_info['city']} {site_info['type'].replace('_', ' ').title()} - ZIP {zip_code}",
            'site_type': site_info['type'],
            'geo_id': city_to_geo.get(site_info['city'], 'USA-WA'),
            'latitude': site_info['lat'],
            'longitude': site_info['lon'],
            'city': site_info['city'],
            'state_prov': 'WA',  # CDM field name
            'postal_code': zip_code,
            'country': 'US'
            # Note: Custom demographic fields removed for CDM compliance
        })
    
    site_df = pd.DataFrame(sites)
    return site_df


def generate_inventory_data(config: dict) -> pd.DataFrame:
    """Generate inventory level data (CDM: inv_level table) - realistic distribution."""
    print("  Generating inventory data...")
    
    np.random.seed(42)
    
    # Get all sites
    all_sites = {**config['hub_sites'], **config['dc_sites'], **config['local_sites']}
    
    inv_records = []
    for zip_code, site_info in all_sites.items():
        # Inventory levels based on site tier and population
        if site_info['type'] == 'REGIONAL_HUB':
            base_inventory = int(site_info['pop'] * 0.10)  # 10% of population (high stock)
        elif site_info['type'] == 'DISTRIBUTION_CENTER':
            base_inventory = int(site_info['pop'] * 0.05)  # 5% of population
        else:  # LOCAL_DISTRIBUTION
            base_inventory = int(site_info['pop'] * 0.02)  # 2% of population (low stock)
        
        inv_records.append({
            'snapshot_date': datetime.now().strftime('%Y-%m-%d'),
            'site_id': zip_code,
            'product_id': 'ANTIVIR-WA-01',
            'connection_id': 'WA-PHARMA-DIST',  # Required in CDM
            'on_hand_inventory': base_inventory,
            'inv_condition': 'HEALTHY',
            'lot_number': f'LOT-2026-{np.random.randint(100, 999)}',
            'expiry_date': (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')  # CDM field name
        })
    
    inv_level_df = pd.DataFrame(inv_records)
    return inv_level_df


def generate_inbound_order_data(config: dict) -> pd.DataFrame:
    """Generate inbound order data (CDM: inbound_order table) - from vendors and transshipments."""
    print("  Generating inbound order data...")
    
    orders = []
    
    # Orders from vendors to regional hub
    for i, vendor in enumerate(config['vendors'], 1):
        orders.append({
            'id': f'WA-VENDOR-PO-{i:03d}',
            'connection_id': 'WA-PHARMA-DIST',  # Required in CDM
            'tpartner_id': vendor['id'],
            'order_type': 'PURCHASE_ORDER',
            'order_status': 'IN_TRANSIT' if i == 1 else 'PENDING'
        })
    
    # Transshipment orders (hub to DCs)
    for i, dc_zip in enumerate(config['dc_sites'].keys(), 1):
        orders.append({
            'id': f'WA-TRANSFER-TO-{i:03d}',
            'connection_id': 'WA-PHARMA-DIST',
            'tpartner_id': 'INTERNAL-TRANSFER',
            'order_type': 'STOCK_TRANSFER',
            'order_status': 'APPROVED'
        })
    
    # Transshipment orders (DCs to local sites)
    dc_zips = list(config['dc_sites'].keys())
    local_zips = list(config['local_sites'].keys())
    for i, local_zip in enumerate(local_zips, 1):
        orders.append({
            'id': f'WA-LOCAL-TO-{i:03d}',
            'connection_id': 'WA-PHARMA-DIST',
            'tpartner_id': 'INTERNAL-TRANSFER',
            'order_type': 'STOCK_TRANSFER',
            'order_status': 'PENDING'
        })
    
    inbound_order_df = pd.DataFrame(orders)
    return inbound_order_df


def generate_trading_partner_data(config: dict) -> pd.DataFrame:
    """Generate trading partner data (CDM: trading_partner table) - vendors, warehouses, internal."""
    print("  Generating trading partner data...")
    
    cdm_mode = config.get('cdm_mode', False)
    partners = []
    
    # Add vendors
    for vendor in config['vendors']:
        entry = {
            'id': vendor['id'],
            'description': vendor['name'],  # CDM field name (was 'name')
            'tpartner_type': 'SUPPLIER',  # CDM field name (was 'partner_type')
            'city': vendor['city'],
            'state_prov': vendor['state'],  # CDM field name (was 'state')
            'country': 'US'
        }
        
        # Add required CDM fields
        if cdm_mode:
            entry.update({
                'geo_id': 'GEO-US',  # Required in CDM
                'eff_start_date': '1900-01-01T00:00:00Z',  # Required in CDM
                'eff_end_date': '9999-12-31T23:59:59Z'  # Required in CDM
            })
        
        partners.append(entry)
    
    # Add external warehouses
    for warehouse in config['external_warehouses']:
        entry = {
            'id': warehouse['id'],
            'description': warehouse['name'],
            'tpartner_type': 'WAREHOUSE',
            'city': warehouse['city'],
            'state_prov': warehouse['state'],
            'country': 'US'
        }
        
        if cdm_mode:
            entry.update({
                'geo_id': 'GEO-US',
                'eff_start_date': '1900-01-01T00:00:00Z',
                'eff_end_date': '9999-12-31T23:59:59Z'
            })
        
        partners.append(entry)
    
    # Add internal transfer partner
    entry = {
        'id': 'INTERNAL-TRANSFER',
        'description': 'Internal Stock Transfer',
        'tpartner_type': 'INTERNAL',
        'city': 'Seattle',
        'state_prov': 'WA',
        'country': 'US'
    }
    
    if cdm_mode:
        entry.update({
            'geo_id': 'GEO-US-WA',
            'eff_start_date': '1900-01-01T00:00:00Z',
            'eff_end_date': '9999-12-31T23:59:59Z'
        })
    
    partners.append(entry)
    
    # Add customer partners
    for customer in config['customers']:
        entry = {
            'id': customer['id'],
            'description': customer['name'],
            'tpartner_type': 'CUSTOMER',
            'city': customer['city'],
            'state_prov': 'WA',
            'country': 'US'
        }
        
        if cdm_mode:
            entry.update({
                'geo_id': 'GEO-US-WA',
                'eff_start_date': '1900-01-01T00:00:00Z',
                'eff_end_date': '9999-12-31T23:59:59Z'
            })
        
        partners.append(entry)
    
    tpartner_df = pd.DataFrame(partners)
    return tpartner_df
def generate_inbound_order_line_data(config: dict) -> pd.DataFrame:
    """Generate inbound order line data (CDM: inbound_order_line table)."""
    print("  Generating inbound order line data...")

    lines = []
    
    # Lines for vendor orders
    hub_zip = list(config['hub_sites'].keys())[0]
    for i, vendor in enumerate(config['vendors'], 1):
        lines.append({
            'id': f'WA-VENDOR-PO-{i:03d}-LINE-001',
            'order_id': f'WA-VENDOR-PO-{i:03d}',
            'connection_id': 'WA-PHARMA-DIST',  # Required in CDM
            'tpartner_id': vendor['id'],
            'product_id': 'ANTIVIR-WA-01',
            'quantity_submitted': 5000 + (i * 1000),  # Required in CDM
            'to_site_id': hub_zip,
            'quantity_uom': 'EA',
            'status': 'OPEN',
            'expected_delivery_date': (datetime.now() + timedelta(days=vendor['lead_time_days'])).strftime('%Y-%m-%d')
        })
    
    # Lines for transshipment to DCs
    for i, dc_zip in enumerate(config['dc_sites'].keys(), 1):
        lines.append({
            'id': f'WA-TRANSFER-TO-{i:03d}-LINE-001',
            'order_id': f'WA-TRANSFER-TO-{i:03d}',
            'connection_id': 'WA-PHARMA-DIST',
            'tpartner_id': 'INTERNAL-TRANSFER',
            'product_id': 'ANTIVIR-WA-01',
            'quantity_submitted': 2000,
            'to_site_id': dc_zip,
            'quantity_uom': 'EA',
            'status': 'OPEN',
            'expected_delivery_date': (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d')
        })
    
    # Lines for transshipment to local sites
    for i, local_zip in enumerate(config['local_sites'].keys(), 1):
        lines.append({
            'id': f'WA-LOCAL-TO-{i:03d}-LINE-001',
            'order_id': f'WA-LOCAL-TO-{i:03d}',
            'connection_id': 'WA-PHARMA-DIST',
            'tpartner_id': 'INTERNAL-TRANSFER',
            'product_id': 'ANTIVIR-WA-01',
            'quantity_submitted': 500,
            'to_site_id': local_zip,
            'quantity_uom': 'EA',
            'status': 'OPEN',
            'expected_delivery_date': (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        })

    inbound_order_line_df = pd.DataFrame(lines)
    return inbound_order_line_df


def generate_inv_policy_data(config: dict) -> pd.DataFrame:
    """Generate inventory policy data (CDM: inv_policy table) - tier-based policies."""
    print("  Generating inventory policy data...")

    # Get all sites
    all_sites = {**config['hub_sites'], **config['dc_sites'], **config['local_sites']}
    
    policies = []
    for zip_code, site_info in all_sites.items():
        # Policy parameters based on site tier
        if site_info['type'] == 'REGIONAL_HUB':
            min_safety = 2000
            max_safety = 10000
            target = 5000
            min_order = 1000
            max_order = 10000
        elif site_info['type'] == 'DISTRIBUTION_CENTER':
            min_safety = 500
            max_safety = 3000
            target = 1500
            min_order = 500
            max_order = 5000
        else:  # LOCAL_DISTRIBUTION
            min_safety = 100
            max_safety = 1000
            target = 500
            min_order = 100
            max_order = 2000
        
        policies.append({
            'id': f'POLICY-{zip_code}-ANTIVIR',
            'site_id': zip_code,
            'product_id': 'ANTIVIR-WA-01',
            'connection_id': 'WA-PHARMA-DIST',  # Required in CDM
            'product_group_id': 'SCN_RESERVED_NO_VALUE_PROVIDED',  # Required in CDM
            'dest_geo_id': 'SCN_RESERVED_NO_VALUE_PROVIDED',  # Required in CDM
            'vendor_tpartner_id': 'SCN_RESERVED_NO_VALUE_PROVIDED',  # Required in CDM
            'ss_policy': 'abs_level',
            'min_safety_stock': min_safety,
            'max_safety_stock': max_safety,
            'target_inventory_qty': target,
            'min_order_qty': min_order,
            'max_order_qty': max_order,
            'eff_start_date': '1900-01-01',
            'eff_end_date': '9999-12-31'
        })

    inv_policy_df = pd.DataFrame(policies)
    return inv_policy_df
def generate_geography_data(config: dict) -> pd.DataFrame:
    """Generate geography data (CDM: geography table) - extended metro areas."""
    print("  Generating geography data...")

    # Create geography hierarchy: USA → Washington → Metro areas
    geography_df = pd.DataFrame([
        {
            'id': 'USA',
            'connection_id': 'WA-PHARMA-DIST',  # Required in CDM
            'description': 'United States',
            'parent_geo_id': None,
            'country': 'US'
        },
        {
            'id': 'USA-WA',
            'connection_id': 'WA-PHARMA-DIST',
            'description': 'Washington State',
            'parent_geo_id': 'USA',
            'state_prov': 'WA',
            'country': 'US'
        },
        {
            'id': 'USA-WA-SEATTLE',
            'connection_id': 'WA-PHARMA-DIST',
            'description': 'Seattle Metro Area',
            'parent_geo_id': 'USA-WA',
            'city': 'Seattle',
            'state_prov': 'WA',
            'country': 'US'
        },
        {
            'id': 'USA-WA-SPOKANE',
            'connection_id': 'WA-PHARMA-DIST',
            'description': 'Spokane Metro Area',
            'parent_geo_id': 'USA-WA',
            'city': 'Spokane',
            'state_prov': 'WA',
            'country': 'US'
        },
        {
            'id': 'USA-WA-VANCOUVER',
            'connection_id': 'WA-PHARMA-DIST',
            'description': 'Vancouver Metro Area',
            'parent_geo_id': 'USA-WA',
            'city': 'Vancouver',
            'state_prov': 'WA',
            'country': 'US'
        },
        {
            'id': 'USA-WA-WENATCHEE',
            'connection_id': 'WA-PHARMA-DIST',
            'description': 'Wenatchee Metro Area',
            'parent_geo_id': 'USA-WA',
            'city': 'Wenatchee',
            'state_prov': 'WA',
            'country': 'US'
        }
    ])

    return geography_df


def generate_product_hierarchy_data(config: dict) -> pd.DataFrame:
    """Generate product hierarchy data (CDM: product_hierarchy table)."""
    print("  Generating product hierarchy data...")

    # Create a simple product hierarchy: Pharmaceuticals -> Antivirals
    product_hierarchy_df = pd.DataFrame([
        {
            'id': 'PHARMA',
            'connection_id': 'WA-PHARMA-DIST',  # Required in CDM
            'description': 'Pharmaceutical Products',
            'parent_product_group_id': None
        },
        {
            'id': 'PHARMA-ANTIVIRAL',
            'connection_id': 'WA-PHARMA-DIST',
            'description': 'Antiviral Medications',
            'parent_product_group_id': 'PHARMA'
        }
    ])

    return product_hierarchy_df


def generate_company_data(config: dict) -> pd.DataFrame:
    """Generate company data (CDM: company table)."""
    print("  Generating company data...")
    
    cdm_mode = config.get('cdm_mode', False)
    
    if cdm_mode:
        # CDM schema requires: id, connection_id
        entry = {
            'id': 'WA-PHARMA-DIST',
            'connection_id': 'WA-PHARMA-DIST',  # Required in CDM
            'description': 'Emergency antiviral distribution network for Washington State',
        }
    else:
        entry = {
            'id': 'WA-PHARMA-DIST',
            'name': 'Washington Pharmaceutical Distribution Network',
            'description': 'Emergency antiviral distribution network for Washington State',
            'address_line1': '1234 Healthcare Way',
            'city': 'Seattle',
            'state_prov': 'WA',
            'postal_code': '98101',
            'country': 'US'
        }
    
    company_df = pd.DataFrame([entry])
    return company_df


def generate_transportation_lane_data(config: dict) -> pd.DataFrame:
    """Generate transportation lane data (CDM: transportation_lane table)."""
    print("  Generating transportation lane data...")
    
    lanes = []
    
    # Hub to DCs
    hub_zip = list(config['hub_sites'].keys())[0]
    for dc_zip, dc_info in config['dc_sites'].items():
        lanes.append({
            'id': f'LANE-{hub_zip}-{dc_zip}',
            'from_site_id': hub_zip,
            'to_site_id': dc_zip,
            'connection_id': 'WA-PHARMA-DIST',  # Required in CDM
            'from_geo_id': 'USA-WA-SEATTLE',  # Required in CDM
            'to_geo_id': 'USA-WA-SPOKANE' if dc_zip == '99201' else 'USA-WA-SEATTLE',  # Required in CDM
            'carrier_tpartner_id': 'SCN_RESERVED_NO_VALUE_PROVIDED',  # Required in CDM
            'trans_mode': 'TRUCK',  # CDM field name (was transportation_mode)
            'service_type': 'STANDARD',  # Required in CDM
            'product_group_id': 'SCN_RESERVED_NO_VALUE_PROVIDED',  # Required in CDM
            'transit_time': 1.0,  # 1 day
            'time_uom': 'DAY',  # CDM field name (was transit_time_uom)
            'distance': 150.0 if dc_zip == '99201' else 35.0,  # Spokane farther
            'distance_uom': 'MI',
            'cost_per_unit': 0.50,
            'eff_start_date': '1900-01-01',
            'eff_end_date': '9999-12-31'
        })
    
    # DCs to local sites
    dc_zips = list(config['dc_sites'].keys())
    for local_zip, local_info in config['local_sites'].items():
        # Assign each local site to nearest DC
        source_dc = dc_zips[0] if local_info['city'] in ['Bellevue', 'Olympia'] else dc_zips[1]
        
        # Map local sites to geo IDs
        local_geo_map = {
            '98004': 'USA-WA-SEATTLE',  # Bellevue
            '98501': 'USA-WA-SEATTLE',  # Olympia
            '98661': 'USA-WA-VANCOUVER',  # Vancouver
            '98801': 'USA-WA-WENATCHEE'  # Wenatchee
        }
        
        lanes.append({
            'id': f'LANE-{source_dc}-{local_zip}',
            'from_site_id': source_dc,
            'to_site_id': local_zip,
            'connection_id': 'WA-PHARMA-DIST',
            'from_geo_id': 'USA-WA-SPOKANE' if source_dc == '99201' else 'USA-WA-SEATTLE',
            'to_geo_id': local_geo_map.get(local_zip, 'USA-WA'),
            'carrier_tpartner_id': 'SCN_RESERVED_NO_VALUE_PROVIDED',
            'trans_mode': 'TRUCK',
            'service_type': 'STANDARD',
            'product_group_id': 'SCN_RESERVED_NO_VALUE_PROVIDED',
            'transit_time': 0.5,  # Half day
            'time_uom': 'DAY',
            'distance': 25.0,
            'distance_uom': 'MI',
            'cost_per_unit': 0.25,
            'eff_start_date': '1900-01-01',
            'eff_end_date': '9999-12-31'
        })
    
    # Emergency transshipment lanes (DC to DC)
    if len(dc_zips) >= 2:
        lanes.append({
            'id': f'LANE-{dc_zips[0]}-{dc_zips[1]}',
            'from_site_id': dc_zips[0],
            'to_site_id': dc_zips[1],
            'connection_id': 'WA-PHARMA-DIST',
            'from_geo_id': 'USA-WA-SEATTLE',
            'to_geo_id': 'USA-WA-SPOKANE',
            'carrier_tpartner_id': 'SCN_RESERVED_NO_VALUE_PROVIDED',
            'trans_mode': 'TRUCK',
            'service_type': 'STANDARD',
            'product_group_id': 'SCN_RESERVED_NO_VALUE_PROVIDED',
            'transit_time': 1.5,
            'time_uom': 'DAY',
            'distance': 180.0,
            'distance_uom': 'MI',
            'cost_per_unit': 0.75,
            'eff_start_date': '1900-01-01',
            'eff_end_date': '9999-12-31'
        })
        
        lanes.append({
            'id': f'LANE-{dc_zips[1]}-{dc_zips[0]}',
            'from_site_id': dc_zips[1],
            'to_site_id': dc_zips[0],
            'connection_id': 'WA-PHARMA-DIST',
            'from_geo_id': 'USA-WA-SPOKANE',
            'to_geo_id': 'USA-WA-SEATTLE',
            'carrier_tpartner_id': 'SCN_RESERVED_NO_VALUE_PROVIDED',
            'trans_mode': 'TRUCK',
            'service_type': 'STANDARD',
            'product_group_id': 'SCN_RESERVED_NO_VALUE_PROVIDED',
            'transit_time': 1.5,
            'time_uom': 'DAY',
            'distance': 180.0,
            'distance_uom': 'MI',
            'cost_per_unit': 0.75,
            'eff_start_date': '1900-01-01',
            'eff_end_date': '9999-12-31'
        })
    
    transportation_lane_df = pd.DataFrame(lanes)
    return transportation_lane_df


def generate_shipment_data(config: dict) -> pd.DataFrame:
    """Generate shipment data (CDM: shipment table)."""
    print("  Generating shipment data...")
    
    shipments = []
    
    # Active shipment from vendor
    hub_zip = list(config['hub_sites'].keys())[0]
    shipments.append({
        'id': 'SHIP-VENDOR-001',
        'supplier_tpartner_id': 'SUPPLIER-PHARMA-01',  # CDM field name (was tpartner_id)
        'connection_id': 'WA-PHARMA-DIST',  # Required in CDM
        'product_id': 'ANTIVIR-WA-01',
        'order_id': 'WA-VENDOR-PO-001',  # Required in CDM
        'order_line_id': 'WA-VENDOR-PO-001-LINE-001',  # Required in CDM
        'package_id': 'PKG-001',  # Required in CDM
        'ship_from_site_id': 'WAREHOUSE-EAST',
        'ship_to_site_id': hub_zip,
        'planned_ship_date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
        'planned_delivery_date': (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d'),
        'shipment_status': 'IN_TRANSIT'
    })
    
    # Completed transshipment
    dc_zip = list(config['dc_sites'].keys())[0]
    shipments.append({
        'id': 'SHIP-TRANSFER-001',
        'supplier_tpartner_id': 'INTERNAL-TRANSFER',
        'connection_id': 'WA-PHARMA-DIST',
        'product_id': 'ANTIVIR-WA-01',
        'order_id': 'WA-TRANSFER-TO-001',
        'order_line_id': 'WA-TRANSFER-TO-001-LINE-001',
        'package_id': 'PKG-002',
        'ship_from_site_id': hub_zip,
        'ship_to_site_id': dc_zip,
        'planned_ship_date': (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d'),
        'planned_delivery_date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
        'actual_delivery_date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
        'shipment_status': 'DELIVERED'
    })
    
    shipment_df = pd.DataFrame(shipments)
    return shipment_df


def generate_outbound_order_line_data(config: dict) -> pd.DataFrame:
    """Generate outbound order line data (CDM: outbound_order_line table)."""
    print("  Generating outbound order line data...")
    
    orders = []
    
    # Orders from customers (hospitals, clinics)
    all_sites = {**config['hub_sites'], **config['dc_sites'], **config['local_sites']}
    
    for i, customer in enumerate(config['customers'], 1):
        # Find nearest site to customer
        customer_city = customer['city']
        fulfillment_site = None
        for zip_code, site_info in all_sites.items():
            if site_info['city'] == customer_city:
                fulfillment_site = zip_code
                break
        
        if not fulfillment_site:
            fulfillment_site = list(config['hub_sites'].keys())[0]
        
        # Normal demand vs. pandemic spike
        if customer['type'] == 'HOSPITAL':
            base_qty = 500
            spike_qty = 1500  # 300% spike
        else:  # CLINIC
            base_qty = 200
            spike_qty = 600
        
        # Normal order (before pandemic)
        orders.append({
            'id': f'OUT-{customer["id"]}-NORMAL-{i:03d}',
            'cust_order_id': f'CUST-ORDER-{i:03d}-NORMAL',  # Required in CDM
            'connection_id': 'WA-PHARMA-DIST',  # Required in CDM
            'product_id': 'ANTIVIR-WA-01',
            'customer_tpartner_id': customer['id'],  # CDM field name (was tpartner_id)
            'init_quantity_requested': base_qty,  # CDM field name (was quantity)
            'quantity_uom': 'EA',
            'order_date': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
            'requested_delivery_date': (datetime.now() - timedelta(days=28)).strftime('%Y-%m-%d'),
            'status': 'FULFILLED',  # CDM field name (was order_status)
            'ship_from_site_id': fulfillment_site
        })
        
        # Pandemic spike order
        orders.append({
            'id': f'OUT-{customer["id"]}-SPIKE-{i:03d}',
            'cust_order_id': f'CUST-ORDER-{i:03d}-SPIKE',
            'connection_id': 'WA-PHARMA-DIST',
            'product_id': 'ANTIVIR-WA-01',
            'customer_tpartner_id': customer['id'],
            'init_quantity_requested': spike_qty,
            'quantity_uom': 'EA',
            'order_date': datetime.now().strftime('%Y-%m-%d'),
            'requested_delivery_date': (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d'),
            'status': 'PENDING',
            'ship_from_site_id': fulfillment_site
        })
    
    outbound_order_line_df = pd.DataFrame(orders)
    return outbound_order_line_df


def generate_forecast_data(config: dict) -> pd.DataFrame:
    """Generate forecast data (CDM: forecast table)."""
    print("  Generating forecast data...")
    
    forecasts = []
    
    # Generate 30-day forecast for each site
    all_sites = {**config['hub_sites'], **config['dc_sites'], **config['local_sites']}
    
    for zip_code, site_info in all_sites.items():
        # Base daily demand
        if site_info['type'] == 'REGIONAL_HUB':
            base_demand = 100
        elif site_info['type'] == 'DISTRIBUTION_CENTER':
            base_demand = 50
        else:
            base_demand = 20
        
        # Generate forecast for next 30 days
        for day in range(30):
            forecast_start = datetime.now() + timedelta(days=day)
            forecast_end = forecast_start + timedelta(days=1)
            
            # Pandemic spike starts at day 7
            if day < 7:
                demand = base_demand
            elif day < 14:
                # Ramp up to 300% spike
                spike_factor = 1 + (2 * (day - 7) / 7)  # 1x to 3x
                demand = int(base_demand * spike_factor)
            else:
                # Sustained high demand
                demand = int(base_demand * 3)
            
            forecasts.append({
                'snapshot_date': datetime.now().strftime('%Y-%m-%d'),  # Required in CDM
                'connection_id': 'WA-PHARMA-DIST',  # Required in CDM
                'product_id': 'ANTIVIR-WA-01',
                'site_id': zip_code,
                'region_id': 'USA-WA',  # Required in CDM
                'product_group_id': 'SCN_RESERVED_NO_VALUE_PROVIDED',  # Required in CDM
                'forecast_start_dttm': forecast_start.strftime('%Y-%m-%dT00:00:00Z'),  # Required in CDM
                'forecast_end_dttm': forecast_end.strftime('%Y-%m-%dT00:00:00Z'),  # Required in CDM
                'mean': demand  # CDM field name (was forecast_quantity)
            })
    
    forecast_df = pd.DataFrame(forecasts)
    return forecast_df


def generate_calendar_data(config: dict) -> pd.DataFrame:
    """Generate calendar data (CDM: calendar table)."""
    print("  Generating calendar data...")
    
    cdm_mode = config.get('cdm_mode', False)
    calendar_entries = []
    
    # Generate calendar for next 90 days
    for day in range(90):
        cal_date = datetime.now() + timedelta(days=day)
        is_weekend = cal_date.weekday() >= 5
        
        # Mark holidays
        is_holiday = False
        holiday_name = None
        if cal_date.month == 12 and cal_date.day == 25:
            is_holiday = True
            holiday_name = 'Christmas'
        elif cal_date.month == 1 and cal_date.day == 1:
            is_holiday = True
            holiday_name = 'New Year'
        
        if cdm_mode:
            # CDM schema for calendar
            entry = {
                'calendar_id': f'CAL-{cal_date.strftime("%Y%m%d")}',
                'connection_id': 'WA-PHARMA-DIST',  # Required
                'date': cal_date.strftime('%Y-%m-%dT00:00:00Z'),  # TIMESTAMP
                'year': cal_date.year,
                'month': cal_date.month,
                'week': cal_date.isocalendar()[1],  # ISO week number
                'day': cal_date.day,
                'is_working': 'N' if (is_weekend or is_holiday) else 'Y',
                'is_holiday': 'Y' if is_holiday else 'N',
                'eff_start_date': '1900-01-01T00:00:00Z',
                'eff_end_date': '9999-12-31T23:59:59Z'
            }
        else:
            # Original format
            entry = {
                'id': f'CAL-{cal_date.strftime("%Y%m%d")}',
                'calendar_date': cal_date.strftime('%Y-%m-%d'),
                'day_of_week': cal_date.strftime('%A'),
                'is_weekend': is_weekend,
                'is_holiday': is_holiday,
                'holiday_name': holiday_name,
                'is_working_day': not (is_weekend or is_holiday),
                'fiscal_year': cal_date.year,
                'fiscal_quarter': (cal_date.month - 1) // 3 + 1,
                'fiscal_month': cal_date.month
            }
        
        calendar_entries.append(entry)
    
    calendar_df = pd.DataFrame(calendar_entries)
    return calendar_df




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


if __name__ == '__main__':
    """Run data generation when executed as script."""
    generate_all_data(DATA_CONFIG)
