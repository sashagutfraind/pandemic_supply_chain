#!/usr/bin/env python3
"""
Compute antiviral access metrics for each ZIP code and site.

For each ZIP code:
- Find nearest site by Haversine distance
- Compute population served by each site
- Calculate doses per capita
- Calculate population-weighted distance

Outputs:
- coverage-output/access_by_zip.csv: Access metrics per ZIP code
- coverage-output/access_by_site.csv: Aggregated metrics per site
- coverage-output/visualization_data.csv: Data for plotting
"""

import pandas as pd
import numpy as np
from pathlib import Path

# Optional visualization imports
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

OUTPUT_DIR = Path(__file__).parent / "coverage-output"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate Haversine distance between two points in kilometers."""
    R = 6371  # Earth radius in km
    
    lat1_rad = np.radians(lat1)
    lat2_rad = np.radians(lat2)
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    
    a = np.sin(dlat/2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    
    return R * c


def find_nearest_site(zip_lat: float, zip_lon: float, sites_df: pd.DataFrame) -> tuple:
    """Find nearest site to a ZIP code."""
    distances = sites_df.apply(
        lambda row: haversine_distance(zip_lat, zip_lon, row['latitude'], row['longitude']),
        axis=1
    )
    
    nearest_idx      = distances.idxmin()
    nearest_site     = sites_df.loc[nearest_idx]
    nearest_distance = distances[nearest_idx]
    
    return nearest_site['site_id'], nearest_site['zip_code'], nearest_distance


def compute_access_metrics() -> tuple:
    """Compute access metrics for all ZIP codes."""
    print("Computing access metrics...")
    
    # Load data
    zip_pop_path = OUTPUT_DIR / 'cov_2_zip_population.csv'
    stocks_path = OUTPUT_DIR / 'cov_3_current_stocks.csv'
    
    if not zip_pop_path.exists():
        print(f"  ✗ Missing: {zip_pop_path}")
        print("  Run: python coverage_1_gazeteer_zips.py && python coverage_2_download_acs.py")
        return None, None
    
    if not stocks_path.exists():
        print(f"  ✗ Missing: {stocks_path}")
        print("  Run: python coverage_3_current_stocks.py")
        return None, None
    
    zip_pop = pd.read_csv(zip_pop_path, dtype={'zip_code': str})
    stocks = pd.read_csv(stocks_path, dtype={'zip_code': str, 'site_id': str})
    
    print(f"  Loaded {len(zip_pop)} ZIP codes")
    print(f"  Loaded {len(stocks)} sites with inventory")
    
    # For each ZIP code, find nearest site
    access_records = []
    
    for _, zip_row in zip_pop.iterrows():
        zip_code = zip_row['zip_code']
        zip_lat = zip_row['latitude']
        zip_lon = zip_row['longitude']
        population = zip_row['total_pop']
        
        # Find nearest site
        nearest_site_id, nearest_site_zip, distance_km = find_nearest_site(
            zip_lat, zip_lon, stocks
        )
        
        access_records.append({
            'zip_code': zip_code,
            'latitude': zip_lat,
            'longitude': zip_lon,
            'population': population,
            'nearest_site_id': nearest_site_id,
            'nearest_site_zip': nearest_site_zip,
            'distance_km': distance_km,
            'distance_miles': distance_km * 0.621371
        })
    
    access_df = pd.DataFrame(access_records)
    
    # Merge with stock levels
    access_df = access_df.merge(
        stocks[['site_id', 'total_doses']],
        left_on='nearest_site_id',
        right_on='site_id',
        how='left'
    )
    
    # Compute per-site aggregations
    site_metrics = access_df.groupby('nearest_site_id').agg({
        'population': 'sum',
        'distance_km': lambda x: np.average(x, weights=access_df.loc[x.index, 'population']),
        'zip_code': 'count',
        'total_doses': 'first'
    }).reset_index()
    
    site_metrics.rename(columns={
        'population': 'total_population_served',
        'distance_km': 'pop_weighted_distance_km',
        'zip_code': 'zip_codes_served'
    }, inplace=True)
    
    # Calculate doses per capita
    site_metrics['doses_per_1000_people'] = (
        site_metrics['total_doses'] / site_metrics['total_population_served'] * 1000
    )
    
    # Merge site details
    site_metrics = site_metrics.merge(
        stocks[['site_id', 'zip_code', 'latitude', 'longitude', 'city', 'state_prov']],
        left_on='nearest_site_id',
        right_on='site_id',
        how='left'
    )
    
    return access_df, site_metrics


def create_visualizations(site_metrics: pd.DataFrame) -> None:
    """Create visualizations of coverage metrics."""
    if not HAS_MATPLOTLIB:
        print("\n⚠️  Skipping visualizations (matplotlib not installed)")
        return
    
    print("\nCreating visualizations...")
    
    # Set style
    sns.set_style("whitegrid")
    
    # 1. Best stocked sites (by doses per 1000 people)
    fig, axes = plt.subplots(2, 1, figsize=(12, 10))
    
    # Top 10 best stocked
    top_sites = site_metrics.nlargest(10, 'doses_per_1000_people')
    axes[0].barh(top_sites['zip_code'], top_sites['doses_per_1000_people'], color='green')
    axes[0].set_xlabel('Doses per 1,000 People')
    axes[0].set_ylabel('Site ZIP Code')
    axes[0].set_title('Top 10 Best Stocked Sites')
    axes[0].invert_yaxis()
    
    # Bottom 10 most understocked
    bottom_sites = site_metrics.nsmallest(10, 'doses_per_1000_people')
    axes[1].barh(bottom_sites['zip_code'], bottom_sites['doses_per_1000_people'], color='red')
    axes[1].set_xlabel('Doses per 1,000 People')
    axes[1].set_ylabel('Site ZIP Code')
    axes[1].set_title('Top 10 Most Understocked Sites')
    axes[1].invert_yaxis()
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'cov_4_stock_levels.png', dpi=150, bbox_inches='tight')
    print(f"  ✓ Saved: cov_4_stock_levels.png")
    plt.close()
    
    # 2. Population served vs doses
    fig, ax = plt.subplots(figsize=(10, 6))
    scatter = ax.scatter(
        site_metrics['total_population_served'],
        site_metrics['total_doses'],
        s=100,
        c=site_metrics['doses_per_1000_people'],
        cmap='RdYlGn',
        alpha=0.6
    )
    ax.set_xlabel('Population Served')
    ax.set_ylabel('Total Doses Available')
    ax.set_title('Site Inventory vs Population Served')
    plt.colorbar(scatter, label='Doses per 1,000 People')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'cov_4_population_vs_doses.png', dpi=150, bbox_inches='tight')
    print(f"  ✓ Saved: cov_4_population_vs_doses.png")
    plt.close()


def main():
    print("="*70)
    print("Coverage Analysis - Step 4: Compute Access Metrics")
    print("="*70)
    print()
    
    # Compute metrics
    access_df, site_metrics = compute_access_metrics()
    
    if access_df is None or site_metrics is None:
        return 1
    
    # Save results
    access_file = OUTPUT_DIR / 'cov_4_access_by_zip.csv'
    site_file = OUTPUT_DIR / 'cov_4_access_by_site.csv'
    
    access_df.to_csv(access_file, index=False)
    site_metrics.to_csv(site_file, index=False)
    
    print(f"\n✓ Saved access by ZIP: {access_file}")
    print(f"✓ Saved access by site: {site_file}")
    
    # Display summary statistics
    print(f"\n{'='*70}")
    print("SUMMARY STATISTICS")
    print("="*70)
    
    print(f"\nZIP Code Coverage:")
    print(f"  Total ZIP codes: {len(access_df)}")
    print(f"  Total population: {access_df['population'].sum():,.0f}")
    print(f"  Average distance to nearest site: {access_df['distance_miles'].mean():.1f} miles")
    print(f"  Max distance to nearest site: {access_df['distance_miles'].max():.1f} miles")
    
    print(f"\nSite Performance:")
    print(f"  Total sites: {len(site_metrics)}")
    print(f"  Total doses available: {site_metrics['total_doses'].sum():,.0f}")
    print(f"  Average population per site: {site_metrics['total_population_served'].mean():,.0f}")
    print(f"  Average doses per 1,000 people: {site_metrics['doses_per_1000_people'].mean():.1f}")
    
    print(f"\nBest Stocked Sites (Top 5):")
    top5 = site_metrics.nlargest(5, 'doses_per_1000_people')
    for _, row in top5.iterrows():
        print(f"  {row['zip_code']} ({row['city']}): {row['doses_per_1000_people']:.1f} doses/1000 people")
    
    print(f"\nMost Understocked Sites (Bottom 5):")
    bottom5 = site_metrics.nsmallest(5, 'doses_per_1000_people')
    for _, row in bottom5.iterrows():
        print(f"  {row['zip_code']} ({row['city']}): {row['doses_per_1000_people']:.1f} doses/1000 people")
    
    # Create visualizations
    create_visualizations(site_metrics)
    
    print("\n" + "="*70)
    print("Complete")
    print("="*70)
    
    return 0


if __name__ == '__main__':
    exit(main())
