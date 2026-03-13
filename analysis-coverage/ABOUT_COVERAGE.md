# Antiviral Coverage Analysis

## Problem Statement

During a pandemic, rapid access to antiviral medications is critical for public health outcomes. This analysis evaluates the geographic accessibility of antiviral stockpiles across Washington State ZIP codes to identify:

1. **Coverage gaps**: ZIP codes with poor access to nearby stockpiles
2. **Understocked sites**: Distribution centers serving large populations with insufficient inventory
3. **Optimization opportunities**: Potential transshipment routes to improve equity

## Methodology

### Step 1: Geographic Data Collection (`coverage_1_gazeteer_zips.py`)

Retrieves ZIP code centroids (latitude/longitude) from the US Census Gazetteer:
- Source: 2024 Census ZCTA (ZIP Code Tabulation Area) Gazetteer
- Data: Geographic coordinates for all Washington State ZIP codes (98xxx, 99xxx)
- Output: `coverage-output/cov_1_zip_centroids.csv`

### Step 2: Population Data Collection (`coverage_2_download_acs.py`)

Fetches demographic data from the American Community Survey (ACS):
- Source: 2022 ACS 5-Year Data Profiles API
- Metrics: Total population, median income, poverty rate, labor force
- Coverage: ALL Washington State ZIP codes (no sampling)
- Outputs:
  - `coverage-output/cov_2_zip_population.csv`: Population data for all ZIP codes
  - `coverage-output/cov_2_census_raw.csv`: Raw census API response (cached)
- Note: ZIP codes are not a perfect aggregation level for reasons of shape, real travel distance etc

### Step 3: Current Stock Levels (`coverage_3_current_stocks.py`)

Queries AWS Supply Chain inventory data from the Data Lake:
- Source: AWS Supply Chain Data Lake (S3 bucket)
- Datasets: `inv_level` (inventory) and `site` (locations)
- Method: Reads directly from S3 where Data Lake stores ingested data
- Output: `coverage-output/cov_3_current_stocks.csv`

### Step 4: Access Metrics Computation (`coverage_4_compute_access.py`)

Calculates accessibility metrics using Haversine distance formula:

**For each ZIP code:**
- Identifies nearest distribution site
- Calculates straight-line distance (km and miles)
- Records population and available doses at nearest site

**For each site:**
- Total population served (sum of all ZIP codes using this site)
- Number of ZIP codes served
- Total doses available
- Doses per 1,000 people (coverage ratio)
- Population-weighted average distance

**Outputs:**
- `coverage-output/cov_4_access_by_zip.csv`: Per-ZIP code metrics
- `coverage-output/cov_4_access_by_site.csv`: Per-site aggregated metrics
- `coverage-output/cov_4_stock_levels.png`: Bar charts of best/worst stocked sites (optional, requires matplotlib)
- `coverage-output/cov_4_population_vs_doses.png`: Scatter plot of population vs inventory (optional, requires matplotlib)

## Key Metrics

### Doses per 1,000 People
Primary equity metric indicating coverage adequacy:
- **High values (>100)**: Well-stocked relative to population
- **Low values (<50)**: Understocked, potential shortage risk
- **Target**: Varies by disease severity and treatment protocols

### Population-Weighted Distance
Average distance weighted by population size:
- Accounts for both distance and population density
- Lower values indicate better geographic accessibility
- Measured in kilometers and miles

### Distance to Nearest Site
Direct Haversine distance (great-circle distance):
- Assumes straight-line travel (not road distance)
- Useful for identifying remote/underserved areas
- Does not account for transportation infrastructure

## Limitations

1. **Distance Calculation**: Uses Haversine (straight-line) distance, not actual road distance or travel time
2. **Static Analysis**: Snapshot of current inventory, does not model dynamic demand or replenishment
3. **Simplified Allocation**: Assumes each ZIP code uses only its nearest site (no overflow or preference modeling)
4. **No Capacity Constraints**: Does not account for site storage limits or throughput capacity

## Use Cases

### Identify Transshipment Opportunities
Sites with high doses-per-capita serving small populations can transfer inventory to understocked sites serving larger populations.

### Equity Analysis
Compare coverage across demographic groups (income, poverty rate) to identify disparities in access.

### Emergency Response Planning
Prioritize sites for emergency restocking based on population served and current stock levels.

### Network Optimization
Identify optimal locations for new distribution sites to minimize population-weighted distance.

## Running the Analysis

```bash
# Step 1: Get ZIP code coordinates
cd analysis-coverage
python coverage_1_gazeteer_zips.py

# Step 2: Get population data
python coverage_2_download_acs.py

# Step 3: Get current stocks from AWS Supply Chain
python coverage_3_current_stocks.py

# Step 4: Compute access metrics and visualizations
python coverage_4_compute_access.py
```

## Output Files

All outputs saved to `analysis-coverage/coverage-output/`:

**Step 1 outputs:**
- `cov_1_zip_centroids.csv`: ZIP code coordinates (850 ZIP codes)

**Step 2 outputs:**
- `cov_2_zip_population.csv`: Population and demographics by ZIP (850 ZIP codes, no sampling)
- `cov_2_census_raw.csv`: Raw census API response (cached for faster re-runs)

**Step 3 outputs:**
- `cov_3_current_stocks.csv`: Inventory levels by site from AWS Supply Chain Data Lake

**Step 4 outputs:**
- `cov_4_access_by_zip.csv`: Access metrics per ZIP code
- `cov_4_access_by_site.csv`: Aggregated metrics per site
- `cov_4_stock_levels.png`: Visualization of best/worst stocked sites (optional, requires matplotlib)
- `cov_4_population_vs_doses.png`: Population vs inventory scatter plot (optional, requires matplotlib)

## Integration with AWS Supply Chain

This analysis uses data from AWS Supply Chain's Data Lake:
- **Sites**: Distribution center locations (ZIP codes with lat/long)
- **Inventory**: Current on-hand stock levels (`inv_level` dataset)
- **Products**: Antiviral medications (e.g., Oseltamivir)

The analysis can inform AWS Supply Chain's planning features:
- **Demand Planning**: Forecast needs by ZIP code population
- **Supply Planning**: Optimize replenishment orders
- **Transshipment**: Move inventory from overstocked to understocked sites
