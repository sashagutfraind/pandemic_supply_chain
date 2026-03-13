"""
American Community Survey (ASS) data retrieval
"""
import requests
import pandas as pd
import numpy as np
from pathlib import Path

# Output directory
OUTPUT_DIR = Path(__file__).parent / "coverage-output"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# Cache file for raw census data
CENSUS_CACHE = OUTPUT_DIR / "cov_2_census_raw.csv"

# 1. Fetch Socioeconomic Data from Census Data Profiles
if CENSUS_CACHE.exists():
    print(f"Loading cached census data from {CENSUS_CACHE}")
    census_df = pd.read_csv(CENSUS_CACHE, dtype=str)
else:
    print("Downloading census data from API...")
    url = "https://api.census.gov/data/2022/acs/acs5/profile?get=NAME,DP03_0062E,DP03_0128PE,DP03_0001E,DP05_0001E&for=zip%20code%20tabulation%20area:*"
    response = requests.get(url)
    
    if response.status_code != 200:
        print("API Error")
        exit(1)
    
    data = response.json()
    census_df = pd.DataFrame(data[1:], columns=data[0])
    
    # Save raw data before filtering
    census_df.to_csv(CENSUS_CACHE, index=False)
    print(f"Saved raw census data to {CENSUS_CACHE}")

# configuration
states_to_include = ['WA']

# Define state prefixes (You can expand this list)
state_zip_prefixes = {
    'TX': ('75', '76', '77', '78', '79'),
    'CA': ('90', '91', '92', '93', '94', '95', '96'),
    'FL': ('32', '33', '34'),
    'NY': ('10', '11', '12', '13', '14'),
    'WA': ('98', '99')
}

target_prefixes = tuple(p for s in states_to_include for p in state_zip_prefixes[s])

my_census = census_df[census_df['zip code tabulation area'].str.startswith(target_prefixes)].copy()

# Rename for clarity
my_census.rename(columns={
    'DP03_0062E': 'median_income',
    'DP03_0128PE': 'poverty_rate',
    'DP03_0001E': 'labor_force',
    'DP05_0001E': 'total_pop',
    'zip code tabulation area': 'zip_code'
}, inplace=True)

# Convert numeric columns (Census returns strings)
cols = ['median_income', 'poverty_rate', 'labor_force', 'total_pop']
my_census[cols] = my_census[cols].apply(pd.to_numeric, errors='coerce')

# 2. Merge with zip_centroids data
zip_centroids = pd.read_csv(OUTPUT_DIR / 'cov_1_zip_centroids.csv', dtype={'zip_code': str})
my_census = my_census.merge(
    zip_centroids[['zip_code', 'latitude', 'longitude']], 
    on='zip_code', 
    how='left'
)

# 3. Save complete population data for coverage analysis (no sampling)
pop_df = my_census[['zip_code', 'latitude', 'longitude', 'total_pop', 'median_income', 'poverty_rate']].copy()
pop_df.to_csv(OUTPUT_DIR / 'cov_2_zip_population.csv', index=False)
print(f"Created cov_2_zip_population.csv with {len(pop_df)} ZIP codes for coverage analysis (no sampling).")