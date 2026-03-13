"""
gets ZIP codes from the census, based on the user's selected US state
"""
import pandas as pd
from pathlib import Path

# The Census Gazetteer files are usually tab-delimited
# Replace with the actual URL or local path to the 2024 ZCTA gazetteer file
gazetteer_url = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_zcta_national.zip"

# Read the Gazetteer file (ZCTAs are in the first column 'GEOID')
# Note: Census uses 'INTPTLAT' and 'INTPTLONG' for centroids
gaz_df = pd.read_csv(gazetteer_url, sep='\t', dtype={'GEOID': str})

# Clean up column names (Census often adds extra spaces in Gazetteer headers)
gaz_df.columns = gaz_df.columns.str.strip()

# Define state prefixes (You can expand this list)
state_zip_prefixes = {
    'TX': ('75', '76', '77', '78', '79'),
    'CA': ('90', '91', '92', '93', '94', '95', '96'),
    'FL': ('32', '33', '34'),
    'NY': ('10', '11', '12', '13', '14'),
    'WA': ('98', '99')
}

# Select which states you want for this run
states_to_include = ['WA']
target_prefixes = tuple(p for s in states_to_include for p in state_zip_prefixes[s])

# Filter
my_centroids = gaz_df[gaz_df['GEOID'].str.startswith(target_prefixes)].copy()

# Select and rename core columns
my_centroids = my_centroids[['GEOID', 'INTPTLAT', 'INTPTLONG']]
my_centroids.rename(columns={
    'GEOID': 'zip_code',
    'INTPTLAT': 'latitude',
    'INTPTLONG': 'longitude'
}, inplace=True)

print(f"Successfully extracted {len(my_centroids)} target ZCTA centroids.")
print(my_centroids.head())

# Save to both locations
output_dir = Path(__file__).parent / "coverage-output"
output_dir.mkdir(exist_ok=True, parents=True)

my_centroids.to_csv(output_dir / 'cov_1_zip_centroids.csv', index=False)
print(f"Saved to: {output_dir / 'cov_1_zip_centroids.csv'}")