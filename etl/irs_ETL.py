import pandas as pd
import requests
from io import StringIO

# function to download and parse site data
def download_and_parse_data(url):
    response = requests.get(url)
    
    # Ensure the request was successful
    if response.status_code != 200:
        raise Exception(f"Failed to download data: {response.status_code}")
    
    # Parse the CSV data into a pandas DataFrame
    data = pd.read_csv(StringIO(response.text), header=0)
    
    return data

# Dataframe 1
# URL from the irs website
url = 'https://www.irs.gov/pub/irs-soi/21zpallagi.csv'

# Download and parse the data
irs_data = download_and_parse_data(url)

# filter by New York state
ny_df = irs_data[irs_data['STATE'] == 'NY'].reset_index(drop=True)


# Dataframe 2
# URL for nyc zip codes/neighborhoods
url = 'https://raw.githubusercontent.com/erikgregorywebb/nyc-housing/master/Data/nyc-zip-codes.csv'

# Download and parse the data
zips = download_and_parse_data(url)

# rename columns to make merging the 2 dataframes easier
zips = zips.rename(columns={'ZipCode': 'zipcode'})

# merge tax data with nyc neighborhood data on zipcode
nyc_irs = pd.merge(ny_df, zips, on='zipcode')

# save final csv to be used for adding data to database
nyc_irs.to_csv('./nyc_irs.csv', index=False)