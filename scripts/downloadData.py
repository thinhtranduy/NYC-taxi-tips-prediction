from urllib.request import urlretrieve
import requests

import os
import urllib
import certifi
import ssl

output_relative_dir = './data'

# check if it exists as it makedir will raise an error if it does exist
if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)
    
    
tlc_output_dir = output_relative_dir + '/landing'
taxi_zones_output_dir =  output_relative_dir + '/taxi_zones'

for target_dir in ('/landing', '/taxi_zones'):
    if not os.path.exists(output_relative_dir + target_dir):
        os.makedirs(output_relative_dir + target_dir)


YEAR = '2023'
MONTHS = range(7,13)
TYPE = "yellow_cab"

URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"#year-month.parquet
    
# Ensure the output directory exists
# os.makedirs(output_relative_dir, exist_ok=True)

# Create SSL context with certifi
session = requests.Session()
session.verify = certifi.where()

for month in MONTHS:
    month = str(month).zfill(2)
    print(f"Begin month {month}")

    # Generate URL and output location
    url = f'{URL_TEMPLATE}{YEAR}-{month}.parquet'
    output_dir = f"{tlc_output_dir}/{YEAR}-{month}-{TYPE}.parquet"

    # Download file
    response = session.get(url, stream=True)
    response.raise_for_status()  # Check for HTTP errors
    
    # Save the file
    with open( output_dir, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    
    print(f"Completed month {month}")