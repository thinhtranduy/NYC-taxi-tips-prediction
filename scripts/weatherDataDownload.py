import requests
import pandas as pd
import json
import numpy as np
import datetime as dt
import certifi
from io import StringIO
from datetime import datetime
import os


url = "https://www.ncei.noaa.gov/data/global-hourly/access/2023/72505394728.csv"

response = requests.get(url, verify=certifi.where())
response.raise_for_status() 

weather_2023_df = pd.read_csv(StringIO(response.text))

if not os.path.exists('data'):
    os.makedirs('data')

# Save the DataFrame to a CSV file in the 'data' directory
weather_2023_df.to_csv('data/weather_2023.csv', index=False)