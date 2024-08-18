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
response.raise_for_status()  # Raise an error for bad status codes

# Load the data into a pandas DataFrame
weather_2023_df = pd.read_csv(StringIO(response.text))

if not os.path.exists('data'):
    os.makedirs('data')

# Save the DataFrame to a CSV file in the 'data' directory
weather_2023_df.to_csv('data/weather_2023.csv', index=False)
# # print(weather_2023_df.head())
# weather_2023_df['DATE'] = pd.to_datetime(weather_2023_df['DATE'], format="%Y-%m-%dT%H:%M:%S")
df = weather_2023_df.loc[weather_2023_df['REPORT_TYPE'] == 'FM-15', :]

# print(df)
