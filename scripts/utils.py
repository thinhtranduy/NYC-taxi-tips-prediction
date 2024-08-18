from pyspark.sql import functions as F
from pyspark.sql.functions import col, unix_timestamp, month, dayofmonth, hour, format_number,  expr, when
from pyspark.sql.types import DoubleType, DateType


import pandas as pd
import numpy as np


def dropNull(sdf): 
    sdf = sdf.dropna()
    return sdf

def filteringOnCondition(sdf): 
    start_date = '2023-07-01'
    end_date = '2023-12-31'
    condition = (
    (F.col('trip_distance') > 0) &
    (F.col('passenger_count') > 0) &
    (F.col('fare_amount') > 0) &
    (F.col('extra') >= 0) &
    (F.col('mta_tax') == 0.5) &
    (F.col('tip_amount') >=  0) &
    (F.col('tolls_amount') >=  0) &
    (F.col('improvement_surcharge') >=  0) &
    (F.col('total_amount') > 0) &
    (F.col('congestion_surcharge') >=  0) &
    (F.col('airport_fee') >=  0) &
    (F.col('vendorid').isin([1, 2])) &
    (F.col('ratecodeid').isin([1, 2, 3, 4, 5, 6])) &
    (F.col('payment_type').isin([1])) &
    (F.col('pulocationid') >= 1) &
    (F.col('pulocationid') <= 263) &
    (F.col('tpep_pickup_datetime') < F.col('tpep_dropoff_datetime')) & 
    (F.col('tpep_pickup_datetime') >= F.lit(start_date).cast(DateType())) &
    (F.col('tpep_pickup_datetime') < F.lit(end_date).cast(DateType()))
    )
    sdf = sdf.withColumn(
    'is_valid_record',
    F.when(condition, True).otherwise(False)
    ) 
    sdf_valid = sdf.filter(F.col('is_valid_record') == True)
    return sdf_valid
    

def featureExtracting(sdf): 
    DROP_COLS = ['store_and_fwd_flag','mta_tax', 'is_valid_record', 'dolocationid', 
             'vendorid', 'payment_type', 'tolls_amount', 'improvement_surcharge',
             'congestion_surcharge', 'airport_fee', 'ratecodeid'
                ]
    sdf_cleaned = sdf.drop(*DROP_COLS)
    
    return sdf_cleaned


def extractingDateAndTime(sdf):
    sdf = sdf.withColumn("pu_month", month(col("tpep_pickup_datetime")))
    sdf = sdf.withColumn("pu_day", dayofmonth(col("tpep_pickup_datetime")))
    sdf = sdf.withColumn("pu_hour", hour(col("tpep_pickup_datetime")))
    
    # Reorder columns to place new columns right after 'tpep_pickup_datetime'
    columns = [
        'tpep_pickup_datetime', 'pu_month', 'pu_day', 'pu_hour'
    ] + [col for col in sdf.columns if col not in ['tpep_pickup_datetime', 'pu_month', 'pu_day', 'pu_hour']]
    
    # Select columns in the new order
    sdf = sdf.select(columns)
    
    return sdf


def reFormat(sdf): 
    # Drop the specified columns
    sdf = sdf.drop("pulocationid","tpep_pickup_datetime", "tpep_dropoff_datetime")

    # Format 'time_travel_minutes' to 4 decimal places
    sdf = sdf.withColumn("time_travel_minutes", format_number(col("time_travel_minutes"), 4))

    # Define the new column order without duplicates
    new_column_order = [
         'pu_location','pu_month', 'pu_day', 'pu_hour', 'is_weekend', 
        'time_travel_minutes', 'passenger_count', 'trip_distance', 
        'fare_amount', 'extra', 'tip_amount', 'total_amount','wind_speed',	
        'dew_point',	'atmospheric_pressure',	'temperature'
    ]

    # Reorder columns
    sdf = sdf.select(new_column_order)
    return sdf


def joiningWeatherData(data, weatherData):
    data = data.withColumn("tpep_pickup_datetime", F.date_format("tpep_pickup_datetime", "yyyy-MM-dd"))
    joined_data = data.join(weatherData, (data.tpep_pickup_datetime == weatherData.date) &
                                  (data.pu_hour == weatherData.hour), 
                         "left")
    
    result_df = joined_data.select(
    col("tpep_pickup_datetime"), col("tpep_dropoff_datetime"), col("pu_month"), col("pu_day"),
    col("pu_hour"), col("is_weekend"), col("time_travel_minutes"), col("passenger_count"),
    col("trip_distance"), col("fare_amount"), col("extra"), col("tip_amount"), col("total_amount"),
    col("pu_location"), col("wind_speed"), col("dew_point"), col("atmospheric_pressure"), col("temperature")
    )
    
    return result_df


def joiningBoroughs(data, taxiZone):
    data = data.join(taxiZone, on='pulocationid', how='left')

    data =data.withColumn(
        'pu_location',
        when(col('pulocationid') == 1, 'Newark Airport')
        .when(col('pulocationid') == 132, 'JFK Airport')
        .when(col('pulocationid') == 138, 'LaGuardia Airport')
        .otherwise(col('Borough'))
    )
    data = data.drop('Borough', 'Zone', 'service_zone')

    return data
    
    

    