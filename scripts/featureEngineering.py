from pyspark.sql.functions import *
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import *

# Cyclical encoding for months, days, and hours
def encodingTime(sdf):
    df = df.withColumn("hour_sin", sin(2 * pi() * col("pu_hour") / lit(24)))
    df = df.withColumn("hour_cos", cos(2 * pi() * col("pu_hour") / lit(24)))

    df = df.withColumn("month_sin", sin(2 * pi() * col("pu_month") / lit(12)))
    df = df.withColumn("month_cos", cos(2 * pi() * col("pu_month") / lit(12)))
    
    df = df.withColumn("day_sin", sin(2 * pi() * col("pu_day") / lit(31)))
    df = df.withColumn("day_cos", cos(2 * pi() * col("pu_day") / lit(31)))
    

def isPeakHour(sdf):
    peak_hours = (col("pu_hour").between(6, 10)) | (col("pu_hour").between(15, 19))
    sdf = sdf.withColumn("is_peak_hour", when(peak_hours, 1).otherwise(0))
    
    return sdf

def isWeekend(sdf):
    sdf = sdf.withColumn(
    'is_weekend',
    when(dayofweek(col('tpep_pickup_datetime')).isin([6, 7]), 1).otherwise(0)
    )
    return sdf

def calculateTimeTravel(sdf):
    sdf = sdf.withColumn('tpep_pickup_datetime',to_timestamp(col('tpep_pickup_datetime')))\
    .withColumn('tpep_dropoff_datetime',to_timestamp(col('tpep_dropoff_datetime')))\
    .withColumn('DiffInSeconds',unix_timestamp("tpep_dropoff_datetime") - unix_timestamp('tpep_pickup_datetime'))
    
    sdf = sdf.withColumn('trip_duration_minutes',col('DiffInSeconds')/60)
    sdf = sdf.drop('DiffInSeconds')
    return sdf


def normalizationFeature(sdf, columns): 
   for col in columns:
    # Assemble the column into a vector
    assembler = VectorAssembler(inputCols=[col], outputCol=f"{col}_vec")
    assembled_df = assembler.transform(sdf)
    
    # Apply StandardScaler
    scaler = StandardScaler(
        inputCol=f"{col}_vec",
        outputCol=f"{col}_scaled",
        withStd=True,
        withMean=True
    )
    scaler_model = scaler.fit(assembled_df)
    scaled_df = scaler_model.transform(assembled_df)
    
    # Drop the intermediate vector column
    scaled_df = scaled_df.drop(f"{col}_vec")
    
    # Update the original DataFrame with scaled features
    sdf = scaled_df
    
    return sdf
