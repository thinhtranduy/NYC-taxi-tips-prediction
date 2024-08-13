Negative amount for fare, tips and other fees-related columns
Negative or extremely short trip distance
Negative trip-duration
Negative or zero passenger count

Pickup datetime outside range 00-24

MTA_tax != 0.5
Improvement surcharge != 1


VendorID: integer (nullable = true)      --> isIn[1,2]
 |-- tpep_pickup_datetime: timestamp_ntz (nullable = true)     --> isIn(2023 time)    
 |-- tpep_dropoff_datetime: timestamp_ntz (nullable = true)     --> isIn(2023 time)   
 |-- passenger_count: long (nullable = true)   --> ```[count > 0]```
 |-- trip_distance: double (nullable = true)  --> ```[count > 0] ```
 |-- RatecodeID: long (nullable = true) -->         isInRange[1,6]
 |-- store_and_fwd_flag: string (nullable = true)   
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- payment_type: long (nullable = true)     isInRange[1,6]
 |-- fare_amount: double (nullable = true)   ```[count > 0] ```
 |-- extra: double (nullable = true)   ```[count >= 0] ```
 |-- mta_tax: double (nullable = true) ```[count  = 0.5] ```
 |-- tip_amount: double (nullable = true)   ```[count >= 0] ```
 |-- tolls_amount: double (nullable = true)   ```[count >= 0] ```
 |-- improvement_surcharge: double (nullable = true)  ```[count >= 0] ```
 |-- total_amount: double (nullable = true)  ```[count >= 0] ```
 |-- congestion_surcharge: double (nullable = true) ```[count >= 0] ```
 |-- Airport_fee: double (nullable = true) ```[count >= 0] ```