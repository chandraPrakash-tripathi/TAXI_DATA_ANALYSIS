--study full data

SELECT *
FROM taxi_db.raw.yellow_tripdata_ext_jan
LIMIT 100;


-- segregate columns
SELECT
  VALUE:Airport_fee::FLOAT AS Airport_fee,
  VALUE:DOLocationID::INT AS DOLocationID,
  VALUE:PULocationID::INT AS PULocationID,
  VALUE:RatecodeID::INT AS RatecodeID,
  VALUE:VendorID::INT AS VendorID,
  VALUE:cbd_congestion_fee::FLOAT AS cbd_congestion_fee,
  VALUE:congestion_surcharge::FLOAT AS congestion_surcharge,
  VALUE:extra::FLOAT AS extra,
  VALUE:fare_amount::FLOAT AS fare_amount,
  VALUE:improvement_surcharge::FLOAT AS improvement_surcharge,
  VALUE:mta_tax::FLOAT AS mta_tax,
  VALUE:passenger_count::INT AS passenger_count,
  VALUE:payment_type::INT AS payment_type,
  VALUE:store_and_fwd_flag::STRING AS store_and_fwd_flag,
  VALUE:tip_amount::FLOAT AS tip_amount,
  VALUE:tolls_amount::FLOAT AS tolls_amount,
  VALUE:total_amount::FLOAT AS total_amount,
  VALUE:tpep_dropoff_datetime::BIGINT AS tpep_dropoff_datetime,
  VALUE:tpep_pickup_datetime::BIGINT AS tpep_pickup_datetime,
  VALUE:trip_distance::FLOAT AS trip_distance
FROM yellow_tripdata_ext_jan limit 100;






