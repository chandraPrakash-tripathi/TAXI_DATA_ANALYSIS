WITH jan AS (
    SELECT
        VALUE:Airport_fee::FLOAT                AS airport_fee,
        VALUE:DOLocationID::INT                 AS do_location_id,
        VALUE:PULocationID::INT                 AS pu_location_id,
        VALUE:RatecodeID::INT                   AS ratecode_id,
        VALUE:VendorID::INT                     AS vendor_id,
        VALUE:cbd_congestion_fee::FLOAT         AS cbd_congestion_fee,
        VALUE:congestion_surcharge::FLOAT       AS congestion_surcharge,
        VALUE:extra::FLOAT                      AS extra,
        VALUE:fare_amount::FLOAT                AS fare_amount,
        VALUE:improvement_surcharge::FLOAT      AS improvement_surcharge,
        VALUE:mta_tax::FLOAT                    AS mta_tax,
        VALUE:passenger_count::INT              AS passenger_count,
        VALUE:payment_type::INT                 AS payment_type,
        VALUE:store_and_fwd_flag::STRING        AS store_and_fwd_flag,
        VALUE:tip_amount::FLOAT                 AS tip_amount,
        VALUE:tolls_amount::FLOAT               AS tolls_amount,
        VALUE:total_amount::FLOAT               AS total_amount,
        VALUE:tpep_dropoff_datetime::BIGINT     AS tpep_dropoff_datetime,
        VALUE:tpep_pickup_datetime::BIGINT      AS tpep_pickup_datetime,
        VALUE:trip_distance::FLOAT              AS trip_distance,
        '2025-01'::string                       AS file_month
    FROM {{ source('taxi_raw', 'yellow_tripdata_ext_jan') }}
),

feb AS (
    SELECT
        VALUE:Airport_fee::FLOAT,
        VALUE:DOLocationID::INT,
        VALUE:PULocationID::INT,
        VALUE:RatecodeID::INT,
        VALUE:VendorID::INT,
        VALUE:cbd_congestion_fee::FLOAT,
        VALUE:congestion_surcharge::FLOAT,
        VALUE:extra::FLOAT,
        VALUE:fare_amount::FLOAT,
        VALUE:improvement_surcharge::FLOAT,
        VALUE:mta_tax::FLOAT,
        VALUE:passenger_count::INT,
        VALUE:payment_type::INT,
        VALUE:store_and_fwd_flag::STRING,
        VALUE:tip_amount::FLOAT,
        VALUE:tolls_amount::FLOAT,
        VALUE:total_amount::FLOAT,
        VALUE:tpep_dropoff_datetime::BIGINT,
        VALUE:tpep_pickup_datetime::BIGINT,
        VALUE:trip_distance::FLOAT,
        '2025-02'::string AS file_month
    FROM {{ source('taxi_raw', 'yellow_tripdata_ext_feb') }}
),

mar AS (
    SELECT
        VALUE:Airport_fee::FLOAT,
        VALUE:DOLocationID::INT,
        VALUE:PULocationID::INT,
        VALUE:RatecodeID::INT,
        VALUE:VendorID::INT,
        VALUE:cbd_congestion_fee::FLOAT,
        VALUE:congestion_surcharge::FLOAT,
        VALUE:extra::FLOAT,
        VALUE:fare_amount::FLOAT,
        VALUE:improvement_surcharge::FLOAT,
        VALUE:mta_tax::FLOAT,
        VALUE:passenger_count::INT,
        VALUE:payment_type::INT,
        VALUE:store_and_fwd_flag::STRING,
        VALUE:tip_amount::FLOAT,
        VALUE:tolls_amount::FLOAT,
        VALUE:total_amount::FLOAT,
        VALUE:tpep_dropoff_datetime::BIGINT,
        VALUE:tpep_pickup_datetime::BIGINT,
        VALUE:trip_distance::FLOAT,
        '2025-03'::string AS file_month
    FROM {{ source('taxi_raw', 'yellow_tripdata_ext_mar') }}
),

unioned AS (
    SELECT * FROM jan
    UNION ALL
    SELECT * FROM feb
    UNION ALL
    SELECT * FROM mar
),

final AS (
    SELECT
        *,
        TO_TIMESTAMP(tpep_pickup_datetime / 1000000)  AS pickup_ts,
        TO_TIMESTAMP(tpep_dropoff_datetime / 1000000) AS dropoff_ts
    FROM unioned
)

SELECT *
FROM final
