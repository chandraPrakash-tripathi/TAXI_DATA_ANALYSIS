create or replace schema taxi_db.raw;

CREATE OR REPLACE FILE FORMAT parquet_format TYPE = PARQUET;

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY='"'
  NULL_IF = ('', 'NULL');


---storage integration--
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'your-arn'
  STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket/')
  COMMENT = 's3 integration';

desc integration s3_int;


--create external stage
CREATE OR REPLACE STAGE taxi_stage
  URL = 's3://your-bucket/'
  STORAGE_INTEGRATION = s3_int
  COMMENT = 'stage';

  list @taxi_stage;


--external table for fhvhv
CREATE OR REPLACE EXTERNAL TABLE taxi_db.raw.fhvhv_tripdata_ext_jan
WITH LOCATION = @taxi_stage
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*fhvhv_tripdata_2025-01.*'
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE taxi_db.raw.fhvhv_tripdata_ext_feb
WITH LOCATION = @taxi_stage
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*fhvhv_tripdata_2025-02.*'
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE taxi_db.raw.fhvhv_tripdata_ext_mar
WITH LOCATION = @taxi_stage
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*fhvhv_tripdata_2025-03.*'
AUTO_REFRESH = FALSE;

SELECT *
FROM taxi_db.raw.fhvhv_tripdata_ext_jan
LIMIT 100;

SELECT *
FROM taxi_db.raw.fhvhv_tripdata_ext_feb
LIMIT 100;

SELECT *
FROM taxi_db.raw.fhvhv_tripdata_ext_mar
LIMIT 100;


--external table for yellow_tripdata
CREATE OR REPLACE EXTERNAL TABLE taxi_db.raw.yellow_tripdata_ext_jan
WITH LOCATION = @taxi_stage
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*yellow_tripdata_2025-01.*'
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE taxi_db.raw.yellow_tripdata_ext_feb
WITH LOCATION = @taxi_stage
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*yellow_tripdata_2025-02.*'
AUTO_REFRESH = FALSE;

CREATE OR REPLACE EXTERNAL TABLE taxi_db.raw.yellow_tripdata_ext_mar
WITH LOCATION = @taxi_stage
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*yellow_tripdata_2025-03.*'
AUTO_REFRESH = FALSE;

SELECT *
FROM taxi_db.raw.yellow_tripdata_ext_jan
LIMIT 100;

SELECT *
FROM taxi_db.raw.yellow_tripdata_ext_feb
LIMIT 100;

SELECT *
FROM taxi_db.raw.yellow_tripdata_ext_mar
LIMIT 100;


list @taxi_stage;
--s3://your-bucket/taxi_zone_lookup.csv
--taxi zone lookup
CREATE OR REPLACE EXTERNAL TABLE taxi_db.raw.taxi_zone_lookup
WITH LOCATION = @taxi_stage
FILE_FORMAT = (FORMAT_NAME = csv_format)
PATTERN = '.*taxi_zone_lookup.*'
AUTO_REFRESH = FALSE;

SELECT *
FROM taxi_db.raw.taxi_zone_lookup
LIMIT 100;



