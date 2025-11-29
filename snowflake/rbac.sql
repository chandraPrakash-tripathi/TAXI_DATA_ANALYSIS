use role accountadmin;
CREATE ROLE IF NOT EXISTS dbt_role;

GRANT ROLE dbt_role TO USER CPTRIPATHI;

create warehouse dbt_wh with warehouse_size='x-small';
--Warehouse access so dbt can run queries
GRANT USAGE ON WAREHOUSE dbt_wh TO ROLE dbt_role;

--Database-level: allow using DB and creating schemas (so dbt can create stg/mart)
GRANT USAGE ON DATABASE TAXI_DB TO ROLE dbt_role;

GRANT CREATE SCHEMA ON DATABASE TAXI_DB TO ROLE dbt_role;


--Grant usage on any future schemas created in this database (so dbt can use them once created)
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE TAXI_DB TO ROLE dbt_role;

-- RAW schema: dbt needs read access to sources
GRANT USAGE ON SCHEMA TAXI_DB.RAW TO ROLE dbt_role;
GRANT SELECT ON ALL TABLES IN SCHEMA TAXI_DB.RAW TO ROLE dbt_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA TAXI_DB.RAW TO ROLE dbt_role;



