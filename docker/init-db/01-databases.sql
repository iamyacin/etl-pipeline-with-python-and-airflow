-- Local development only. The Airflow metadata database is created by the
-- official image; these are the warehouse databases used by the pipeline.
CREATE ROLE etl LOGIN PASSWORD 'etl';

CREATE DATABASE warehouse OWNER etl;
CREATE DATABASE warehouse_test OWNER etl;

GRANT ALL PRIVILEGES ON DATABASE warehouse TO etl;
GRANT ALL PRIVILEGES ON DATABASE warehouse_test TO etl;
