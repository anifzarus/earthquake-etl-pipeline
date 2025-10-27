This project is an automated ETL pipeline that extracts daily earthquake data from the USGS API, processes and cleans it, and loads it into Google BigQuery for analytics. The pipeline uses Apache Airflow for orchestration, Python for data processing, and supports backfill for historical data.

Key features:

Automated daily data ingestion using Airflow DAGs.

Cleans and transforms earthquake data: magnitude, location, depth, timestamp, tsunami info, and more.

Loads data into BigQuery using WRITE_APPEND.

Removes temporary files after loading to conserve storage.

Supports backfills with catchup=True.

Demonstrates production-aware ETL, XCom usage, and cloud integration.
