# earthquake-etl-pipeline
An automated Airflow DAG that extracts daily earthquake data from the USGS API, transforms and cleans it, and loads it into BigQuery. Supports backfill with catchup=True, uses XComs for task communication, and deletes temporary files after loading. Cloud-ready and production-aware.
