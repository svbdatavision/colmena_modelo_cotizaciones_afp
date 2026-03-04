def connection():
    raise RuntimeError(
        "Snowflake connector removed in Databricks migration. "
        "Use Spark SQL tables with extract.py/upload.py."
    )
