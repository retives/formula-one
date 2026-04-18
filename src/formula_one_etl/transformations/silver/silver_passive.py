from pyspark import pipelines as dp
from pyspark.sql.functions import col
from transformations.utils.schemas import *


def cast_to_schema(df, schema):
    """Select and cast columns to match the declared schema."""
    return df.select([col(f.name).cast(f.dataType) for f in schema.fields])


@dp.table(
    name="dbr_dev.tokariev_silver.silver_driver_championship",
    schema=driver_championship_schema,
)
def driver_championship():
    return cast_to_schema(
        spark.read.table("dbr_dev.tokariev_bronze.bronze_driver_championship"),
        driver_championship_schema,
    ).dropDuplicates()


@dp.table(
    name="dbr_dev.tokariev_silver.silver_team_championship",
    # schema=teams_championship_schema,
)
def team_championship():
    return cast_to_schema(
        spark.read.table("dbr_dev.tokariev_bronze.bronze_team_championship"),
        teams_championship_schema,
    ).dropDuplicates()


@dp.table(
    name="dbr_dev.tokariev_silver.silver_drivers",
    schema=drivers_schema,
)
def drivers():
    return cast_to_schema(
        spark.read.table("dbr_dev.tokariev_bronze.bronze_drivers"),
        drivers_schema,
    ).dropDuplicates()


@dp.table(
    name="dbr_dev.tokariev_silver.silver_meetings",
    schema=meetings_schema,
    row_filter="ROW FILTER dbr_dev.tokariev_silver.filter_current_year ON (year)",
)
def meetings():
    return cast_to_schema(
        spark.read.table("dbr_dev.tokariev_bronze.bronze_meetings"),
        meetings_schema,
    ).dropDuplicates()


@dp.table(
    name="dbr_dev.tokariev_silver.silver_sessions",
    schema=sessions_schema,
    row_filter="ROW FILTER dbr_dev.tokariev_silver.filter_current_year ON (year)",
)
def sessions():
    return cast_to_schema(
        spark.read.table("dbr_dev.tokariev_bronze.bronze_sessions"),
        sessions_schema,
    ).dropDuplicates()
