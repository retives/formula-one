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
)
def meetings():
    return cast_to_schema(
        spark.read.table("dbr_dev.tokariev_bronze.bronze_meetings"),
        meetings_schema,
    ).dropDuplicates()


@dp.table(
    name="dbr_dev.tokariev_silver.silver_sessions",
    schema=sessions_schema,
)
def sessions():
    return cast_to_schema(
        spark.read.table("dbr_dev.tokariev_bronze.bronze_sessions"),
        sessions_schema,
    ).dropDuplicates()


@dp.table(
    name="dbr_dev.tokariev_silver.silver_laps_duration",
    schema=laps_schema,
)
def laps_duration():
    return cast_to_schema(
        spark.read.table("dbr_dev.tokariev_bronze.bronze_laps_duration"),
        laps_schema,
    ).dropDuplicates()


@dp.table(
    name="dbr_dev.tokariev_silver.silver_overtakes",
    schema=overtakes_schema,
)
def overtakes():
    return cast_to_schema(
        spark.read.table("dbr_dev.tokariev_bronze.bronze_overtakes"),
        overtakes_schema,
    ).dropDuplicates()


@dp.table(
    name="dbr_dev.tokariev_silver.silver_pit_stops",
    schema=pit_stop_schema,
)
def pit_stops():
    return cast_to_schema(
        spark.read.table("dbr_dev.tokariev_bronze.bronze_pit_stops"),
        pit_stop_schema,
    ).dropDuplicates()


@dp.table(
    name="dbr_dev.tokariev_silver.silver_position",
    schema=position_schema,
)
def position():
    return cast_to_schema(
        spark.read.table("dbr_dev.tokariev_bronze.bronze_position"),
        position_schema,
    ).dropDuplicates()


@dp.table(
    name="dbr_dev.tokariev_silver.silver_race_control",
    schema=race_control_schema,
)
def race_control():
    return cast_to_schema(
        spark.read.table("dbr_dev.tokariev_bronze.bronze_race_control"),
        race_control_schema,
    ).dropDuplicates()


@dp.table(
    name="dbr_dev.tokariev_silver.silver_weather",
    schema=weather_schema,
)
def weather():
    return cast_to_schema(
        spark.read.table("dbr_dev.tokariev_bronze.bronze_weather"),
        weather_schema,
    ).dropDuplicates()

# Drivers

@dp.table(
    name="dbr_dev.tokariev_silver.silver_driver_standings"
)
def driver_standings():
    return spark.read.table("dbr_dev.tokariev_bronze.bronze_driver_championship").dropDuplicates()\
        .join(spark.read.table("dbr_dev.tokariev_bronze.bronze_drivers").drop("meeting_key", "session_key"), "driver_number").dropDuplicates()

@dp.table(
    name="dbr_dev.tokariev_silver.silver_driver_overtakes"
)
def driver_overtakes():
    return spark.read.table("dbr_dev.tokariev_bronze.bronze_overtakes").withColumn("driver_number", col("overtaking_driver_number")).dropDuplicates()\
        .join(spark.read.table("dbr_dev.tokariev_bronze.bronze_drivers").drop("meeting_key", "session_key"), "driver_number").dropDuplicates()

@dp.table(
    name="dbr_dev.tokariev_silver.silver_driver_laps"
)
def driver_laps():
    return spark.read.table("dbr_dev.tokariev_bronze.bronze_laps_duration").dropDuplicates()\
        .join(spark.read.table("dbr_dev.tokariev_bronze.bronze_drivers").drop("meeting_key", "session_key"), "driver_number").dropDuplicates()

@dp.table(
    name="dbr_dev.tokariev_silver.silver_driver_pit_stops"
)
def driver_pit_stops():
    return spark.read.table("dbr_dev.tokariev_bronze.bronze_pit_stops").dropDuplicates()\
        .join(spark.read.table("dbr_dev.tokariev_bronze.bronze_drivers").drop("meeting_key", "session_key"), "driver_number").dropDuplicates()




