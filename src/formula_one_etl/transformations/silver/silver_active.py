from pyspark import pipelines as dp
from pyspark.sql.functions import col
from transformations.utils.schemas import *


def cast_to_schema(df, schema):
    """Select and cast columns to match the declared schema."""
    return df.select([col(f.name).cast(f.dataType) for f in schema.fields])

@dp.table(
    name="dbr_dev.tokariev_silver.silver_laps_duration",
    schema="""
        date_start TIMESTAMP,
        driver_number INT,
        duration_sector_1 DECIMAL(10,3) MASK dbr_dev.tokariev_silver.mask_forbidden_decimal,
        duration_sector_2 DECIMAL(10,3) MASK dbr_dev.tokariev_silver.mask_forbidden_decimal,
        duration_sector_3 DECIMAL(10,3) MASK dbr_dev.tokariev_silver.mask_forbidden_decimal,
        i1_speed INT,
        i2_speed INT,
        is_pit_out_lap BOOLEAN,
        lap_duration DECIMAL(10,3),
        lap_number INT,
        meeting_key INT NOT NULL,
        session_key INT NOT NULL,
        segments_sector_1 ARRAY<INT>,
        segments_sector_2 ARRAY<INT>,
        segments_sector_3 ARRAY<INT>,
        st_speed INT
    """,
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
    schema="""
        date DATE,
        driver_number INT NOT NULL,
        lane_duration DECIMAL(10,3) MASK dbr_dev.tokariev_silver.mask_forbidden_decimal,
        lap_number INT,
        meeting_key INT NOT NULL,
        pit_duration DECIMAL(10,3) MASK dbr_dev.tokariev_silver.mask_forbidden_decimal,
        session_key INT NOT NULL,
        stop_duration DECIMAL(10,3) MASK dbr_dev.tokariev_silver.mask_forbidden_decimal
    """,
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
