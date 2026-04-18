from pyspark import pipelines as dp
from transformations.utils.schemas import *

@dp.table(
    name="dbr_dev.tokariev_bronze.bronze_laps_duration",
    schema="""
        date_start STRING,
        driver_number BIGINT,
        duration_sector_1 DOUBLE MASK dbr_dev.tokariev_bronze.mask_forbidden_double,
        duration_sector_2 DOUBLE MASK dbr_dev.tokariev_bronze.mask_forbidden_double,
        duration_sector_3 DOUBLE MASK dbr_dev.tokariev_bronze.mask_forbidden_double,
        i1_speed BIGINT,
        i2_speed BIGINT,
        is_pit_out_lap BOOLEAN,
        lap_duration DOUBLE,
        lap_number BIGINT,
        meeting_key BIGINT,
        segments_sector_1 ARRAY<BIGINT>,
        segments_sector_2 ARRAY<BIGINT>,
        segments_sector_3 ARRAY<BIGINT>,
        session_key BIGINT,
        st_speed BIGINT
    """
)
def bronze_laps_duration():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_raw/openf1_data/laps/*/*/*.json")

@dp.table(
    name="dbr_dev.tokariev_bronze.bronze_overtakes",
)
def bronze_overtakes():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_raw/openf1_data/overtakes/*/*/*.json")

@dp.table(
    name="dbr_dev.tokariev_bronze.bronze_pit_stops",
    schema="""
        date STRING,
        driver_number BIGINT,
        lane_duration DOUBLE MASK dbr_dev.tokariev_bronze.mask_forbidden_double,
        lap_number BIGINT,
        meeting_key BIGINT,
        pit_duration DOUBLE MASK dbr_dev.tokariev_bronze.mask_forbidden_double,
        session_key BIGINT,
        stop_duration DOUBLE MASK dbr_dev.tokariev_bronze.mask_forbidden_double
    """
)
def bronze_pit():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_raw/openf1_data/pit/*/*/*.json")


@dp.table(
    name="dbr_dev.tokariev_bronze.bronze_weather",
)
def bronze_weather():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_raw/openf1_data/weather/*/*/*.json")

@dp.table(
    name="dbr_dev.tokariev_bronze.bronze_race_control",
)
def bronze_race_control():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_raw/openf1_data/race_control/*/*/*.json")

@dp.table(
    name="dbr_dev.tokariev_bronze.bronze_position",
)
def bronze_():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_raw/openf1_data/position/*/*/*.json")
