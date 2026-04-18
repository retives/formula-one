from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql.functions import col
import pyspark.sql.functions as F


# --- Gold driver enrichment tables  ---

@dp.table(
    name="dbr_dev.tokariev_gold.gold_driver_standings"
)
def driver_standings():
    return spark.read.table("dbr_dev.tokariev_silver.silver_driver_championship").dropDuplicates()\
        .join(spark.read.table("dbr_dev.tokariev_silver.silver_drivers").drop("meeting_key", "session_key"), "driver_number").dropDuplicates()

@dp.table(
    name="dbr_dev.tokariev_gold.gold_driver_overtakes"
)
def driver_overtakes():
    return spark.read.table("dbr_dev.tokariev_silver.silver_overtakes").withColumn("driver_number", col("overtaking_driver_number")).dropDuplicates()\
        .join(spark.read.table("dbr_dev.tokariev_silver.silver_drivers").drop("meeting_key", "session_key"), "driver_number").dropDuplicates()

@dp.table(
    name="dbr_dev.tokariev_gold.gold_driver_laps",
    schema="""
        driver_number INT,
        date_start TIMESTAMP,
        duration_sector_1 DECIMAL(10,3) MASK dbr_dev.tokariev_gold.mask_forbidden_decimal,
        duration_sector_2 DECIMAL(10,3) MASK dbr_dev.tokariev_gold.mask_forbidden_decimal,
        duration_sector_3 DECIMAL(10,3) MASK dbr_dev.tokariev_gold.mask_forbidden_decimal,
        i1_speed INT,
        i2_speed INT,
        is_pit_out_lap BOOLEAN,
        lap_duration DECIMAL(10,3),
        lap_number INT,
        meeting_key INT,
        session_key INT,
        segments_sector_1 ARRAY<INT>,
        segments_sector_2 ARRAY<INT>,
        segments_sector_3 ARRAY<INT>,
        st_speed INT,
        broadcast_name STRING,
        country_code STRING,
        first_name STRING,
        full_name STRING,
        headshot_url STRING,
        last_name STRING,
        name_acronym STRING,
        team_colour STRING,
        team_name STRING
    """
)
def driver_laps():
    return spark.read.table("dbr_dev.tokariev_silver.silver_laps_duration").dropDuplicates()\
        .join(spark.read.table("dbr_dev.tokariev_silver.silver_drivers").drop("meeting_key", "session_key"), "driver_number").dropDuplicates()

@dp.table(
    name="dbr_dev.tokariev_gold.gold_driver_pit_stops",
    schema="""
        driver_number INT,
        date DATE,
        lane_duration DECIMAL(10,3) MASK dbr_dev.tokariev_gold.mask_forbidden_decimal,
        lap_number INT,
        meeting_key INT,
        pit_duration DECIMAL(10,3) MASK dbr_dev.tokariev_gold.mask_forbidden_decimal,
        session_key INT,
        stop_duration DECIMAL(10,3) MASK dbr_dev.tokariev_gold.mask_forbidden_decimal,
        broadcast_name STRING,
        country_code STRING,
        first_name STRING,
        full_name STRING,
        headshot_url STRING,
        last_name STRING,
        name_acronym STRING,
        team_colour STRING,
        team_name STRING
    """
)
def driver_pit_stops():
    return spark.read.table("dbr_dev.tokariev_silver.silver_pit_stops").dropDuplicates()\
        .join(spark.read.table("dbr_dev.tokariev_silver.silver_drivers").drop("meeting_key", "session_key"), "driver_number").dropDuplicates()


# --- Gold aggregation / analysis tables ---

@dp.table(
    name="dbr_dev.tokariev_gold.gold_championship_standings"
)
def championship_standings():
    window_spec = Window.partitionBy("driver_number", "year") \
    .orderBy("date_start") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    meetings = spark.read.table("dbr_dev.tokariev_silver.silver_meetings").select("meeting_key", "year", "date_start")

    df = spark.read.table("dbr_dev.tokariev_gold.gold_driver_standings").join(meetings, "meeting_key")\
        .withColumns({
            "points_gained": F.col("points_current") - F.col("points_start"),
            "position_change": F.col("position_start") - F.col("position_current"),
        })\
        .withColumn("points_count", F.sum("points_gained").over(window_spec))\
        .select("full_name", "name_acronym", "team_name", "team_colour", "points_gained", "position_change", "points_count", "meeting_key")
    return df


@dp.table(
    name="dbr_dev.tokariev_gold.gold_pit_stops_analysis",
    schema="""
        session_key INT,
        pit_stops BIGINT,
        avg_pit_stop_time DECIMAL(14,7) MASK dbr_dev.tokariev_gold.mask_forbidden_decimal_14_7,
        max_pit_stop_time DECIMAL(10,3) MASK dbr_dev.tokariev_gold.mask_forbidden_decimal,
        min_pit_stop_time DECIMAL(10,3) MASK dbr_dev.tokariev_gold.mask_forbidden_decimal
    """
)
def pit_stops_analysis():
    df = spark.read.table("dbr_dev.tokariev_gold.gold_driver_pit_stops")\
        .groupBy("session_key")\
        .agg(
            F.count("session_key").alias("pit_stops"),
            F.avg("stop_duration").alias("avg_pit_stop_time"),
            F.max("stop_duration").alias("max_pit_stop_time"),
            F.min("stop_duration").alias("min_pit_stop_time"),
        )
    return df

@dp.table(
    name="dbr_dev.tokariev_gold.gold_overtakes_driver"
)
def driver_overtakes_driver():
    df = spark.read.table("dbr_dev.tokariev_gold.gold_driver_overtakes")\
    .groupBy("driver_number", "session_key")\
    .agg(
        F.count("session_key").alias("overtakes_count")
    )
    return df


@dp.table(
    name="dbr_dev.tokariev_gold.gold_drivers"
)
def drivers():
    return spark.read.table("dbr_dev.tokariev_silver.silver_drivers")

@dp.table(
    name="dbr_dev.tokariev_gold.gold_team_championship"
)
def team_championship():
    return spark.read.table("dbr_dev.tokariev_silver.silver_team_championship")
