from pyspark import pipelines as dp
from pyspark.sql import Window
import pyspark.sql.functions as F


@dp.table(
    name="dbr_dev.tokariev_gold.gold_championship_standings"
)
def championship_standings():
    window_spec = Window.partitionBy("driver_number", "year") \
    .orderBy("date_start") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    meetings = spark.read.table("dbr_dev.tokariev_silver.silver_meetings").select("meeting_key", "year", "date_start")

    df = spark.read.table("dbr_dev.tokariev_silver.silver_driver_standings").join(meetings, "meeting_key")\
        .withColumns({
            "points_gained": F.col("points_current") - F.col("points_start"),
            "position_change": F.col("position_start") - F.col("position_current"),
        })\
        .withColumn("points_count", F.sum("points_gained").over(window_spec))\
        .select("full_name", "name_acronym", "team_name", "team_colour", "points_gained", "position_change", "points_count", "meeting_key")
    return df


@dp.table(
    name="dbr_dev.tokariev_gold.gold_pit_stops_analysis"
)
def pit_stops_analysis():
    df = spark.read.table("dbr_dev.tokariev_silver.silver_driver_pit_stops")\
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
    df = spark.read.table("dbr_dev.tokariev_silver.silver_driver_overtakes")\
    .groupBy("driver_number", "session_key")\
    .agg(
        F.count("session_key").alias("overtakes_count")
    )
    return df

@dp.table(
    name="dbr_dev.tokariev_silver.gold_driver_laps"
)
def driver_laps():
    df = spark.read.table("dbr_dev.tokariev_silver.silver_driver_laps")
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