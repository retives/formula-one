from pyspark import pipelines as dp
from transformations.utils.api_requests import *
from transformations.utils.schemas import *
@dp.table(
    name="bronze_laps_duration",
    schema=laps_schema
)
def bronze_laps_duration():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_bronze/openf1_data/laps/*.json")

@dp.table(
    name="bronze_overtakes",
    schema=overtakes_schema
)
def bronze_overtakes():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_bronze/openf1_data/overtakes/*.json")

@dp.table(
    name="bronze_pit_stops",
    schema=pit_stop_schema
)
def bronze_pit():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_bronze/openf1_data/pit/*.json")


@dp.table(
    name="bronze_weather",
    schema=weather_schema
)
def bronze_weather():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_bronze/openf1_data/weather/*.json")

@dp.table(
    name="bronze_race_control",
    schema=race_control_schema
)
def bronze_race_control():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_bronze/openf1_data/race_control/*.json")

@dp.table(
    name="bronze_position",
    schema=position_schema
)
def bronze_():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_bronze/openf1_data/position/*.json")



