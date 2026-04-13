from pyspark import pipelines as dp
from transformations.utils.api_requests import *

@dp.table(name="bronze_meetings")
def bronze_meetings():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_bronze/openf1_data/meetings/*.json")

@dp.table(name="bronze_sessions")
def bronze_sessions():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_bronze/openf1_data/sessions/*.json")

@dp.table(name="bronze_drivers")
def bronze_drivers():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_bronze/openf1_data/drivers/*.json")

@dp.table(name="bronze_session_results")
def bronze_session_results():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_bronze/openf1_data/session_results/*.json")

@dp.table(name="bronze_starting_grid")
def bronze_starting_grid():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_bronze/openf1_data/starting_grid/*.json")

@dp.table(name="bronze_driver_championship")
def bronze_driver_championship():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_bronze/openf1_data/championship_drivers/*.json")

@dp.table(name="bronze_team_championship")
def bronze_driver_championship():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_bronze/openf1_data/championship_teams/*.json")