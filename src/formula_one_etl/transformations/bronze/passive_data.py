from pyspark import pipelines as dp


# @dp.table(name="dbr_dev.tokariev_bronze.bronze_meetings")
# def bronze_meetings():
#     return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_raw/openf1_data/meetings/*/*/*.json")

# @dp.table(name="dbr_dev.tokariev_bronze.bronze_sessions")
# def bronze_sessions():
#     return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_raw/openf1_data/sessions/*/*/*.json")

@dp.table(name="dbr_dev.tokariev_bronze.bronze_drivers")
def bronze_drivers():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_raw/openf1_data/drivers/*/*/*.json")

@dp.table(name="dbr_dev.tokariev_bronze.bronze_session_results")
def bronze_session_results():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_raw/openf1_data/session_result/*/*/*.json")

# @dp.table(name="dbr_dev.tokariev_bronze.bronze_starting_grid")
# def bronze_starting_grid():
#     return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_raw/openf1_data/starting_grid/*/*/*.json")

@dp.table(name="dbr_dev.tokariev_bronze.bronze_driver_championship")
def bronze_driver_championship():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_raw/openf1_data/championship_drivers/*/*/*.json")

@dp.table(name="dbr_dev.tokariev_bronze.bronze_team_championship")
def bronze_driver_championship():
    return spark.read.format("json").load("/Volumes/dbr_dev/tokariev_raw/openf1_data/championship_teams/*/*/*.json")