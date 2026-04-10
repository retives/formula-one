from pyspark import pipelines as dp
from transformations.utils.api_requsts import *

@dp.table(name="bronze_meetings")
def bronze_meetings():
    return ingest_data(ENDPOINTS['meetings'])

@dp.table(name="bronze_sessions")
def bronze_sessions():
    return ingest_data(ENDPOINTS['sessions'])

@dp.table(name="bronze_drivers")
def bronze_drivers():
    return ingest_data(ENDPOINTS['drivers'])

# @dp.table(name="bronze_session_results")
# def bronze_session_results():
#     return ingest_data(ENDPOINTS['session_result'])

@dp.table(name="bronze_starting_grid")
def bronze_starting_grid():
    return ingest_data(ENDPOINTS['starting_grid'])