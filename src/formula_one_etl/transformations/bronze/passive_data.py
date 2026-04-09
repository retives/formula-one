from pyspark import pipelines as dp
from formula_one_etl.transformations.utils.schemas import *
from formula_one_etl.transformations.utils.api_requsts import *

@dp.table(
    name="bronze_meetings",
    schema=meetings_schema
)
def bronze_meetings():
    return ingest_data(ENDPOINTS['meetings'])

@dp.table(
    name="bronze_sessions",
    schema=sessions_schema
)
def bronze_sessions():
    return ingest_data(ENDPOINTS['sessions'])

@dp.table(
    name="bronze_drivers",
    schema=drivers_schema
)
def bronze_drivers():
    return ingest_data(ENDPOINTS['drivers'])

@dp.table(
    name="bronze_session_results",
    schema=session_results_schema
)
def bronze_session_results():
    return ingest_data(ENDPOINTS['session_result'])

@dp.table(
    name="bronze_starting_grid",
    schema=starting_grid_schema
)
def bronze_starting_grid():
    return ingest_data(ENDPOINTS['starting_grid'])
