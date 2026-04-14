import sys
sys.path.append("/Workspace/Users/tr3e1t0ry@softserve.academy/formula-one/src/formula_one_etl/transformations")
from utils.api_requests import *
# --- CONFIGURATION ---
BASE_URL = "https://api.openf1.org/v1"
VOLUME_PATH = "/Volumes/dbr_dev/tokariev_raw/openf1_data"
RATE_LIMIT_SLEEP = 2.5
# Endpoints we want to ingest
ENDPOINTS = [
    "meetings",
    "sessions",
    "championship_drivers",
    "championship_teams",
    "drivers",
    "session_result",
    "starting_grid",
]
run_ingestion(ENDPOINTS, VOLUME_PATH, RATE_LIMIT_SLEEP)
