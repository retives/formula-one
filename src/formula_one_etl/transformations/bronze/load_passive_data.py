
# --- CONFIGURATION ---
BASE_URL = "https://api.openf1.org/v1"
VOLUME_PATH = "/Volumes/dbr_dev/tokariev_raw/openf1_data"

# Endpoints we want to ingest
ENDPOINTS = [
    "championship_drivers",
    "sessions",
    "championship_teams",
    "drivers",
    "session_result",
    "meetings",
    "starting_grid",
    
]

run_ingestion(ENDPOINTS, BASE_URL, VOLUME_PATH)
