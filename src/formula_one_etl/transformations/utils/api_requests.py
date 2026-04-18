import requests
import json
import os
import time
from datetime import datetime, timezone
from pyspark.sql import SparkSession

# # --- CONFIGURATION ---
# BASE_URL = "https://api.openf1.org/v1"
# VOLUME_PATH = "/Volumes/dbr_dev/tokariev_raw/openf1_data"
# RATE_LIMIT_SLEEP = 2.5

spark = SparkSession.builder.getOrCreate()

def get_target_session():
    """
    Reads the calendar table and finds the session closest to 'now' 
    that has already started.
    """
    now = datetime.now(timezone.utc)
    
    query = f"""
        SELECT meeting_key, session_key 
        FROM dbr_dev.tokariev_bronze.bronze_sessions 
        WHERE YEAR(date_start) = {datetime.now(timezone.utc).year}
        AND date_start < '{now}'
        ORDER BY date_start DESC
    """

    rows = spark.sql(query).collect()
    
    return [(row['meeting_key'], row['session_key']) for row in rows]

def fetch_and_save(endpoint, m_key, s_key):
    """
    Fetches data and saves to Volume. 
    Returns: (Success Boolean, Status Code)
    """
    params = {"meeting_key": m_key} if endpoint in ["sessions", "championship_drivers", "championship_teams", "meetings"] else {"session_key": s_key}
    
    print(f"  Fetching {endpoint} for Session {s_key}...")
    try:
        response = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=20)
        
        if response.status_code == 200:
            data = response.json()
            if data:
                folder_path = f"{VOLUME_PATH}/{endpoint}/m_{m_key}/s_{s_key}"
                os.makedirs(folder_path, exist_ok=True)
                file_path = f"{folder_path}/{endpoint}.json"
                
                with open(file_path, "w") as f:
                    json.dump(data, f)
                return True, 200
            return False, 204
            
        return False, response.status_code
    except Exception as e:
        print(f"    Error: {e}")
        return False, 500

def run_ingestion(ENDPOINTS, VOLUME_PATH, RATE_LIMIT_SLEEP, ):
    # Get the last 5 sessions to allow for fallback
    sessions = get_target_session()
    
    if not sessions:
        print("No active or past sessions found.")
        return
    print(sessions)
    for endpoint in ENDPOINTS:
        success = False
        
        # Retry logic: Try current session, if 404, try previous
        for m_key, s_key in sessions:
            time.sleep(RATE_LIMIT_SLEEP)
            
            is_ok, status = fetch_and_save(endpoint, m_key, s_key)
            
            if is_ok:
                print(f"    [Success] Saved {endpoint} for Session {s_key}")
                success = True

            
            elif status == 404:
                print(f"    [404 Not Found] Session {s_key} has no data for {endpoint}. Trying previous session...")
                continue # Go to the next session in the list
                
            else:
                print(f"    [Failed] Status {status} for {endpoint}. Skipping this endpoint.")
                break 

        if not success:
            print(f"    [Final Notice] Could not ingest {endpoint} after checking multiple sessions.")
