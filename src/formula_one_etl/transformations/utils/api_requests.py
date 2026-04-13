import requests
import json
import os
import time
from datetime import datetime, timezone
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
]

http = requests.Session()

def get_utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

def get_latest_keys():
    """
    Resolves the actual latest meeting and its primary Race session.
    F1 'latest' can be buggy, so we fetch the meeting then find the sessions.
    """
    print("Resolving latest meeting and session keys...")
    
    m_resp = http.get(f"{BASE_URL}/meetings?meeting_key=latest")
    m_resp.raise_for_status()
    meeting_key = m_resp.json()[0]['meeting_key']
    
    date = get_utc_now_iso()
    s_resp = http.get(f"{BASE_URL}/sessions", params={"meeting_key": meeting_key, "date_end<":date})
    s_resp.raise_for_status()
    sessions = s_resp.json()
    
    sessions.sort(key=lambda x: x['session_key'], reverse=True)
    
    session_key = sessions[0]['session_key']
    for s in sessions:
        if "Race" in s['session_name']:
            session_key = s['session_key']
            break
            
    return meeting_key, session_key

def fetch_with_retry(url, params, retries=10):
    """Iterative approach to handle 404s or network blips."""
    for i in range(retries):
        try:
            response = http.get(url, params=params, timeout=15)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                print(f"  [404] No data found for {url}. Retrying with shifted params...")
                # If meeting_key didn't work, we decrement and try the previous one
                if "meeting_key" in params:
                    params["meeting_key"] -= 1
                if "session_key" in params:
                    params["session_key"] -= 1
            else:
                print(f"  [Error] Status {response.status_code}")
        except Exception as e:
            print(f"  [Attempt {i+1}] Connection error: {e}")
        
        time.sleep(1) # Small backoff
    return None

def save_to_volume(data, endpoint, m_key, s_key, volume_path):
    """Saves the JSON to the Unity Catalog Volume path."""
    folder_path = f"{VOLUME_PATH}/{endpoint}/m_{m_key}/s_{s_key}"
    os.makedirs(folder_path, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"{folder_path}/{endpoint}.json"
    
    with open(file_path, "w") as f:
        json.dump(data, f)
    
    return file_path

def run_ingestion(endpoints, base_url, volume_path):
    try:
        meeting_key, session_key = get_latest_keys()
        print(f"Targeting Meeting: {meeting_key} | Session: {session_key}")
        print("-" * 30)
        
        for endpoint in endpoints:
            print(f"Processing: {endpoint}...")
            
            if endpoint in ["sessions", "championship_drivers", "championship_teams", "meetings"]:
                params = {"meeting_key": meeting_key}
            else:
                params = {"session_key": session_key}
                
            data = fetch_with_retry(f"{base_url}/{endpoint}", params)
            
            if data and len(data) > 0:
                saved_path = save_to_volume(data, endpoint, meeting_key, session_key, volume_path)
                print(f"  [Success] Saved {len(data)} records to: {saved_path}")
            else:
                print(f"  [Skipped] No data retrieved for {endpoint}")
                
    except Exception as e:
        print(f"Critical Failure in Ingestion: {e}")


