import requests
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

BASE_URL = "https://api.openf1.org/v1"
ENDPOINTS = {
    "meetings": f"{BASE_URL}/meetings",
    "championship_drivers": f"{BASE_URL}/championship_drivers",
    "sessions": f"{BASE_URL}/sessions",
    "championship_teams": f"{BASE_URL}/championship_teams",
    "drivers": f"{BASE_URL}/drivers",
    "session_result": f"{BASE_URL}/session_result",
    "starting_grid": f"{BASE_URL}/starting_grid",

}

def ingest_data(url:str):
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Error occurred, data won't be loaded correctly. Status code - {response.status_code}")

    data = response.json()
    print(data)
    df = spark.createDataFrame(data)
    return df