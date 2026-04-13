import requests
import json
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def load_data(url: str, name: str):
    response = requests.get(url, params={"meeting_key": "latest"})

    if response.status_code != 200:
        print(f"Error: Status code {response.status_code}")
        return 

    data = response.json()
    
    if isinstance(data, dict):
        data = [data]

    df = spark.createDataFrame(pd.DataFrame(data))
    return df
    # df.write.mode("overwrite").saveAsTable(name)