import sys, os
sys.path.append("/Workspace/Users/tr3e1t0ry@softserve.academy/formula-one/src/formula_one_etl/transformations")
from utils.season_calendar import *
import requests
from datetime import datetime
from pyspark import pipelines as dp
import pandas as pd
from pyspark.sql.functions import to_timestamp, col

@dp.table(
    name="dbr_dev.tokariev_bronze.bronze_meetings",
    row_filter="ROW FILTER dbr_dev.tokariev_bronze.filter_current_year ON (year)"
)
def bronze_meetings():
    current_year = datetime.now().year
    url = f"https://api.openf1.org/v1/meetings?year={current_year}"
    
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    
    data = response.json()

    pdf = pd.DataFrame(data)
    df = spark.createDataFrame(pdf).withColumn("date_start", to_timestamp(col("date_start")))\
        .withColumn("date_end", to_timestamp(col("date_end")))
    return df

@dp.table(
    name="dbr_dev.tokariev_bronze.bronze_sessions",
    row_filter="ROW FILTER dbr_dev.tokariev_bronze.filter_current_year ON (year)"
)
def bronze_sessions():
    current_year = datetime.now().year
    url = f"https://api.openf1.org/v1/sessions?year={current_year}&session_name=Race"
    
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    
    data = response.json()
    
    pdf = pd.DataFrame(data)
    df = spark.createDataFrame(pdf).withColumn("date_start", to_timestamp(col("date_start")))\
        .withColumn("date_end", to_timestamp(col("date_end")))
    return df
