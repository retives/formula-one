from pyspark.sql import SparkSession
import requests
import pandas as pd
from datetime import datetime

def sync_f1_calendar(endpoint):
    spark = SparkSession.builder.getOrCreate()
    base_url = "https://api.openf1.org/v1"
    current_year =  datetime.now().year
    
    m_resp = requests.get(f"{base_url}/meetings?year={current_year}")
    meetings = pd.DataFrame(m_resp.json())
    

    s_resp = requests.get(f"{base_url}/sessions?year={current_year}")
    sessions = pd.DataFrame(s_resp.json())

    calendar_df = sessions[sessions['session_name'].str.contains("Race", na=False)]
    
    final_df = calendar_df[['meeting_key', 'session_key', 'date_start', 'location', 'meeting_official_name']]
    
    spark_df = spark.createDataFrame(final_df)
    spark_df.write.format("delta").mode("overwrite").saveAsTable("f1_catalog.raw.calendar")
    print("Calendar synced to f1_catalog.raw.calendar")

# sync_f1_calendar()