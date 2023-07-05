# Import necessary libraries
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import matplotlib.pyplot as plt

# Path to your service account JSON key file
key_path = "unique-bebop-339511-bedb79f68fdf.json"

# Create a Google Cloud BigQuery client
credentials = Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials)

# Define the BigQuery dataset and table

project_id = "bigquery-public-data"
dataset_id = "ga4_obfuscated_sample_ecommerce"
table_id = "events_*"


st.write("b.How much time does it take on average to purchase an item?")

query = f"""with session_purchase as (SELECT
    user_pseudo_id,
    (SELECT EP.value.int_value FROM UNNEST(event_params) AS EP WHERE key = 'ga_session_id') as session_id,
    TIMESTAMP_MICROS(MIN(event_timestamp)) AS session_start_time,
    TIMESTAMP_MICROS(MAX(event_timestamp)) AS purchase_time
    FROM `{project_id}.{dataset_id}.{table_id}`
  WHERE
    event_name IN ('session_start', 'purchase')
  GROUP BY
    user_pseudo_id,
    session_id
  HAVING
    COUNT(DISTINCT event_name) = 2)

SELECT
  AVG(TIMESTAMP_DIFF(purchase_time, session_start_time, MINUTE)) AS average_time_between_session_purchase
FROM
  session_purchase"""

st.subheader("SQL Query:")
st.code(query)

df = client.query(query).to_dataframe()

avg_time = round(df.iloc[0,0],2)

st.write(f"On average it takes {avg_time} minutes to purchase an item")