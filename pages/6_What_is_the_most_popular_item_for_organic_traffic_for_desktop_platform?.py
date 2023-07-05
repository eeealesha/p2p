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

# Prepare the SQL query to find the most popular item for organic traffic on desktop

st.write("d.What is the most popular item for organic traffic for desktop platform?")

def run_bigquery(device_category="desktop", traffic_source="organic", order="purchase"):
    query = f"""
    SELECT 
items.item_name,
COUNT(DISTINCT IF(event_name = 'purchase', user_pseudo_id, NULL)) AS purchase,
COUNT(DISTINCT IF(event_name = 'view_item', user_pseudo_id, NULL)) AS view_item,
COUNT(DISTINCT IF(event_name = 'add_to_cart', user_pseudo_id, NULL)) AS add_to_cart,
COUNT(DISTINCT IF(event_name = 'select_item', user_pseudo_id, NULL)) AS select_item,
COUNT(DISTINCT IF(event_name = 'begin_checkout', user_pseudo_id, NULL)) AS begin_checkout
FROM `{project_id}.{dataset_id}.{table_id}`,
unnest(items) as items
where items.item_name != '(not set)'
and traffic_source.medium = '{traffic_source}'
and device.category = '{device_category}'
group by items.item_name
order by {order} desc
LIMIT 1
"""
    st.subheader("SQL Query:")
    st.code(query)
    return client.query(query).to_dataframe()

# Define device categories and traffic sources for dropdowns

# Display the result in the Streamlit app

df = run_bigquery('desktop', 'organic', 'purchase')

st.write(f"Most Popular Item by purchase for organic Traffic (desktop): ")

st.dataframe(df)