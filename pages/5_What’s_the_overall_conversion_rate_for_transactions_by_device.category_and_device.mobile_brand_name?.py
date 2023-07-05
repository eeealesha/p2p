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



st.write("c.Conversion Rate for Transactions by Device Category and Mobile Brand:")

def calculate_conversion_rate():
    query = f"""
    SELECT
        device.category AS device_category,
        device.mobile_brand_name AS mobile_brand,
        COUNT(DISTINCT IF(event_name = 'purchase', user_pseudo_id, NULL)) AS total_purchases,
        COUNT(DISTINCT IF(event_name = 'view_item', user_pseudo_id, NULL)) AS total_views,
        SAFE_DIVIDE(COUNT(DISTINCT IF(event_name = 'purchase', user_pseudo_id, NULL)), COUNT(DISTINCT IF(event_name = 'view_item', user_pseudo_id, NULL))) AS conversion_rate
FROM `{project_id}.{dataset_id}.{table_id}`
GROUP BY
        device_category,
        mobile_brand
order by conversion_rate desc
    """

    # Execute the BigQuery query
    st.subheader("SQL Query:")
    st.code(query)
    return client.query(query).to_dataframe()

# Display the result in the Streamlit app
df = calculate_conversion_rate()

st.dataframe(df)