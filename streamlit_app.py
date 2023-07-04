# Import necessary libraries
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

# Path to your service account JSON key file
key_path = "unique-bebop-339511-bedb79f68fdf.json"

# Create a Google Cloud BigQuery client
credentials = Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials)

# Define the BigQuery dataset and table

project_id = "bigquery-public-data"
dataset_id = "ga4_obfuscated_sample_ecommerce"
table_id = "events_*"

# Prepare the SQL query
query = f"""
    SELECT user_id, COUNT(DISTINCT event_bundle_sequence_id) AS session_count
    FROM `{project_id}.{dataset_id}.{table_id}`
    GROUP BY user_id
    ORDER BY session_count DESC
"""

query = f"""
    SELECT user_pseudo_id, COUNT(DISTINCT event_bundle_sequence_id) AS session_count
    FROM `{project_id}.{dataset_id}.{table_id}`
    GROUP BY user_pseudo_id
    ORDER BY session_count DESC
"""

# Run the query and retrieve the data as a pandas DataFrame
df = client.query(query).to_dataframe()

# Calculate the mean and median values
mean_value = df['session_count'].mean()
median_value = df['session_count'].median()

# Display the mean and median values in Streamlit
st.write("Mean Value of Session Counts:", mean_value)
st.write("Median Value of Session Counts:", median_value)

# Display the SQL query
st.subheader("SQL Query:")
st.code(query)

# Pagination
page_size = 10
page_number = st.selectbox("Page Number", range(1, len(df)//page_size+2), index=0)
start_index = (page_number - 1) * page_size
end_index = page_number * page_size
paginated_df = df[start_index:end_index]

# Display the paginated session count data in Streamlit
st.write("Session Count Data (Descending Order):")
st.dataframe(paginated_df)
