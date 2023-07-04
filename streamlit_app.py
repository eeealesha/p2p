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
    SELECT user_pseudo_id, 
    COUNT(
    DISTINCT(SELECT EP.value.int_value FROM UNNEST(event_params) AS EP WHERE key = 'ga_session_id'))
    AS session_count
    FROM `{project_id}.{dataset_id}.{table_id}`
    GROUP BY user_pseudo_id
    ORDER BY session_count DESC
"""

# Run the query and retrieve the data as a pandas DataFrame
df = client.query(query).to_dataframe()

# Calculate the mean and median values
mean_value = df["session_count"].mean()
median_value = df["session_count"].median()
st.write("Part 1 - Google Merchant Store")
st.write("a. How many sessions does each user create?")
# Display the SQL query
st.subheader("SQL Query:")
st.code(query)
# Display the mean and median values in Streamlit
st.write("Mean Value of Session Counts:", round(mean_value, 2))
st.write("Median Value of Session Counts:", median_value)
# Pagination
page_size = 10
page_number = st.selectbox("Page Number", range(1, len(df) // page_size + 2), index=0)
start_index = (page_number - 1) * page_size
end_index = page_number * page_size
paginated_df = df[start_index:end_index]

# Display the paginated session count data in Streamlit
st.write("Session Count Data (Descending Order):")
st.dataframe(paginated_df)

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
device_categories = ["desktop", "mobile", "tablet"]
traffic_sources = ["organic", "direct", "referral", "cpc", "email", "social", "other"]
order = ["purchase", "view_item", "add_to_cart", "select_item", "begin_checkout"]

# Set default values for select boxes
default_device_category = device_categories[0]  # Set first category as default
default_traffic_source = traffic_sources[0]  # Set first traffic source as default
default_order = order[0]  # Set first traffic source as default

# Add widgets for user input
selected_device_category = st.selectbox("Select Device Category:", device_categories, index=device_categories.index(default_device_category))
selected_traffic_source = st.selectbox("Select Traffic Source:", traffic_sources, index=traffic_sources.index(default_traffic_source))
selected_order = st.selectbox("Select Order By:", order, index=order.index(default_order))

# Display the result in the Streamlit app

st.write(f"Most Popular Item by {selected_order} for {selected_traffic_source} Traffic ({selected_device_category}):")

df = run_bigquery(selected_device_category, selected_traffic_source, selected_order)

st.table(df)
