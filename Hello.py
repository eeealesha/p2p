import streamlit as st

st.write("# Web Analytic Test Case by Aleksey Gavrilov for p2p.com")

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
st.header("Part 1 - Google Merchant Store")

st.markdown(
    """
    ### Insights:
        - Users usually creaye 1.33 sessions, that means they do not comeback a lot.
        - Most users are from desktop with Chrome browser.
        - It takes 20 minutes is average to buy anything on the site, we might consider add quick checkout option for the itmes.
        - Some items have low CR, we might consider making better target promotions, thus increasing traffic qulality.
"""
)
st.subheader("a. How many sessions does each user create?")
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

st.subheader("b.How much time does it take on average to purchase an item?")

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


st.subheader("b. How this time is distributed across users?")

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
  user_pseudo_id,
  AVG(TIMESTAMP_DIFF(purchase_time, session_start_time, MINUTE)) AS average_time_between_session_purchase
FROM
  session_purchase
group by 
user_pseudo_id"""

st.subheader("SQL Query:")
st.code(query)

df = client.query(query).to_dataframe()

fig, ax = plt.subplots()
ax.hist(df['average_time_between_session_purchase'], bins=20)

st.pyplot(fig)

st.subheader("b. How this time is distributed across items?")

query = f"""with session_purchase as (
SELECT
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
     COUNT(DISTINCT event_name) = 2),
     items as (
SELECT
    user_pseudo_id,
    (SELECT EP.value.int_value FROM UNNEST(event_params) AS EP WHERE key = 'ga_session_id') as session_id,
    items.item_name
 FROM `{project_id}.{dataset_id}.{table_id}`,
unnest(items) as items
  WHERE
    event_name IN ('purchase')
    and items.item_name != '(not set)')

SELECT
   item_name,
   AVG(TIMESTAMP_DIFF(purchase_time, session_start_time, MINUTE)) AS average_time_between_session_purchase
 FROM
   session_purchase as sp join items as i
   on sp.user_pseudo_id = i.user_pseudo_id 
   and sp.session_id = i.session_id
group by item_name
"""

st.subheader("SQL Query:")
st.code(query)

df = client.query(query).to_dataframe()

fig, ax = plt.subplots()
ax.hist(df['average_time_between_session_purchase'], bins=20)

st.pyplot(fig)

st.subheader("c.Conversion Rate for Transactions by Device Category and Mobile Brand:")

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

st.subheader("d.What is the most popular item for organic traffic for desktop platform?")

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

st.subheader("d.Dashboard for most popular item?")

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

st.dataframe(df)

st.header("Part 2 - Python tasks")

def is_palindrome(word):
    # Remove spaces and convert to lowercase
    word = word.replace(" ", "").lower()
    # Check if the word is equal to its reverse
    return word == word[::-1]

st.subheader("Palindrome Checker")

st.code("""def is_palindrome(word):
    word = word.replace(" ", "").lower()
    return word == word[::-1]""")

    # User input for the word
word = st.text_input("Enter a word")

# Check if the word is a palindrome
if st.button("Check Palindrome"):
    if is_palindrome(word):
        st.write(f"{word} is a palindrome!")
    else:
        st.write(f"{word} is not a palindrome.")

def longest_sequence(word):
    max_length = 0
    current_length = 1
    longest_symbol = ""
    current_symbol = ""

    for i in range(1, len(word)):
        if word[i] == word[i - 1]:
            current_length += 1
            current_symbol = word[i]
        else:
            if current_length > max_length:
                max_length = current_length
                longest_symbol = current_symbol
            current_length = 1
            current_symbol = ""

    # Check if the last sequence is the longest
    if current_length > max_length:
        max_length = current_length
        longest_symbol = current_symbol

    return longest_symbol, max_length

st.subheader("Longest Sequence of Equal Symbols")

st.code("""def longest_sequence(word):
    max_length = 0
    current_length = 1
    longest_symbol = ""
    current_symbol = ""

    for i in range(1, len(word)):
        if word[i] == word[i - 1]:
            current_length += 1
            current_symbol = word[i]
        else:
            if current_length > max_length:
                max_length = current_length
                longest_symbol = current_symbol
            current_length = 1
            current_symbol = ""

    if current_length > max_length:
        max_length = current_length
        longest_symbol = current_symbol

    return longest_symbol, max_length""")

# User input for the word
word = st.text_input("Enter a sequence", key="sequence")

# Calculate the longest sequence of equal symbols
if st.button("Calculate"):
    symbol, length = longest_sequence(word)
    st.write(f"The longest sequence of equal symbols in {word} is '{symbol}' with a length of {length}.")