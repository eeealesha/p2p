import streamlit as st

st.write("# Web Analytic Test Case by Aleksey Gavrilov for p2p.com")

st.markdown(
    """
    **Tasks:**
    **Part 1 - Google Merchant Store**

1. Get access to dataset https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset
2. Please provide SQL queries that solve for the following:
    1. How many sessions does each user create?
    2. How much time does it take on average to purchase an item? How this time is distributed across users/items?
    3. What’s the overall conversion rate for transactions by device.category and device.mobile_brand_name?
    4. What is the most popular item for organic traffic for desktop platform?
3. Please create a dashboard that allows users to view the most popular items by traffic source and platform
4. Please provide any insight from data, that seems most interesting to you

**Part 2 - Python tasks**

1. Please write an example of python code, that checks, if the string is palindrome
”**abcba”** is palindrome.
**”abca”** is not a palindrome.
2. Please write an example of python code, that calculates the longest sequence of equal symbols. 
”**abbbcbba”** - the longest sequence is b in the length of 3. 
**”abbbccccaaa”** - the longest sequence is c in the length of 4.
"""
)