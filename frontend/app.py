import streamlit as st
import requests
import pandas as pd

# API URL
API_URL = "http://localhost:5000/globalrefresh?subcluster_name=secondary_subcluster_1"

# Function to fetch data from API
def fetch_data():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data: {e}")
        return None

# Function to convert JSON data into a table-friendly format
def transform_data(data):
    table_data = []
    for key, records in data.items():
        if isinstance(records, list):  # Ignore "last_updated"
            for record in records:
                table_data.append({"Query Name": key, "Status": record["status"], "Message": record["message"]})
    return pd.DataFrame(table_data)

# Streamlit UI
st.title("Global Refresh Dashboard")

# Load data on first page load
if "data" not in st.session_state:
    st.session_state.data = fetch_data()

# Show last updated timestamp
if st.session_state.data:
    st.write(f"**Last Updated:** {st.session_state.data.get('last_updated', 'N/A')}")

# Display table
if st.session_state.data:
    df = transform_data(st.session_state.data)
    st.table(df)

# Refresh button
if st.button("Refresh Data 🔄"):
    st.session_state.data = fetch_data()
    st.rerun()
