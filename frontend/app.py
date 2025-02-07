import streamlit as st
import requests
import pandas as pd

# Base API URL
BASE_API_URL = "http://localhost:5000/globalrefresh"

# Function to fetch data from API
def fetch_data(query_name=None):
    try:
        url = f"{BASE_API_URL}?subcluster_name=secondary_subcluster_1"
        if query_name:
            url += f"&query_name={query_name}"  # Add query_name if provided

        response = requests.get(url)
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

# Global Refresh Button
if st.button("🔄 Global Refresh"):
    st.session_state.data = fetch_data()
    st.rerun()

# Show last updated timestamp
if st.session_state.data:
    st.write(f"**Last Updated:** {st.session_state.data.get('last_updated', 'N/A')}")

# Display table with refresh buttons per row
if st.session_state.data:
    df = transform_data(st.session_state.data)

    st.write("### Query Status")
    for _, row in df.iterrows():
        col1, col2, col3, col4 = st.columns([3, 2, 5, 2])  # Layout for better spacing
        col1.write(row["Query Name"])
        col2.write(row["Status"])
        col3.write(row["Message"])
        if col4.button("🔄 Refresh", key=row["Query Name"]):
            st.session_state.data = fetch_data(query_name=row["Query Name"])
            st.rerun()
