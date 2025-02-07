import streamlit as st
import requests
import pandas as pd
import hashlib

# Base API URL
BASE_API_URL = "http://localhost:5500/globalrefresh"

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
    last_updated = data.get("last_updated", "N/A")  # Global last updated timestamp

    for key, records in data.items():
        if key == "last_updated":  # Skip global last_updated field
            continue

        if isinstance(records, list):  
            for record in records:
                table_data.append({
                    "Query Name": key,
                    "Status": record["status"],
                    "Message": record["message"],
                    "Last Updated": last_updated  # Add global timestamp
                })
        elif isinstance(records, dict) and "insights" in records:
            for record in records["insights"]:
                table_data.append({
                    "Query Name": key,
                    "Status": record["status"],
                    "Message": record["message"],
                    "Last Updated": last_updated  # Add global timestamp
                })
    
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

# Show global last updated timestamp
if st.session_state.data:
    st.write(f"**Last Updated:** {st.session_state.data.get('last_updated', 'N/A')}")

# Display table with refresh buttons per row
if st.session_state.data:
    df = transform_data(st.session_state.data)

    st.write("### Query Status")
    for index, row in df.iterrows():
        col1, col2, col3, col4, col5 = st.columns([3, 2, 5, 3, 2])  # Adjust layout
        col1.write(row["Query Name"])
        col2.write(row["Status"])
        col3.write(row["Message"])
        col4.write(f"🕒 {row['Last Updated']}")  # Display last updated time

        # Create a unique key using index + hash of message
        unique_key = f"{row['Query Name']}_{index}"

        if col5.button("🔄 Refresh", key=unique_key):
            st.session_state.data = fetch_data(query_name=row["Query Name"])
            st.rerun()
