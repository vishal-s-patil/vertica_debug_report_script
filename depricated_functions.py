def prepare_date_conditions(from_date_time, to_date_time, query):
    """
    Prepares a list of tuples representing date conditions for a given query.
    
    :param date_time: The input date-time as a string in ISO 8601 format (e.g., '2025-01-15T00:00:00').
    :param table_datefield_columns_map: A dictionary mapping table names to date field column names.
    :param query: The SQL query string to extract the table name from.
    :return: A list of tuples containing the date conditions.
    """
    table_datefield_columns_map = {
        "sessions": "statement_start",
        "netstats.sessions_full": "statement_start",
        "netstats.resource_queues_full": "queue_entry_timestamp",
        "netstats.resource_queues": "queue_entry_time",
        "resource_queues": "queue_entry_timestamp",
        "netstats.storage_containers": "created_time",
        "netstats.error_messages": "event_timestamp",
        "netstats.query_profiles": "query_start",
        "query_profiles": "query_start"
    }
    try:
        # Parse the date_time string and calculate to_date_time (+1 day)
        # parsed_date_time = datetime.fromisoformat(date_time)
        parsed_from_date_time = datetime.strptime(from_date_time, '%Y-%m-%d %H:%M:%S')
        parsed_to_date_time = datetime.strptime(to_date_time, '%Y-%m-%d %H:%M:%S')
        # to_date_time = (parsed_date_time + timedelta(days=1)).isoformat()
        # to_date_time = (parsed_date_time + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
        
        # Extract the table name from the query
        table_name = extract_table_name_from_query(query)
        if table_name not in table_datefield_columns_map:
            raise ValueError(f"Table name '{table_name}' not found in table_datefield_columns_map.")

        # Get the corresponding field from the mapping
        field = table_datefield_columns_map[table_name]

        # Prepare the conditions
        conditions = [
            (field, '>=', parsed_from_date_time, 'string'),
            (field, '<', parsed_to_date_time, 'string')
        ]
        return conditions
    except Exception as e:
        print(f"Error while preparing date conditions: {e}")
        return []

def add_where_conditions_to_query(query, where_conditions):
    """
    Adds where conditions to a query by replacing the <where_conditions> placeholder.
    Always adds an 'AND' before the conditions. Handles different types of values and conditions.

    :param query: SQL query string with a placeholder <where_conditions>.
    :param where_conditions: List of tuples containing:
                             (column_name, condition, value, type_of_value)
                             Example: [("age", ">", 18, "int"), ("status", "=", "active", "string")]
    :return: Modified query with the <where_conditions> placeholder replaced.
    """
    try:
        # Validate that the placeholder exists in the query
        if "<where_conditions>" not in query:
            raise ValueError("Query must contain the placeholder <where_conditions>.")

        # Build the WHERE clause
        where_clauses = []
        for column_name, condition, value, value_type in where_conditions:
            if value_type == "string":
                value = f"'{value}'"  # Add single quotes for string values
            elif value_type == "int":
                value = str(value)  # Keep integers as-is but convert to string for query
            
            where_clauses.append(f"{column_name} {condition} {value}")

        # Join conditions with 'AND' and prepend 'AND'
        final_conditions = " AND " + " AND ".join(where_clauses) if where_clauses else ""

        # Replace placeholder in the query
        return query.replace("<where_conditions>", final_conditions)

    except Exception as e:
        print(f"Error while adding where conditions: {e}")
        return query  


def extract_table_name_from_query(query):
    """
    Extracts the table name from the query based on predefined mappings.

    :param query: The SQL query string.
    :return: The table name as a string, or 'Unknown' if no match is found.
    """
    # Lowercase the query for case-insensitive matching
    query = query.lower()

    # Define the mappings of substrings to table names
    table_mappings = {
        "from resource_queues": "resource_queues",
        "from sessions": "sessions",
        "from netstats.error_messages": "netstats.error_messages",
        "from netstats.query_profiles": "netstats.query_profiles",
        "from error_messages": "error_messages",
        "from netstats.storage_containers": "netstats.storage_containers",
        "from netstats.sessions_full": "netstats.sessions_full",
        "from netstats.resource_queues_full": "netstats.resource_queues_full",
        "from query_profiles": "query_profiles"
    }

    # Check for each mapping in the query
    for substring, table_name in table_mappings.items():
        if substring in query:
            return table_name

    # Default return if no match is found
    return "Unknown"
