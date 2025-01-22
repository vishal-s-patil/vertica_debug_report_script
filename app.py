import json
import os
from dotenv import load_dotenv
import vertica_python
import csv
from tabulate import tabulate
import argparse
from datetime import datetime, timedelta
import re

with open("config.json", "r") as config_file:
    config = json.load(config_file)

load_dotenv()

def get_vertica_connection():
    """
    Establishes and returns a connection to the Vertica database.
    Uses the connection string from the .env file.
    Handles errors gracefully.
    """
    try:
        vertica_connection_string = os.getenv("VERTICA_CONNECTION_STRING")
        if not vertica_connection_string:
            raise ValueError("VERTICA_CONNECTION_STRING is not set in the .env file.")
        
        conn_info = {}
        for part in vertica_connection_string.split(";"):
            key, value = part.split("=")
            conn_info[key.strip()] = value.strip()

        conn_info.setdefault("tlsmode", "disable")
        connection = vertica_python.connect(**conn_info)
        # print("Successfully connected to Vertica.")
        return connection

    except Exception as e:
        print(f"Error while connecting to Vertica: {e}")
        return None


def execute_vertica_query(vertica_connection, query):
    """
    Executes a query on the given Vertica connection and returns the result.

    :param vertica_connection: Active Vertica connection object.
    :param query: SQL query to execute.
    :return: Query results as a list of tuples.
    """
    try:
        with vertica_connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return None


def replace_tables_in_query(query):
    """
    Replaces occurrences of specific strings in a query based on a list of replacements.

    :param query: The SQL query string where replacements are to be made.
    :param replacements: A list of tuples where each tuple contains:
                         (string_to_replace, replacement_string).
    :return: The query with all replacements applied.
    """
    replacements = [
        ("from sessions", "from netstats.sessions_full"),
        ("from resource_queues", "from netstats.resource_queues_full"),
        ("from error_messages", "from netstats.error_messages")
    ]

    query = query.lower()
    
    try:
        for old, new in replacements:
            query = query.replace(old, new)
        return query
    except Exception as e:
        print(f"Error while replacing strings in query: {e}")
        return query


def replace_conditions(query, conditions_dict):
    # Find all patterns like {column_name condition placeholder}
    pattern = re.compile(r'\{([^}]+)\}')
    
    # Iterate through all the matches
    matches = pattern.findall(query)
    
    for match in matches:
        # Example format: statement_start>=from_date_time
        # Split it by the first occurrence of the operator ('=', '<', '>', etc.)
        condition_parts = re.split(r'([<>!=]=?|[><]=?)', match, 1)
        
        if len(condition_parts) == 3:
            column_name = condition_parts[0].strip()
            operator = condition_parts[1].strip()
            placeholder = match.split(operator, 1)[1].strip()
            
            # Replace the placeholder with the corresponding value from the dictionary
            if placeholder in conditions_dict:
                value = conditions_dict[placeholder]
                
                # Check if the value is an integer or other type (no quotes)
                if isinstance(value, int):
                    # Construct the new condition without quotes around the value
                    new_condition = f"AND {column_name} {operator} {value}"
                else:
                    # Otherwise, keep the value as a string, enclosed in quotes
                    new_condition = f"AND {column_name} {operator} '{value}'"
                
                # Replace the entire placeholder with the new condition
                query = query.replace(f"{{{match}}}", new_condition)
    
    
    return re.sub(r'\{[^}]*\}', '', query).strip()


def execute_queries_from_csv(csv_file_path, subcluster_name, from_date_time, to_date_time, serial_numbers=None):
    """
    Reads queries from a CSV file and executes them on the Vertica database.
    Replaces the placeholder <subcluster_name> with the actual subcluster_name passed as an argument.
    Executes and tabulates only the queries whose serial numbers are provided in the list.
    If the list is empty or None, executes all queries.

    :param csv_file_path: Path to the CSV file with columns: sl_no, query_name, query
    :param subcluster_name: Name of the subcluster to replace in the queries.
    :param serial_numbers: List of serial numbers of queries to execute. Executes all queries if None or empty.
    """
    try:
        # Step 1: Establish a connection to Vertica
        vertica_connection = get_vertica_connection()
        if not vertica_connection:
            print("Failed to connect to the Vertica database. Exiting.")
            return

        # Step 2: Read the CSV file
        with open(csv_file_path, mode='r') as file:
            csv_reader = csv.DictReader(file, delimiter='~')
            
            for row in csv_reader:
                sl_no = int(row['sl_no'])
                query_name = row['query_name']
                query = row['query']
                
                # Check if the query's serial number is in the provided list (if specified)
                if serial_numbers and sl_no not in serial_numbers:
                    continue
                
                replaced_tables = False
                d = {}
                if from_date_time is not None:
                    query = replace_tables_in_query(query)
                    d['from_date_time'] = from_date_time
                    replaced_tables = True
                if to_date_time is not None:
                    if not replaced_tables:
                        query = replace_tables_in_query(query)
                    d['to_date_time'] = to_date_time
                
                query = replace_conditions(query, d)

                # Replace placeholder with the actual subcluster_name
                query = query.replace("<subcluster_name>", subcluster_name)
                
                print(f"\n\nQuery Name: {query_name}")
                print("-" * len(f"Query Name: {query_name}"))
                # print()
                # print(query)
                # print()
                # Step 3: Execute the query
                query_result = execute_vertica_query(vertica_connection, query)
                if query_result:
                    # Step 4: Format and print the result in a tabular format
                    column_headers = [desc[0] for desc in vertica_connection.cursor().description]
                    print(tabulate(query_result, headers=column_headers, tablefmt='grid'))
                else:
                    print("No data returned")
        
        # Close the Vertica connection
        vertica_connection.close()
    except Exception as e:
        print(f"Error while processing the CSV file or executing queries: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Args")
    parser.add_argument("--subcluster_name", required=True)
    parser.add_argument("--inputfilepath", required=True)
    parser.add_argument("--from_date_time", required=False, default=None)
    parser.add_argument("--to_date_time", required=False, default=None)

    queries = []
    args = parser.parse_args()
    subcluster_name_arg = args.subcluster_name
    csv_path = args.inputfilepath
    from_date_time = args.from_date_time
    to_date_time = args.to_date_time

    execute_queries_from_csv(csv_path, subcluster_name_arg, from_date_time, to_date_time, queries)
