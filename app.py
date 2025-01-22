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
        return connection

    except Exception as e:
        print(f"Error while connecting to Vertica: {e}")
        return None


def execute_vertica_query(vertica_connection, query):
    try:
        with vertica_connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return None


def replace_tables_in_query(query):
    replacements = [
        ("from sessions", "from netstats.sessions_full"),
        ("from resource_queues", "from netstats.resource_queues_full"),
        ("from error_messages", "from netstats.error_messages"),
        ("from resource_pool_status", "from netstats.resource_pool_status")
    ]

    query = query.lower()
    
    try:
        for old, new in replacements:
            if "from_date_time" in query or "to_date_time" in query:
                query = query.replace(old, new)
        return query
    except Exception as e:
        print(f"Error while replacing strings in query: {e}")
        return query


def replace_conditions(query, conditions_dict):
    pattern = re.compile(r'\{([^}]+)\}')
    
    matches = pattern.findall(query)
    
    for match in matches:
        condition_parts = re.split(r'([<>!=]=?|[><]=?)', match, 1)
        
        if len(condition_parts) == 3:
            column_name = condition_parts[0].strip()
            operator = condition_parts[1].strip()
            placeholder = match.split(operator, 1)[1].strip()
            
            if placeholder in conditions_dict:
                value = conditions_dict[placeholder]
                
                if isinstance(value, int):
                    new_condition = f"AND {column_name} {operator} {value}"
                else:
                    new_condition = f"AND {column_name} {operator} '{value}'"
                
                query = query.replace(f"{{{match}}}", new_condition)
    
    
    return re.sub(r'\{[^}]*\}', '', query).strip()


def execute_queries_from_csv(csv_file_path, filters, queries_to_execute=None):
    try:
        vertica_connection = get_vertica_connection()
        if not vertica_connection:
            print("Failed to connect to the Vertica database. Exiting.")
            return

        with open(csv_file_path, mode='r') as file:
            csv_reader = csv.DictReader(file, delimiter='~')
            
            for row in csv_reader:
                qid = int(row['qid'])
                query_name = row['query_name']
                query = row['query']
                
                if queries_to_execute and query_name not in queries_to_execute:
                    continue
                
                replaced_tables = False
                d = {}
                if filters['from_date_time'] is not None:
                    query = replace_tables_in_query(query)
                    # d['from_date_time'] = filters['from_date_time']
                    replaced_tables = True
                if filters['to_date_time'] is not None:
                    if not replaced_tables:
                        query = replace_tables_in_query(query)
                    # d['to_date_time'] = filters['to_date_time']
                

                for key, val in filters.items():
                    if val is not None:
                        d[key] = val

                query = replace_conditions(query, d)

                query = query.replace("<subcluster_name>", filters['subcluster_name'])
                
                print(f"\n\nQuery Name: {query_name}")
                print("-" * len(f"Query Name: {query_name}"))
                query_result = execute_vertica_query(vertica_connection, query)
                if query_result:
                    column_headers = [desc[0] for desc in vertica_connection.cursor().description]
                    print(tabulate(query_result, headers=column_headers, tablefmt='grid'))
                else:
                    print("No data returned")
        
        vertica_connection.close()
    except Exception as e:
        print(f"Error while processing the CSV file or executing queries: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Args")
    parser.add_argument("--subcluster_name", required=True)
    parser.add_argument("--inputfilepath", required=True)
    parser.add_argument("--from_date_time", required=False, default=None)
    parser.add_argument("--to_date_time", required=False, default=None)
    parser.add_argument("--pool_name", required=False, default=None)

    args = parser.parse_args()
    
    queries_to_execute = ["long_running_queries"]
    csv_path = args.inputfilepath
    
    filters = {
        "subcluster_name": args.subcluster_name,
        "from_date_time": args.from_date_time,
        "to_date_time": args.to_date_time,
        "pool_name": args.pool_name,
    }
    

    execute_queries_from_csv(csv_path, filters, queries_to_execute)
