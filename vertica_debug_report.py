import json
import os
from dotenv import load_dotenv
import vertica_python
import csv
from tabulate import tabulate
import argparse
from datetime import datetime, timedelta
import re
import sys
from vertica_python import errors


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
    except errors.MissingColumn as e:
        #print(f"Error: Column not present in the query. Details: {e}")
        return -1
    except errors.QueryError as e:
        if "does not exist" in str(e):
            #print(f"Error: Column or table does not exist. Details: {e}")
            return -1
        else:
            print(f"Error executing query: {e}")
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
            if "from_date_time" in query or "to_date_time" in query or "issue_time" in query:
                query = query.replace(old, new)
        return query
    except Exception as e:
        print(f"Error while replacing strings in query: {e}")
        return query
    


def replace_conditions(query, conditions_dict):
    query = query.lower()
    pattern = re.compile(r'\{([^}]+)\}')
    
    matches = pattern.findall(query)
    
    for match in matches:
        # condition_parts = re.split(r'([<>!=]=?|[><]=?)', match, 1)
        # condition_parts = re.split(r'([<>!=]=?|[><]=?|(?i)\b(?:ILIKE|LIKE)\b)', match, 1)
        condition_parts = [part.strip() for part in re.split(r'([<>!=]=?|[><]=?|(?i)\b(?:ILIKE|LIKE)\b)', match, 1)]

        if len(condition_parts) == 3:
            column_name = condition_parts[0].strip()
            operator = condition_parts[1].strip()
            placeholder = match.split(operator, 1)[1].strip()
            placeholder = placeholder.strip("'")

            flag = 0
            if placeholder.startswith("%") and placeholder.endswith("%"):
                flag = 3
            elif placeholder.startswith("%"):
                flag = 1
            elif placeholder.endswith("%"):
                flag = 2

            if placeholder in conditions_dict:
                value = conditions_dict[placeholder]
                
                if isinstance(value, int):
                    new_condition = f"AND {column_name} {operator} {value}"
                else:
                    if flag == 3:
                        new_condition = f"AND {column_name} {operator} '%{value}%'"
                    elif flag == 1:
                        new_condition = f"AND {column_name} {operator} '%{value}'"
                    elif flag == 2:
                        new_condition = f"AND {column_name} {operator} '{value}%'"
                    else:
                        new_condition = f"AND {column_name} {operator} '{value}'"
                
                query = query.replace(f"{{{match}}}", new_condition)
        elif len(condition_parts) == 1:
            placeholder = condition_parts[0].strip("'")
            if placeholder in conditions_dict:
                value = conditions_dict[placeholder]

                if isinstance(value, int):
                    new_condition = f"{value}"
                else:
                    new_condition = f"'{value}'"

                query = query.replace(f"{{{match}}}", new_condition)

    
    return re.sub(r'\{[^}]*\}', '', query).strip()


def process_query_result_and_highlight_text(query_result):
    """
    Processes the query result to color specific substrings
    (normal, warning, fatal) in string values.

    Parameters:
        query_result (list): The nested list query result.

    Returns:
        list: The processed query result with colored strings.
    """
    # Define colors for each severity level
    colors = {
        "normal": "\033[92m",  # Green
        "warning": "\033[93m",  # Orange/Yellow
        "fatal": "\033[91m",  # Red
    }
    reset_color = "\033[0m"  # Reset to default

    def apply_color(text):
        """Apply color to the string if it contains specific keywords."""
        for severity, color_code in colors.items():
            if severity in text.lower():
                text = text.replace(severity, f"{color_code}{severity}{reset_color}")
        return text

    def process_item(item):
        """Recursively process each item in the query result."""
        if isinstance(item, list):
            return [process_item(sub_item) for sub_item in item]
        elif isinstance(item, str):
            return apply_color(item)
        return item  # Return as-is for non-string, non-list items

    return process_item(query_result)


def check_if_past_query_present(query_name, csv_file_path):
    with open(csv_file_path, mode='r') as file:
        csv_reader = csv.DictReader(file, delimiter='~')
        count = 0
        for row in csv_reader:
            query_name_new = row['query_name']
            if query_name_new[-5:] == "_past":
                query_name_new = query_name_new[:-5]

            if query_name_new == query_name:
                count += 1
        
        return count == 2


def execute_queries_from_csv(csv_file_path, filters, verbose, is_now, queries_to_execute=None):
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
                query_description = row['query_description']

                if is_now and query_name[-5:] == "_past":
                    continue

                is_past_query_present = False
                if not is_now:
                    is_past_query_present = check_if_past_query_present(query_name, csv_file_path)
                    if is_past_query_present:
                        query_name = query_name + '_past'
                
                if queries_to_execute and query_name not in queries_to_execute:
                    continue
                
                replaced_tables = False
                d = {}
                if filters['from_date_time'] is not None and is_past_query_present:
                    query = replace_tables_in_query(query)
                    replaced_tables = True
                if filters['to_date_time'] is not None and is_past_query_present:
                    if not replaced_tables:
                        replaced_tables = True
                        query = replace_tables_in_query(query)
                if filters['issue_time'] is not None and is_past_query_present:
                    if not is_now and not replaced_tables:
                        replaced_tables = True
                        query = replace_tables_in_query(query)
                
                for key, val in filters.items():
                    if val is not None:
                        d[key] = val

                query = replace_conditions(query, d)

                query = query.replace("<subcluster_name>", filters['subcluster_name'])
                
                if verbose:
                    print('QUERY: ', f"{query}")
                
                query_result = execute_vertica_query(vertica_connection, query)
                if query_result == -1:
                    print("column not found")
                    continue
                query_result = process_query_result_and_highlight_text(query_result)

                if query_result:
                    
                    if query_name[-5:] == "_past":
                        query_name = query_name[:-5]
                    print(f"\n\nQuery Name: {query_name}")
                    print("-" * len(f"Query Name: {query_name}"))
                    print(f"Query Description: {query_description}")
                    print("-" * len(f"Query Description: {query_description}"))
                    column_headers = [desc[0] for desc in vertica_connection.cursor().description]
                    print(tabulate(query_result, headers=column_headers, tablefmt='grid'))
                else:
                    pass
                    #print("No data returned")
        
        vertica_connection.close()
    except Exception as e:
        print(f"Error while processing the CSV file or executing queries: {e}")


class MyArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        kwargs['add_help'] = False
        super().__init__(*args, **kwargs)
        self.mandatory_arguments = ["subcluster_name", "inputfilepath"]
        
    def print_help(self, *args, **kwargs):
        """Override print_help to customize the output."""
        table_data = []
        print("All the mandatory fields are optional when --help is used.")
        for action in self._actions:
            # Add rows to the table
            table_data.append([
                f"--{action.dest}",
                "mandatory" if action.dest in self.mandatory_arguments else "optional",
                action.help
            ])
        
        # Print the table with headers
        print(tabulate(
            table_data,
            headers=["Argument", "Type", "Description"],
            tablefmt="plain",
            stralign="left"
        ))


def analyze(query_name, query_result):
    thresholds_file_path = "thresholds.json"
    with open(thresholds_file_path, "r") as file:
        json_data = file.read()
        thresholds = json.loads(json_data)
        for row in thresholds:

            print(f"\n\nQuery Name: {query_name}")
            print("-" * len(f"Query Name: {query_name}"))
            print(tabulate(query_result, tablefmt='grid'))

            message_template = row["message"]
            print(message_template)
            message = message_template.replace("{threshold}", row["threshold"])
            print('message', message)


def execute_queries_from_json(json_file_path, filters, verbose, is_now, is_only_insight, queries_to_execute=None):
    try:
        vertica_connection = get_vertica_connection()
        if not vertica_connection:
            print("Failed to connect to the Vertica database. Exiting.")
            return

        with open(json_file_path) as json_file:
            json_data = json_file.read()
            json_data = json.loads(json_data)
            for row in json_data:
                qid = row["qid"]
                query_name = row["query_name"]
                query = row["query"]
                query_description = row["query_description"]
                query_past = row.get("query_past", "")
                
                if queries_to_execute and query_name not in queries_to_execute:
                    continue

                if is_now and "select null" not in query.lower():
                    final_query = query
                elif not is_now and "select null" not in query_past.lower():
                    if query_past == "":
                        final_query = query
                    else:
                        final_query = query_past
                else:
                    continue

                if query_past == "":
                    final_query = replace_tables_in_query(final_query)

                d = {}
                for key, val in filters.items():
                    if val is not None:
                        d[key] = val
                
                final_query = replace_conditions(final_query, d)
                final_query = final_query.replace("<subcluster_name>", filters['subcluster_name'])
                
                if verbose:
                    print('QUERY: ', f"{final_query}")
                
                query_result = execute_vertica_query(vertica_connection, final_query)
                if query_result == -1:
                    print("column not found")
                    continue
                query_result = process_query_result_and_highlight_text(query_result)

                if query_result:
                    if is_only_insight:
                        res = analyze(query_name, query_result)
                    else:
                        print(f"\n\nQuery Name: {query_name}")
                        print("-" * len(f"Query Name: {query_name}"))
                        print(f"Query Description: {query_description}")
                        print("-" * len(f"Query Description: {query_description}"))
                        column_headers = [desc[0] for desc in vertica_connection.cursor().description]
                        print(tabulate(query_result, headers=column_headers, tablefmt='grid'))
                else:
                    pass
        
        vertica_connection.close()
    except Exception as e:
        print(f"Error while processing the CSV file or executing queries: {e}")
    

if __name__ == "__main__":
    parser = MyArgumentParser(description="Args")
    # parser = argparse.ArgumentParser(description="Args")
    # parser.add_argument("--help", required=False, action="store_true", help="show all command line args with description")
    help_flag = False
    if len(sys.argv) == 2:
        if sys.argv[1] == "--help":
            help_flag = True
        
    parser.add_argument("--subcluster_name", required=False if help_flag else True, 
        help="Name of the subcluster.")

    parser.add_argument("--inputfilepath", required=False if help_flag else True, 
        help="Path to the input CSV file in the format: qid~query_name~query~query_description.")

    parser.add_argument("--queries_to_execute", required=False, nargs="*", default=[], 
        help="Space-separated list of query names to execute. If empty, all queries will be executed.")

    parser.add_argument("--from_date_time", required=False, default=None, 
        help="Filter condition for queries with the 'from_date_time' placeholder.")

    parser.add_argument("--to_date_time", required=False, default=None, 
        help="Filter condition for queries with the 'to_date_time' placeholder.")

    parser.add_argument("--pool_name", required=False, default='', 
        help="Filter condition for queries with the 'pool_name' placeholder.")
    
    parser.add_argument("--user_name", required=False, default='', 
        help="Filter condition for queries with the 'user_name' placeholder.")

    parser.add_argument("--table_name", required=False, default=None, 
        help="Filter condition for queries with the 'table_name' placeholder. Supports LIKE/ILIKE with % at the start, end, or both.")

    parser.add_argument("--verbose", required=False, action="store_true", 
        help="Enable verbose mode to display executed queries.")

    parser.add_argument("--only_insights", required=False, action="store_true",
        help="")
    
    parser.add_argument("--issue_time", required=False, 
        help="", default=None)

    if help_flag:
        parser.print_help()
        exit(0)
    
    args = parser.parse_args()

    is_now = False
    if args.to_date_time is None and args.from_date_time is None:
        if args.issue_time is None:
            is_now = True
            args.issue_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    is_only_insight = args.only_insights

    # queries_to_execute = ["long_running_queries", "queue_status"]
    queries_to_execute = args.queries_to_execute
    json_file_path = args.inputfilepath
    
    filters = { # and replacements
        "subcluster_name": args.subcluster_name,
        "from_date_time": args.from_date_time,
        "to_date_time": args.to_date_time,
        "pool_name": args.pool_name,
        "user_name": args.user_name,
        "table_name": args.table_name,
        "issue_time": args.issue_time,
    }
    execute_queries_from_json(json_file_path, filters, args.verbose, is_now, is_only_insight, queries_to_execute)
    # execute_queries_from_csv(csv_path, filters, args.verbose, is_now, queries_to_execute)
