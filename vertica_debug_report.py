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
        # condition_parts = [part.strip() for part in re.split(r'([<>!=]=?|[><]=?|(?i)\b(?:ILIKE|LIKE)\b)', match, 1)]
        condition_parts = [
            part.strip() 
            for part in re.split(r'([<>!=]=?|[><]=?|(?i)\b(?:ILIKE|LIKE|IS\s+NOT|IS)\b)', match, 1)
        ]

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
                
                if isinstance(value, int) or isinstance(value, float) or placeholder=='session_type_2':
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

                if isinstance(value, int)  or isinstance(value, float) or placeholder=="err_type" or placeholder=="order_by" or placeholder=='session_type':
                    new_condition = f"{value}"
                else:
                    new_condition = f"'{value}'"

                query = query.replace(f"{{{match}}}", new_condition)

    
    return re.sub(r'\{[^}]*\}', '', query).strip()


def process_query_result_and_highlight_text(query_result):
    """
    Processes the query result to color specific substrings
    (ok, warning, fatal) in string values.

    Parameters:
        query_result (list): The nested list query result.

    Returns:
        list: The processed query result with colored strings.
    """
    # Define colors for each severity level
    colors = {
        "ok": "\033[92m",  # Green
        "warning": "\033[93m",  # Orange/Yellow
        "fatal": "\033[91m",  # Red
    }
    reset_color = "\033[0m"  # Reset to default



    def apply_color(text):
        """Apply color to the string if it contains specific keywords."""
        for severity, color_code in colors.items():
            if severity in text.lower():
                text = text.replace(severity, f"{color_code}{severity.upper()}{reset_color}")
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
                    print('QUERY: ', f"{query}", end="\n\n")
                
                query_result = execute_vertica_query(vertica_connection, query)
                if query_result == -1:
                    print(query_name, ": column not found\n")
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


def analyze(query_name, query_result, column_headers):
    thresholds_file_path = "thresholds.json"
    with open(thresholds_file_path, "r") as file:
        json_data = file.read()
        thresholds = json.loads(json_data)
        for row in thresholds:
            if query_name!= row["query_name"]:
                continue
            column_to_check = row.get("column", "cnt")
            column_to_check = row.get("column", "cnt")
            index = column_headers.index(column_to_check)

            print(f"\n\nQuery Name: {query_name}")
            print("-" * len(f"Query Name: {query_name}"))
            print(tabulate(query_result, tablefmt='grid'))

            message_template = row["message_template"]
            message = message_template.replace("{threshold}", str(row["threshold"]))
            
            is_out_of_threshold = False
            for result in query_result:
                if result[index] >= row["threshold"]:
                    is_out_of_threshold = True
            
            if is_out_of_threshold:
                print(query_name, ": ", message, sep="")
        if "status" in column_headers:
            index = column_headers.index("status")

            print(f"\n\nQuery Name: {query_name}")
            print("-" * len(f"Query Name: {query_name}"))
            print(tabulate(query_result, tablefmt='grid'))

            normal_count = 0
            for result in query_result:
                if result[index] == "normal":
                    normal_count += 1
            
            print("normal_count: ", normal_count)


def get_error_messages_query():
    return """
    ( select n.subcluster_name, em.event_timestamp, em.user_name, 'memory' as type, SUBSTRING(em.message, 1, 50) from netstats.error_messages as em JOIN nodes AS n ON n.node_name = em.node_name where 1 = 1 { user_name = 'user_name' } and em.event_timestamp >= ( TIMESTAMP { 'from_date_time' } { to_date_time } { 'issue_time' } - INTERVAL '{duration} hour' ) and n.subcluster_name = '<subcluster_name>' and em.event_timestamp <= { 'from_date_time' } { 'to_date_time' } { 'issue_time' } and em.message ilike '%memory%' ORDER BY event_timestamp limit { num_items } ) UNION ( select n.subcluster_name, em.event_timestamp, em.user_name, 'session' as type, SUBSTRING(em.message, 1, 50) from netstats.error_messages as em JOIN nodes AS n ON n.node_name = em.node_name where 1 = 1 { user_name = 'user_name' } and em.event_timestamp >= ( TIMESTAMP { 'from_date_time' } { to_date_time } { 'issue_time' } - INTERVAL '{duration} hour' ) and n.subcluster_name = '<subcluster_name>' and em.event_timestamp <= { 'from_date_time' } { 'to_date_time' } { 'issue_time' } and em.message ilike '%session%' ORDER BY event_timestamp limit { num_items } ) UNION ( select n.subcluster_name, em.event_timestamp, em.user_name, 'resource' as type, SUBSTRING(em.message, 1, 50) from netstats.error_messages as em JOIN nodes AS n ON n.node_name = em.node_name where 1 = 1 { user_name = 'user_name' } and em.event_timestamp >= ( TIMESTAMP { 'from_date_time' } { to_date_time } { 'issue_time' } - INTERVAL '{duration} hour' ) and n.subcluster_name = '<subcluster_name>' and em.event_timestamp <= { 'from_date_time' } { 'to_date_time' } { 'issue_time' } and em.message ilike '%resource%' ORDER BY event_timestamp limit { num_items } ) UNION ( select n.subcluster_name, em.event_timestamp, em.user_name, 'all' as type, SUBSTRING(em.message, 1, 50) from netstats.error_messages as em JOIN nodes AS n ON n.node_name = em.node_name where 1 = 1 { user_name = 'user_name' } and em.event_timestamp >= ( TIMESTAMP { 'from_date_time' } { to_date_time } { 'issue_time' } - INTERVAL '{duration} hour' ) and n.subcluster_name = '<subcluster_name>' and em.event_timestamp <= { 'from_date_time' } { 'to_date_time' } { 'issue_time' } ORDER BY event_timestamp limit { num_items } );
    """

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
                
                if query_name == "error_messages_raw":
                    if filters["err_type"] is None:
                        final_query = get_error_messages_query()

                
                final_query = replace_conditions(final_query, d)
                final_query = final_query.replace("<subcluster_name>", filters['subcluster_name'])
                
                query_result = execute_vertica_query(vertica_connection, final_query)
                if query_result == -1:
                    print(query_name, ": column not found\n")
                    continue
                processed_query_result = process_query_result_and_highlight_text(query_result)

                if processed_query_result:
                    column_headers = [desc[0] for desc in vertica_connection.cursor().description]
                    if is_only_insight:
                        res = analyze(query_name, query_result, column_headers)
                    else:
                        print(f"\n\nQuery Name: {query_name}")
                        print("-" * len(f"Query Name: {query_name}"))
                        print(f"Query Description: {query_description}")
                        print("-" * len(f"Query Description: {query_description}"))
                        if verbose:
                            print('QUERY: ', f"{final_query}")
                            print("-" * 15)
                        print(tabulate(processed_query_result, headers=column_headers, tablefmt='grid'))
                else:
                    print(f"\n\nQuery Name: {query_name}")
                    print("-" * len(f"Query Name: {query_name}"))
                    if verbose:
                        print('QUERY: ', f"{final_query}")
                        print("-" * 15)
                    print("No records found")
                    
        
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
        
    parser.add_argument("--subcluster-name", required=False if help_flag else True, 
        help="Name of the subcluster.")

    parser.add_argument("--inputfilepath", required=False if help_flag else True, 
        help="Path to the input CSV file in the format: qid~query_name~query~query_description.")

    parser.add_argument("--queries-to-execute", required=False, nargs="*", default=[], 
        help="Space-separated list of query names to execute. If empty, all queries will be executed.")

    parser.add_argument("--from-date-time", required=False, default=None, 
        help="Filter condition for queries with the 'from_date_time' placeholder.")

    parser.add_argument("--to-date-time", required=False, default=None, 
        help="Filter condition for queries with the 'to_date_time' placeholder.")

    parser.add_argument("--pool-name", required=False, default=None, 
        help="Filter condition for queries with the 'pool_name' placeholder.")
    
    parser.add_argument("--user-name", required=False, default=None, 
        help="Filter condition for queries with the 'user_name' placeholder.")

    parser.add_argument("--table-name", required=False, default=None, 
        help="Filter condition for queries with the 'table_name' placeholder. Supports LIKE/ILIKE with % at the start, end, or both.")

    parser.add_argument("--verbose", required=False, action="store_true", 
        help="Enable verbose mode to display executed queries.")

    parser.add_argument("--only-insights", required=False, action="store_true",
        help="")
    
    parser.add_argument("--duration", required=False, default=3,
        help="")
    
    parser.add_argument("--issue-time", required=False, 
        help="", default=None)
    
    parser.add_argument("--num-items", required=False, 
        help="", default=5)
    
    parser.add_argument("--err-type", required=False, 
        help="", default=None)
    
    parser.add_argument("--granularity", required=False, 
        help="", default='hour')
    
    parser.add_argument("--order-by", required=False, 
        help="", default=None)
    
    parser.add_argument("--snapshots", required=False, 
        help="", default=5)
    
    parser.add_argument("--issue-level", required=False, 
        help="", default=None)
    
    parser.add_argument("--user-limit", required=False, 
        help="", default=5)
    
    parser.add_argument("--type", required=False, 
        help="", default="active") # session active, inactive and all 
    
    parser.add_argument("--sort-order", required=False, 
        help="", default="desc")

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
    session_type = args.type
    pool_name = args.pool_name
    user_name = args.user_name

    if pool_name is None and user_name is not None:
        pool_name = user_name + "_pool"

    session_type_placeholder_2 = None

    if session_type == "active":
        session_type_placeholder = "is not"
    elif session_type == "inactive":
        session_type_placeholder = "is"
    else:
        session_type_placeholder = "is not"
        session_type_placeholder_2 = "null"
    
    
    filters = { # and replacements
        "subcluster_name": args.subcluster_name,
        "from_date_time": args.from_date_time,
        "to_date_time": args.to_date_time,
        "pool_name": pool_name,
        "user_name": user_name,
        "table_name": args.table_name,
        "issue_time": args.issue_time,
        "user_name": args.user_name,
        "duration": float(args.duration),
        "num_items": int(args.num_items),
        "err_type": args.err_type,
        "granularity": args.granularity,
        "order_by": args.order_by,
        "snapshots": int(args.snapshots),
        "user_limit": int(args.user_limit),
        "issue_level": args.issue_level,
        "session_type": session_type_placeholder,
        "session_type_2": session_type_placeholder_2,
        "sort_order": args.sort_order,
    }

    execute_queries_from_json(json_file_path, filters, args.verbose, is_now, is_only_insight, queries_to_execute)
    # execute_queries_from_csv(csv_path, filters, args.verbose, is_now, queries_to_execute)
