import json
import os
from dotenv import load_dotenv
import vertica_python
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
                    if placeholder=='session_type_2':
                        new_condition = f"OR {column_name} {operator} {value}"
                    else:
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


def process_query_result_and_highlight_text(query_result, column_headers):
    """
    Processes the query result to color specific substrings
    (ok, warning, fatal) in the "status" column values.

    Parameters:
        query_result (list): The nested list query result.
        column_headers (list): The list of column headers.

    Returns:
        list: The processed query result with colored strings in the "status" column.
    """
    # Get the index of the "status" column
    try:
        status_index = column_headers.index("status")
    except ValueError:
        # If "status" column doesn't exist, return the query result as is
        return query_result

    # Define colors for each severity level
    colors = {
        "ok": "\033[92m",      # Green
        "warn": "\033[93m", # Yellow
        "fatal": "\033[91m",   # Red
    }
    reset_color = "\033[0m"  # Reset to default

    def apply_color(text):
        """Apply color to the string if it contains specific keywords."""
        for severity, color_code in colors.items():
            if severity in text.lower():
                text = text.replace(severity, f"{color_code}{severity.upper()}{reset_color}")
        return text

    def process_row(row):
        """Process a single row, applying color to the 'status' column."""
        if status_index < len(row) and isinstance(row[status_index], str):
            row[status_index] = apply_color(row[status_index])
        return row

    # Process each row in the query result
    return [process_row(row) for row in query_result]


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


def get_error_messages_query():
    return """
    ( select n.subcluster_name, em.event_timestamp, em.user_name, 'memory' as type, SUBSTRING(em.message, 1, 50) from netstats.error_messages as em JOIN nodes AS n ON n.node_name = em.node_name where 1 = 1 { user_name = 'user_name' } and em.event_timestamp >= ( TIMESTAMP { 'from_date_time' } { to_date_time } { 'issue_time' } - INTERVAL '{duration} hour' ) and n.subcluster_name = '<subcluster_name>' and em.event_timestamp <= { 'from_date_time' } { 'to_date_time' } { 'issue_time' } and em.message ilike '%memory%' ORDER BY event_timestamp limit { num_items } ) UNION ( select n.subcluster_name, em.event_timestamp, em.user_name, 'session' as type, SUBSTRING(em.message, 1, 50) from netstats.error_messages as em JOIN nodes AS n ON n.node_name = em.node_name where 1 = 1 { user_name = 'user_name' } and em.event_timestamp >= ( TIMESTAMP { 'from_date_time' } { to_date_time } { 'issue_time' } - INTERVAL '{duration} hour' ) and n.subcluster_name = '<subcluster_name>' and em.event_timestamp <= { 'from_date_time' } { 'to_date_time' } { 'issue_time' } and em.message ilike '%session%' ORDER BY event_timestamp limit { num_items } ) UNION ( select n.subcluster_name, em.event_timestamp, em.user_name, 'resource' as type, SUBSTRING(em.message, 1, 50) from netstats.error_messages as em JOIN nodes AS n ON n.node_name = em.node_name where 1 = 1 { user_name = 'user_name' } and em.event_timestamp >= ( TIMESTAMP { 'from_date_time' } { to_date_time } { 'issue_time' } - INTERVAL '{duration} hour' ) and n.subcluster_name = '<subcluster_name>' and em.event_timestamp <= { 'from_date_time' } { 'to_date_time' } { 'issue_time' } and em.message ilike '%resource%' ORDER BY event_timestamp limit { num_items } ) UNION ( select n.subcluster_name, em.event_timestamp, em.user_name, 'all' as type, SUBSTRING(em.message, 1, 50) from netstats.error_messages as em JOIN nodes AS n ON n.node_name = em.node_name where 1 = 1 { user_name = 'user_name' } and em.event_timestamp >= ( TIMESTAMP { 'from_date_time' } { to_date_time } { 'issue_time' } - INTERVAL '{duration} hour' ) and n.subcluster_name = '<subcluster_name>' and em.event_timestamp <= { 'from_date_time' } { 'to_date_time' } { 'issue_time' } ORDER BY event_timestamp limit { num_items } );
    """


def analyse(query, verbose, query_name, query_result, query_description, column_headers, insights_only, with_insights, duration):
    threshold_json_file_path = "thresholds.json"
    json_data = None
    with open(threshold_json_file_path) as json_file:
        json_data = json_file.read()
        thresholds = json.loads(json_data)
    
    if thresholds is None:
        print(f"Error reading {threshold_json_file_path}")
        exit()
    
    for threshold in thresholds:
        if threshold['query_name'] == query_name:
            is_result_printed = False
            for item in threshold['columns']:
                if query_result == None or len(query_result) == 0:
                    if item['default_message'] is not "":
                        print(item['default_message'])
                        return
                
                index = column_headers.index(item['columns_name'])
                if index == -1:
                    print(f"Error: Column '{item['columns_name']}' not found in the query result.")
                    exit()
                
                ok_count, warn_count, fatal_count = 0, 0, 0
                ok_values, warn_values, fatal_values = set(), set(), set()
                unique_values = {}
                total = 0

                if item['unique_column'] == "":
                    for row in query_result:
                        if row[index] > item['threshold']['fatal']:
                            fatal_count+=1
                        elif row[index] > item['threshold']['warn']:
                            warn_count+=1
                        else:
                            total += row[index]
                            ok_count+=1
                else:
                    unique_column = item['unique_column']
                    unique_column_index = column_headers.index(unique_column)
                    for row in query_result:
                        key = row[unique_column_index]
                        if key not in unique_values:
                            unique_values[key] = 0  
                        unique_values[key] += 1  


                    for row in query_result:
                        for unique_column_value, unique_column_cnt in unique_values.items():
                            if row[unique_column_index] == unique_column_value:
                                if row[index] > item['threshold']['fatal']:
                                    fatal_count+=1
                                    fatal_values.add(unique_column_value)
                                elif row[index] > item['threshold']['warn']:
                                    warn_count+=1
                                    warn_values.add(unique_column_value)
                                else:
                                    ok_count+=1
                                    total += row[index]
                                    ok_values.add(unique_column_value)   

                if ok_count>0 or warn_count>0 or fatal_count>0:
                    if not is_result_printed:
                        is_result_printed = True
                    
                        print(f"\n\nQuery Name: {query_name}")
                        print("-" * len(f"Query Name: {query_name}"))
                        print(f"Query Description: {query_description}")
                        print("-" * len(f"Query Description: {query_description}"))
                        if verbose:
                            print('QUERY: ', f"{query}")
                            print("-" * 15)

                        if with_insights:
                            print(tabulate(query_result, headers=column_headers, tablefmt='grid'))
                
                flag = True
                if item['threshold']['ok'] != -1 and ok_count > 0:
                    flag = False
                    # ok_count += warn_count + fatal_count
                    message = "[OK] "
                    message += item['message_template']['ok'].replace('{val_cnt}', str(item['threshold']['ok']))
                    # message = message.replace('{cnt}', str(ok_count))
                    # if len(ok_values) > 0:
                    # total = sum(unique_values.values()) 

                    message = message.replace('{total}', str(total))
                    if len(ok_values) > 0:
                        message = message.replace('{list}', str(ok_values))
                        message = message.replace('{cnt}', str(len(ok_values)))
                    else:
                        message = message.replace('{cnt}', str(ok_count))
                    print(message)
                    
                if item['threshold']['warn'] != -1 and warn_count > 0:
                    flag = False
                    
                    message = "[WARN] "
                    message += item['message_template']['warn'].replace('{val_cnt}', str(item['threshold']['warn']))
                    if len(warn_values) > 0:
                        message = message.replace('{list}', str(warn_values))
                        message = message.replace('{cnt}', str(len(warn_values.union(fatal_values))))
                    else:
                        message = message.replace('{cnt}', str(warn_count))
                    print(message)

                if item['threshold']['fatal'] != -1 and fatal_count > 0:
                    flag = False
                    message = "[FATAL] "
                    message += item['message_template']['fatal'].replace('{val_cnt}', str(item['threshold']['fatal']))
                    if len(fatal_values) > 0:
                        message = message.replace('{list}', str(fatal_values))
                        message = message.replace('{cnt}', str(len(fatal_values)))
                    else:
                        message = message.replace('{cnt}', str(fatal_count))
                    print(message)

                if flag:
                    if item['default_message'] is not "":
                        print(item['default_message'])
    pass


def execute_queries_from_json(json_file_path, filters, verbose, is_now, insights_only, with_insights, queries_to_execute=None):
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
                
                column_headers = None
                processed_query_result = None

                if query_result and query_result != -1:
                    column_headers = [desc[0] for desc in vertica_connection.cursor().description]
                    processed_query_result = process_query_result_and_highlight_text(query_result, column_headers)

                if query_result == -1:
                    if verbose:
                        print('QUERY: ', f"{final_query}")
                        print("-" * 15)
                    print(query_name, ": column not found\n")
                    continue
                
                if processed_query_result:
                    if insights_only or with_insights:
                        analyse(final_query, verbose, query_name, processed_query_result, query_description, column_headers, insights_only, filters["duration"], with_insights, filters["duration"])
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
                    if not (insights_only or with_insights):
                        print(f"\n\nQuery Name: {query_name}")
                        print("-" * len(f"Query Name: {query_name}"))
                        if verbose:
                            print('QUERY: ', f"{final_query}")
                            print("-" * 15)
                        print("No records found")
                    else:
                        analyse(final_query, verbose, query_name, processed_query_result, query_description, column_headers, insights_only, with_insights, filters["duration"])
                            
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
    
    parser.add_argument("--type", required=False, 
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

    parser.add_argument("--insights-only", required=False, action="store_true", 
        help="")

    parser.add_argument("--with-insights", required=False, action="store_true", 
        help="")

    if help_flag:
        parser.print_help()
        exit(0)
    
    args = parser.parse_args()

    is_now = False
    if args.to_date_time is None and args.from_date_time is None:
        if args.issue_time is None:
            is_now = True
            args.issue_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    queries_to_execute = args.queries_to_execute
    json_file_path = args.inputfilepath
    
    type = args.type
    if queries_to_execute is not None and len(queries_to_execute) != 0:
        queries_to_execute = (queries_to_execute[0]).split(',')

    if type is not None:
        if queries_to_execute is not None and len(queries_to_execute) > 1:
            print(f"Multiple queries not allowed when --type is passed.")
            exit()
    
    query_name = None
    if queries_to_execute is not None and len(queries_to_execute) != 0:
        query_name = queries_to_execute[0] # will only work if passed only one value as general type added.

    pool_name = args.pool_name
    user_name = args.user_name

    if pool_name is None and user_name is not None:
        pool_name = user_name + "_pool"

    session_type_placeholder = "is not"
    session_type_placeholder_2 = None
    err_type = None

    if query_name == "sessions":
        if type == "active":
            session_type_placeholder = "is not"
        elif type == "inactive":
            session_type_placeholder = "is"
        else:
            session_type_placeholder = "is not"
            session_type_placeholder_2 = "null"
    elif query_name == "error_messages" or query_name == "error_messages_raw":
        err_type = type
    
    filters = { # and replacements and args
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
        "err_type": err_type,
        "granularity": args.granularity,
        "order_by": args.order_by,
        "snapshots": int(args.snapshots),
        "user_limit": int(args.user_limit),
        "issue_level": args.issue_level,
        "session_type": session_type_placeholder,
        "session_type_2": session_type_placeholder_2,
    }

    insights_only = args.insights_only
    with_insights = args.with_insights

    execute_queries_from_json(json_file_path, filters, args.verbose, is_now, insights_only, with_insights, queries_to_execute)
