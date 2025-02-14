import json
import sys
from dotenv import load_dotenv
from tabulate import tabulate
from datetime import datetime, timedelta
import re
from vertica import vertica
from modules.helpers import replace_conditions, push_to_insights_json, replace_tables_in_query, process_query_result_and_highlight_text
from modules.helpers import get_past_datetime
from query_breakdown import query_breakdown
from modules.args_parser import get_args, pargse_args

THRESHOLD_FILE_PATH="thresholds.json"

def get_error_messages_query():
    return """
    select * from (( select n.subcluster_name, em.transaction_id, em.statement_id, em.event_timestamp, em.user_name, 'memory' as type, SUBSTRING(em.message, 1, 50) from netstats.error_messages as em JOIN nodes AS n ON n.node_name = em.node_name where 1 = 1 { user_name = 'user_name' } and em.event_timestamp >= ( TIMESTAMP { 'from_date_time' } { to_date_time } { 'issue_time' } - INTERVAL '{duration} hour' ) and n.subcluster_name = '<subcluster_name>' and em.event_timestamp <= { 'from_date_time' } { 'to_date_time' } { 'issue_time' } and em.message ilike '%memory%' ORDER BY event_timestamp limit { num_items } ) UNION ( select n.subcluster_name, em.transaction_id, em.statement_id, em.event_timestamp, em.user_name, 'session' as type, SUBSTRING(em.message, 1, 50) from netstats.error_messages as em JOIN nodes AS n ON n.node_name = em.node_name where 1 = 1 { user_name = 'user_name' } and em.event_timestamp >= ( TIMESTAMP { 'from_date_time' } { to_date_time } { 'issue_time' } - INTERVAL '{duration} hour' ) and n.subcluster_name = '<subcluster_name>' and em.event_timestamp <= { 'from_date_time' } { 'to_date_time' } { 'issue_time' } and em.message ilike '%session%' ORDER BY event_timestamp limit { num_items } ) UNION ( select n.subcluster_name, em.transaction_id, em.statement_id, em.event_timestamp, em.user_name, 'resource' as type, SUBSTRING(em.message, 1, 50) from netstats.error_messages as em JOIN nodes AS n ON n.node_name = em.node_name where 1 = 1 { user_name = 'user_name' } and em.event_timestamp >= ( TIMESTAMP { 'from_date_time' } { to_date_time } { 'issue_time' } - INTERVAL '{duration} hour' ) and n.subcluster_name = '<subcluster_name>' and em.event_timestamp <= { 'from_date_time' } { 'to_date_time' } { 'issue_time' } and em.message ilike '%resource%' ORDER BY event_timestamp limit { num_items } ) UNION ( select n.subcluster_name, em.transaction_id, em.statement_id, em.event_timestamp, em.user_name, 'all' as type, SUBSTRING(em.message, 1, 50) from netstats.error_messages as em JOIN nodes AS n ON n.node_name = em.node_name where 1 = 1 { user_name = 'user_name' } and em.event_timestamp >= ( TIMESTAMP { 'from_date_time' } { to_date_time } { 'issue_time' } - INTERVAL '{duration} hour' ) and n.subcluster_name = '<subcluster_name>' and em.event_timestamp <= { 'from_date_time' } { 'to_date_time' } { 'issue_time' } ORDER BY event_timestamp limit { num_items } ) ) as x order by {order_by} event_timestamp;
    """


def get_ips_and_nodes(subcluster_name):
    query = f"select node_address, node_name from nodes where subcluster_name='{subcluster_name}';"
    connection = vertica.get_vertica_connection()
    query_result = vertica.execute_vertica_query(connection, query)

    if query_result is None or len(query_result) == 0:
        print(f"Error getting nodes and ips for subcluster {subcluster_name}")
        exit()
    
    ips, nodes = [], []
    for row in query_result:
        ips.append(row[0])
        nodes.append(row[1])
    return ips, nodes

is_header_printed = False

def print_header(args):
    ips, nodes = get_ips_and_nodes(args["subcluster_name"])

    d = {
        "Subcluster": args["subcluster_name"],
        "User": args["user_name"],
        "Pool": args["pool_name"],
        "Nodes": nodes,
        "IPs": ips,
        "Issue Duration": f"For past {args['duration']} hours from now." if args['is_now'] else "From " + get_past_datetime(args["issue_time"], args['duration']) + " To " + str(args["issue_time"])
    }

    table_data = [[k, v] for k, v in d.items() if v is not None]

    print(tabulate(table_data, tablefmt="grid"))


def replace_row_num_limit(query, new_limit):
    pattern = r"rs\.row_num\s*<=\s*\d+\s"
    replacement = f"rs.row_num <= {new_limit} "
    return re.sub(pattern, replacement, query)


def colour_values(query_result, columns, headers):
    for column in columns:
        if column['columns_name'] == 'deleted_row_cnt':
            continue
        try:
            column_name = column['columns_name']
            index = headers.index(column_name)
            if index == -1:
                print(f'column {column_name} not found')
                return

            _, warn_threshold, fatal_threshold = get_thresholds(column['threshold'])

        except Exception as e:
            print(f'Error in func:colour_values while getting threshold information.', e)
            return

        try:
            for row in query_result:
                if row[index] >= fatal_threshold:
                    row[index] = str('\033[91m') + str(row[index]) + str('\033[0m')
                elif row[index] >= int(warn_threshold):
                    row[index] = str('\033[93m') + str(row[index]) + str('\033[0m')
                else:
                    row[index] = str('\033[92m') + str(row[index]) + str('\033[0m')
        except Exception as e:
            print(f'Error in func:colour_values while coloring the values', e)
            return

    return query_result

def colour_values_deleted_row_count(query_result, item, with_insights, threshold, column_headers):    
    try:
        column_to_colour = "deleted_row_cnt"
        column_to_compare = "total_row_cnt"

        column_to_colour_index = column_headers.index(column_to_colour)
        column_to_compare_index = column_headers.index(column_to_compare)
    except Exception as e:
        print(f'Error in func:colour_values_deleted_row_count while getting column indices.', e)
        exit()
    
    for row in query_result:
        try:
            ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
            row[column_to_colour_index] = int(ansi_escape.sub('', str(row[column_to_colour_index])))

            _, warn_threshold, fatal_threshold = get_thresholds(item['threshold'])

            if int(row[column_to_colour_index]) >= int(row[column_to_compare_index])*(fatal_threshold/100):
                row[column_to_colour_index] = str('\033[91m') + str(row[column_to_colour_index]) + str('\033[0m')
            elif int(row[column_to_colour_index]) >= row[column_to_compare_index]*(warn_threshold/100):
                row[column_to_colour_index] = str('\033[93m') + str(row[column_to_colour_index]) + str('\033[0m')
            else:
                row[column_to_colour_index] = str('\033[92m') + str(row[column_to_colour_index]) + str('\033[0m')
        except Exception as e:
            print(f'Error in func:colour_values_deleted_row_count while coloring the values', e)
            exit()
    
    return query_result


def handle_deleted_row_count(query_result, query_result_show, item, with_insights, threshold, column_headers):
    if with_insights:
        if query_result_show:
            # query_result_show = colour_values(query_result_show, threshold['columns'], column_headers)
            return colour_values_deleted_row_count(query_result_show, item, with_insights, threshold, column_headers)
        else:
            # query_result = colour_values(query_result, threshold['columns'], column_headers)
            return colour_values_deleted_row_count(query_result, item, with_insights, threshold, column_headers)


def get_thresholds(thresholds):
    ok_threshold, warn_threshold, fatal_threshold = None, None, None

    for key, val in thresholds.items():
        if 'warn' in key:
            warn_threshold = val
        elif 'fatal' in key:
            fatal_threshold = val
        else:
            ok_threshold = val
    
    return ok_threshold, warn_threshold, fatal_threshold


def analyse(qid, insights_json, query, verbose, query_name, query_result, query_description, column_headers, insights_only, with_insights, duration, pool_name, issue_level, is_now, user_name, subcluster_name, issue_time, vertica_connection, filters):
    threshold_json_file_path = THRESHOLD_FILE_PATH
    json_data = None
    with open(threshold_json_file_path) as json_file:
        json_data = json_file.read()
        thresholds = json.loads(json_data)
    
    if thresholds is None:
        print(f"Error reading {threshold_json_file_path}")
        exit()
    query_result_show = None

    # if_printref = 0
    for threshold in thresholds:
        if threshold['query_name'] == query_name:
            if with_insights or insights_only:
                
                query_result_show = vertica.execute_vertica_query(vertica_connection, query)
                if column_headers is not None:
                    query_result_show = process_query_result_and_highlight_text(query_result_show, column_headers)

                replaced_query = re.sub(r"LIMIT\s+\d+", "", query, flags=re.IGNORECASE)
                replaced_query = replace_row_num_limit(replaced_query, 1000)

                query_result = vertica.execute_vertica_query(vertica_connection, replaced_query)
            
                if query_result == -1:
                    print(query_name, ": column not found\n")
                    return
            
            if threshold['query_name'] == query_name:
                args = {
                    "subcluster_name": subcluster_name,
                    "user_name": user_name,
                    "pool_name": pool_name,
                    "is_now": is_now,
                    "issue_time": issue_time,
                    "duration": duration,
                }
                global is_header_printed
                if not is_header_printed:
                    is_header_printed = True
                    print_header(args)
                    
                if query_name == "long_running_queries_raw":
                    return
                if query_name == "resource_pool_status":
                    if pool_name is None:
                        print('resource_pool_status: Please provide pool name to get insights.')
                        return
                    else:
                        # print(f"\n\nQuery Name: {query_name}")
                        # print("-" * len(f"Query Name: {query_name}"))
                        # print(f"Query Description: {query_description}")
                        # print("-" * len(f"Query Description: {query_description}"))
                        if verbose:
                            print('QUERY: ', f"{query}")
                            print("-" * 15)

                        if query_result is None and (issue_level == 'ok' or issue_level is None):
                            msg = f'[\033[92mOK\033[0m] No running queries found for given pool name or subclutser.'
                            push_to_insights_json(qid, insights_json, msg, 'OK', query_name)
                            print(msg)
                        else:
                            if with_insights:
                                print(f"\n\nQuery Name: {query_name}")
                                print("-" * len(f"Query Name: {query_name}"))
                                if query_result_show is not None:
                                    # query_result_show = colour_values(query_result_show, threshold['query_name']['columns'], column_headers)
                                    print(tabulate(query_result_show, headers=column_headers, tablefmt='grid', floatfmt=".2f"))
                                else:
                                    print(tabulate(query_result, headers=column_headers, tablefmt='grid', floatfmt=".2f"))
                                
                            total_memory_in_use = 0
                            total_running_queries = 0
                            total_memory_borrowed = 0
                            total_memory_in_use_index = column_headers.index('memory_inuse_kb')
                            total_running_queries_index = column_headers.index('running_query_count')
                            total_memory_borrowed_index = column_headers.index('general_memory_borrowed_kb')
                            for row in query_result:
                                total_memory_in_use += row[total_memory_in_use_index]
                                total_running_queries += row[total_running_queries_index]
                                total_memory_borrowed += row[total_memory_borrowed_index]
                            
                            if total_running_queries == 0 and (issue_level == 'ok' or issue_level is None):
                                msg = f"[\033[92mOK\033[0m] No running queries found."
                                push_to_insights_json(qid, insights_json, msg, 'OK', query_name)
                                print(msg)
                            elif issue_level == 'ok':
                                msg = f"[\033[92mOK\033[0m] Having {total_running_queries} running queries with {total_memory_in_use} kb in use and borrowed {total_memory_borrowed} kb from general pool"
                                push_to_insights_json(qid, insights_json, msg, 'OK', query_name)
                                print(msg)
                            if with_insights:
                                print()
                        
                        return
                elif query_name == "long_running_queries":
                    if (query_result is None or len(query_result) == 0) and (issue_level == 'ok' or issue_level is None):
                        msg = "[\033[92mOK\033[0m] No long running queries."
                        push_to_insights_json(qid, insights_json, msg, 'OK', query_name)
                        print(msg)
                    elif len(query_result) == 1:
                        status_counts = {}
                        for _, status, cnt in query_result:
                            status_counts[status] = status_counts.get(status, 0) + cnt
                        ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
                        status_counts= {ansi_escape.sub('', key): value for key, value in status_counts.items()}
                        
                        # print(f"\n\nQuery Name: {query_name}")
                        # print("-" * len(f"Query Name: {query_name}"))
                        # print(f"Query Description: {query_description}")
                        # print("-" * len(f"Query Description: {query_description}"))
                        if verbose:
                            print('QUERY: ', f"{query}")
                            print("-" * 15)

                        if with_insights:
                            print(f"\n\nQuery Name: {query_name}")
                            print("-" * len(f"Query Name: {query_name}"))
                            if query_result_show is not None:
                                print(tabulate(query_result_show, headers=column_headers, tablefmt='grid', floatfmt=".2f"))
                            else:
                                print(tabulate(query_result, headers=column_headers, tablefmt='grid', floatfmt=".2f"))
                        
                        if "warn" not in status_counts and "fatal" not in status_counts and (issue_level is 'ok' or issue_level is None):
                            msg = "[\033[92mOK\033[0m] No long running queries."
                            push_to_insights_json(qid, insights_json, msg, 'OK', query_name)
                            print(msg)

                        for key, val in status_counts.items():
                            _, warn_threshold, fatal_threshold = get_thresholds(threshold['columns'][0]['threshold'])
                            if key == "warn":
                                r = (str('\033[93m') + str(val) + str('\033[0m'))
                                t = (str('\033[93m') + str(warn_threshold) + " mins" + str('\033[0m'))
                                msg = f"[\033[93mWARN\033[0m] {r} queries are running for more than {t} by {list(set([row[column_headers.index('user_name')] for row in query_result]))}"
                                push_to_insights_json(qid, insights_json, msg, 'WARN', query_name)
                                print(msg)
                            elif key == "fatal":
                                r = (str('\033[91m') + str(val) + str('\033[0m'))
                                t = (str('\033[91m') + str(fatal_threshold) + " mins" + str('\033[0m'))
                                msg = f"[\033[91mFATAL\033[0m] {r} queries are running for more than {t} by {list(set([row[column_headers.index('user_name')] for row in query_result]))}"
                                push_to_insights_json(qid, insights_json, msg, 'FATAL', query_name)
                                print(msg)
                        if with_insights:
                            print()
                    else:
                        status_counts = {}
                        for _, status, cnt in query_result:
                            status_counts[status] = status_counts.get(status, 0) + cnt
                        ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
                        status_counts= {ansi_escape.sub('', key): value for key, value in status_counts.items()}
                        
                        # print(f"\n\nQuery Name: {query_name}")
                        # print("-" * len(f"Query Name: {query_name}"))
                        # print(f"Query Description: {query_description}")
                        # print("-" * len(f"Query Description: {query_description}"))
                        if verbose:
                            print('QUERY: ', f"{query}")
                            print("-" * 15)

                        if with_insights:
                            print(f"\n\nQuery Name: {query_name}")
                            print("-" * len(f"Query Name: {query_name}"))
                            if query_result_show is not None:
                                print(tabulate(query_result_show, headers=column_headers, tablefmt='grid', floatfmt=".2f"))
                            else:
                                print(tabulate(query_result, headers=column_headers, tablefmt='grid', floatfmt=".2f"))
                        
                        if "warn" not in status_counts and "fatal" not in status_counts and (issue_level is 'ok' or issue_level is None):
                            msg = "[\033[92mOK\033[0m] No long running queries."
                            push_to_insights_json(qid, insights_json, msg, 'OK', query_name)
                            print(msg)

                        for key, val in status_counts.items():
                            _, warn_threshold, fatal_threshold = get_thresholds(threshold['columns'][0]['threshold'])
                            if key == "warn":
                                r = (str('\033[93m') + str(val) + str('\033[0m'))
                                t = (str('\033[93m') + str(warn_threshold) + " mins" + str('\033[0m'))
                                msg = f"[\033[93mWARN\033[0m] {r} queries are running for more than {t} by {list(set([row[column_headers.index('user_name')] for row in query_result]))}"
                                push_to_insights_json(qid, insights_json, msg, 'WARN', query_name)
                                print(msg)
                            elif key == "fatal":
                                r = (str('\033[91m') + str(val) + str('\033[0m'))
                                t = (str('\033[91m') + str(fatal_threshold) + " mins" + str('\033[0m'))
                                msg = f"[\033[91mFATAL\033[0m] {r} queries are running for more than {t} by {list(set([row[column_headers.index('user_name')] for row in query_result]))}"
                                push_to_insights_json(qid, insights_json, msg, 'FATAL', query_name)
                                print(msg)
                        if with_insights:
                            print()
                    return
                
                is_result_printed = False
                for item in threshold['columns']:
                    if item['columns_name'] == "deleted_row_cnt":
                        if query_result_show is not None:
                            query_result_show = handle_deleted_row_count(query_result, query_result_show, item, with_insights, threshold, column_headers)
                        else:
                            query_result = handle_deleted_row_count(query_result, query_result_show, item, with_insights, threshold, column_headers)
                    
                    if query_result == None or len(query_result) == 0:
                        if item['default_message'] is not "":
                            msg = item['default_message'].replace('OK', '\033[92mOK\033[0m')
                            push_to_insights_json(qid, insights_json, msg, 'OK', query_name)
                            print(msg)
                            return
                        else:
                            return
                    
                    index = column_headers.index(item['columns_name'])
                    if index == -1:
                        print(f"Error: Column '{item['columns_name']}' not found in the query result.")
                        exit()
                    
                    ok_count, warn_count, fatal_count = 0, 0, 0
                    ok_values, warn_values, fatal_values = set(), set(), set()
                    unique_values = {}
                    total = 0

                    _, warn_threshold, fatal_threshold = get_thresholds(item['threshold'])

                    if item['unique_column'] == "":
                        if item['columns_name'] == "deleted_row_cnt":
                            column_to_compare_index = column_headers.index("total_row_cnt")
                            if item['unique_column'] == "":
                                for row in query_result:
                                    if row[index] >= int(row[column_to_compare_index])*(fatal_threshold/100):
                                        fatal_count+=1
                                    elif row[index] >= int(row[column_to_compare_index])*(warn_threshold/100):
                                        warn_count+=1
                                    else:
                                        total += row[index]
                                        ok_count+=1
                        else:
                            for row in query_result:
                                if row[index] >= fatal_threshold:
                                    fatal_count+=1
                                elif row[index] >= warn_threshold:
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
                                    if row[index] >= fatal_threshold:
                                        fatal_count+=1
                                        fatal_values.add(unique_column_value)
                                    elif row[index] >= warn_threshold:
                                        warn_count+=1
                                        warn_values.add(unique_column_value)
                                    else:
                                        ok_count+=1
                                        ok_values.add(unique_column_value)
                                        total += row[index]   
                    
                    if ok_count>0 or warn_count>0 or fatal_count>0:
                        if not is_result_printed:
                            is_result_printed = True
                        
                            # print(f"\n\nQuery Name: {query_name}")
                            # print("-" * len(f"Query Name: {query_name}"))
                            # print(f"Query Description: {query_description}")
                            # print("-" * len(f"Query Description: {query_description}"))
                            if verbose:
                                print('QUERY: ', f"{query}")
                                print("-" * 15)

                            if with_insights:
                                print(f"\n\nQuery Name: {query_name}")
                                print("-" * len(f"Query Name: {query_name}"))
                                if query_result_show is not None:
                                    query_result_show = colour_values(query_result_show, threshold['columns'], column_headers)
                                    print(tabulate(query_result_show, headers=column_headers, tablefmt='grid', floatfmt=".2f"))
                                else:
                                    query_result = colour_values(query_result, threshold['columns'], column_headers)
                                    print(tabulate(query_result, headers=column_headers, tablefmt='grid', floatfmt=".2f"))
                    
                    flag = True
                    is_upper_level_statsus_printed = False

                    ok_threshold, warn_threshold, fatal_threshold = get_thresholds(item['threshold'])

                    if issue_level is None or issue_level == "ok" or issue_level == "warn" or issue_level == "fatal":
                        if fatal_threshold != -1 and fatal_count > 0 and not is_upper_level_statsus_printed:
                            flag = False
                            is_upper_level_statsus_printed = True
                            message = "[\033[91mFATAL\033[0m] "
                            message += item['message_template']['fatal'].replace('{val_cnt}', str('\033[91m') + str(fatal_threshold ) + str('\033[0m')) # '\033[91m' + + '\033[0m'
                            message = message.replace('{duration}', str(duration))
                            if len(fatal_values) > 0:
                                message = message.replace('{list}', str(fatal_values))
                                message = message.replace('{cnt}', str(len(fatal_values)))
                            else:
                                message = message.replace('{cnt}', str(fatal_count))
                            push_to_insights_json(qid, insights_json, message, 'FATAL', query_name)
                            print(message)

                    if issue_level is None or issue_level == "ok" or issue_level == "warn":
                        if warn_threshold != -1 and warn_count > 0 and not is_upper_level_statsus_printed:
                            flag = False
                            is_upper_level_statsus_printed = False

                            message = "[\033[93mWARN\033[0m] "
                            message += item['message_template']['warn'].replace('{val_cnt}', str('\033[93m') + str( warn_threshold ) + str('\033[0m')) #  + +  
                            message = message.replace('{duration}', str(duration))
                            if len(warn_values) > 0:
                                message = message.replace('{list}', str(warn_values))
                                message = message.replace('{cnt}', str(len(warn_values.union(fatal_values))))
                            else:
                                message = message.replace('{cnt}', str(warn_count))
                            push_to_insights_json(qid, insights_json, message, 'WARN', query_name)
                            print(message)

                    if issue_level is None or issue_level == "ok":
                        if (ok_threshold != -1 and ok_count > 0) and not is_upper_level_statsus_printed:
                            flag = False
                            is_upper_level_statsus_printed = True
                            # ok_count += warn_count + fatal_count
                            message = "[\033[92mOK\033[0m] "
                            
                            message += item['message_template']['ok'].replace('{val_cnt}', str(ok_threshold))
                            
                            message = message.replace('{duration}', str(duration))

                            message = message.replace('{total}', str(total))
                            if len(ok_values) > 0:
                                message = message.replace('{list}', str(ok_values))
                                message = message.replace('{cnt}', str(len(ok_values)))
                            else:
                                message = message.replace('{cnt}', str(ok_count))
                            push_to_insights_json(qid, insights_json, message, 'OK', query_name)
                            print(message)
                    
                    if flag:
                        if item['default_message'] is not "":
                            msg = item['default_message'].replace('OK', '\033[92mOK\033[0m')
                            push_to_insights_json(qid, insights_json, msg, 'OK', query_name)
                            print(msg)

                if with_insights:
                    print()


def replace_thresholds(query, query_name):
    threshold_json_file_path = THRESHOLD_FILE_PATH
    json_data = None
    with open(threshold_json_file_path) as json_file:
        json_data = json_file.read()
        thresholds = json.loads(json_data)
    
    if thresholds is None:
        print(f"Error reading {threshold_json_file_path}")
        exit()
    
    for threshold in thresholds:
        if threshold['query_name'] == query_name:
            ok_threshold, warn_threshold, fatal_threshold = get_thresholds(threshold['columns'][0]['threshold'])

            query = query.replace("{ok_threshold}", str(ok_threshold))
            query = query.replace("{warn_threshold}", str(warn_threshold))
            query = query.replace("{fatal_threshold}", str(fatal_threshold))
    
    return query


# def format_relativedelta(query_result, column_headers, column_name="running_time"):
#     index = column_headers.index(column_name)
#     for row in query_result:
#         delta = row[index]
#         print(delta)
#         row[index] = f"{delta.minutes:02}:{delta.seconds:02}.{delta.microseconds:06}"
    
#     return query_result


def format_relativedelta(query_result, column_headers, column_name="running_time"):
    index = column_headers.index(column_name)
    
    for row in query_result:
        delta = row[index]
        # print(delta)  # Debugging print statement

        # Construct the formatted time string based on available components
        formatted_parts = []
        
        if delta.days:
            formatted_parts.append(f"{delta.days}d")
        if delta.hours:
            formatted_parts.append(f"{delta.hours}h")
        if delta.minutes:
            formatted_parts.append(f"{delta.minutes}m")
        if delta.seconds or delta.microseconds:
            formatted_parts.append(f"{delta.seconds}.{delta.microseconds:06}s")

        # Join the parts together
        row[index] = "".join(formatted_parts)
    
    return query_result


def execute_queries_from_json(insights_json, json_file_path, filters, verbose, is_now, insights_only, with_insights, queries_to_execute=None):
    try:
        vertica_connection = vertica.get_vertica_connection()
        if not vertica_connection:
            print("Failed to connect to the Vertica database. Exiting.")
            return

        with open(json_file_path) as json_file:
            json_data = json_file.read()
            json_data = json.loads(json_data)

            # insights_json = {}
            for row in json_data:
                qid = row["qid"]
                query_name = row["query_name"]
                query = row["query"]
                query_description = row["query_description"]
                query_past = row.get("query_past", "")
                
                if queries_to_execute and query_name not in queries_to_execute:
                    continue

                if (queries_to_execute is None or len(queries_to_execute) == 0) and query_name == 'get_query':
                    continue

                if "get_query" in queries_to_execute and (filters['txn_id'] is None or filters['statement_id'] is None):
                    print(f"Please provide txn_id and statement_id.")
                    continue
                    
                if (queries_to_execute is None or len(queries_to_execute) == 0) and "_raw" in query_name:
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
            
                if query_name == "performance_buckets" and filters['user_name'] is None:
                    if "performance_buckets" in queries_to_execute:
                        print('Please provide a user name to use performance_buckets')
                    continue

                if not is_now and query_past == "":
                    final_query = replace_tables_in_query(final_query)

                d = {}
                for key, val in filters.items():
                    if val is not None:
                        d[key] = val
                
                if query_name == "error_messages_raw":
                    if filters["err_type"] is None:
                        final_query = get_error_messages_query()

                if "end as status" in query.lower():
                    final_query = replace_thresholds(final_query, query_name)
                
                final_query = replace_conditions(final_query, d)
                final_query = final_query.replace("<subcluster_name>", filters['subcluster_name'])

                query_result = vertica.execute_vertica_query(vertica_connection, final_query)

                if query_result == -1:
                    if verbose:
                        print('QUERY: ', f"{final_query}")
                        print("-" * 15)
                    print(query_name, ": column not found\n")
                    continue
                
                column_headers = None
                processed_query_result = None

                if query_result and query_result != -1:
                    column_headers = [desc[0] for desc in vertica_connection.cursor().description]

                if query_result and len(query_result) > 0 and (query_name == "long_running_queries_raw"):
                    query_result = format_relativedelta(query_result, column_headers)

                if query_result and len(query_result) > 0:
                    processed_query_result = process_query_result_and_highlight_text(query_result, column_headers)
                
                threshold_json_file_path = THRESHOLD_FILE_PATH
                json_data = None
                with open(threshold_json_file_path) as json_file:
                    json_data = json_file.read()
                    thresholds = json.loads(json_data)
                
                if thresholds is None:
                    print(f"Error reading {threshold_json_file_path}")
                    exit()

                if processed_query_result:
                    if insights_only or with_insights:
                        analyse(qid, insights_json, final_query, verbose, query_name, processed_query_result, query_description, column_headers, insights_only, with_insights, filters["duration"], filters["pool_name"], filters["issue_level"], is_now, filters['user_name'],filters['subcluster_name'], filters['issue_time'], vertica_connection, filters) 
                    else:
                        for threshold in thresholds:
                            if query_name == threshold['query_name'] and "_raw" not in query_name:
                                processed_query_result = colour_values(processed_query_result, threshold['columns'], column_headers)

                        print(f"\n\nQuery Name: {query_name}")
                        print("-" * len(f"Query Name: {query_name}"))
                        # print(f"Query Description: {query_description}")
                        # print("-" * len(f"Query Description: {query_description}"))
                        if verbose:
                            print('QUERY: ', f"{final_query}")
                            print("-" * 15)
                        print(tabulate(processed_query_result, headers=column_headers, tablefmt='grid', floatfmt=".2f"))
                else:
                    if not (insights_only or with_insights):
                        print(f"\n\nQuery Name: {query_name}")
                        print("-" * len(f"Query Name: {query_name}"))
                        if verbose:
                            print('QUERY: ', f"{final_query}")
                            print("-" * 15)
                        print("No records found")
                    else:
                        analyse(qid, insights_json, final_query, verbose, query_name, processed_query_result, query_description, column_headers, insights_only, with_insights, filters["duration"], filters["pool_name"], filters["issue_level"], is_now, filters['user_name'],filters['subcluster_name'], filters['issue_time'], vertica_connection, filters)
        vertica_connection.close()
    except Exception as e:
        print(f"Error while processing the CSV file or executing queries: {e}")
    
def execute_query_breakdown(args, is_now, verbose):
    query_breakdown_chars = int(args.query_breakdown_chars) if args.query_breakdown_chars is not None else args.query_breakdown_chars
    q = query_breakdown(args.client_breakdown, args.granularity, args.query_pattern, query_breakdown_chars, args.case_sensitive, int(args.num_items), float(args.duration_hours), args.issue_time, args.order_by)
    
    if not is_now:
        q = replace_tables_in_query(q, True)

    vertica_connection = vertica.get_vertica_connection()
    q_res = vertica.execute_vertica_query(vertica_connection, q)
    
    query_name = 'query_breakdown'

    if verbose:
        print('QUERY: ', f"{q}")
        print("-" * 15)

    if not q_res or len(q_res) == 0:
        print(f"\n\nQuery Name: {query_name}")
        print("-" * len(f"Query Name: {query_name}"))
        print('No records found.')
        return
    

    column_headers = [desc[0] for desc in vertica_connection.cursor().description]

    print(f"\n\nQuery Name: {query_name}")
    print("-" * len(f"Query Name: {query_name}"))
    print(tabulate(q_res, headers=column_headers, tablefmt='grid', floatfmt=".2f"))


if __name__ == "__main__":
    help_flag = False
    if len(sys.argv) == 2:
        if sys.argv[1] == "--help":
            help_flag = True
    elif len(sys.argv) == 1:
        help_flag = True

    args = get_args(help_flag)
    filters, is_now, insights_only, with_insights, json_file_path, queries_to_execute = pargse_args(help_flag)

    if len(queries_to_execute) != 0 and 'query_breakdown' in queries_to_execute:
        execute_query_breakdown(args, is_now, args.verbose)
        exit()

    insights_json = {}

    execute_queries_from_json(insights_json, json_file_path, filters, filters['verbose'], is_now, insights_only, with_insights, queries_to_execute)

    # print(insights_json)