import re
from datetime import datetime, timedelta

def get_past_datetime(issue_time, duration):
    issue_time_dt = datetime.strptime(issue_time, "%Y-%m-%d %H:%M:%S")
    return str(issue_time_dt - timedelta(hours=duration))


def process_query_result_and_highlight_text(query_result, column_headers):
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


def replace_tables_in_query(query, force=False):
    replacements = [
        ("from sessions", "from netstats.sessions_full"),
        ("from resource_queues", "from netstats.resource_queues_full"),
        ("from error_messages", "from netstats.error_messages"),
        ("from resource_pool_status", "from netstats.resource_pool_status"),
        ("from query_profiles", "from netstats.query_profiles"),
        ("from storage_containers", "from netstats.storage_containers")
    ]

    query = query.lower()
    
    try:
        for old, new in replacements:
            if force or "from_date_time" in query or "to_date_time" in query or "issue_time" in query:
                query = query.replace(old, new)
        return query
    except Exception as e:
        print(f"Error while replacing strings in query: {e}")
        return query

def push_to_insights_json(qid, insights_json, message, level, query_name):
    # remove coloring
    ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
    message = ansi_escape.sub('', message)
    
    colour = 'green' if 'OK' in message else 'red' if 'FATAL' in message else 'yellow'

    # remove status in message
    message = message.replace('[OK] ', '') 
    message = message.replace('[WARN] ', '') 
    message = message.replace('[FATAL] ', '') 

    insights_json[query_name] = insights_json.get(query_name, {})
    insights_json[query_name]['insights'] = insights_json[query_name].get('insights', [])

    insights_json[query_name]['insights'].append({
        "message": message,
        "status": level,
        'order': qid,
        'colour': colour,
        'display': True
    })

    return insights_json

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
            # for part in re.split(r'([<>!=]=?|[><]=?|(?i)\b(?:ILIKE|LIKE|IS\s+NOT|IS)\b)', match, 1)
            for part in re.split(r'(?i)([<>!=]=?|[><]=?|\b(?:ILIKE|LIKE|IS\s+NOT|IS)\b)', match, 1)
        ]
        #
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

                if isinstance(value, int)  or isinstance(value, float) or placeholder=="err_type" or placeholder=="order_by" or placeholder=='session_type' or placeholder=='dimension_replacements' or placeholder=='groupby_replacements':
                    new_condition = f"{value}"
                else:
                    new_condition = f"'{value}'"

                query = query.replace(f"{{{match}}}", new_condition)

    return re.sub(r'\{[^}]*\}', '', query).strip()