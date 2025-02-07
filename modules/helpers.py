import re

def push_to_insights_json(insights_json, message, level, query_name):
    # remove coloring
    ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
    message = ansi_escape.sub('', message)
    
    # remove status in message
    message = message.replace('[OK] ', '') 
    message = message.replace('[WARN] ', '') 
    message = message.replace('[FATAL] ', '') 

    insights_json[query_name] = insights_json.get('query_name', [])

    if query_name == 'delete_vectors':
        print()
        print(insights_json[query_name])
        print()
    

    insights_json[query_name].append({
        "message": message,
        "status": level
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