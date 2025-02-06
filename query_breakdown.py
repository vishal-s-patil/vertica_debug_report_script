import re

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

granularity_dimension = """date_trunc('{duration}', query_start)"""

client_breakdown_dimension = "case when regexp_like(query, 'cid\\s*=\\s*\\d+') = true then regexp_substr(replace(query, ' ', ''), 'cid\\s*=\\s*(\\d+)', 1, 1, '', 1) else regexp_substr(query, 's\\_(\\d\\d+)\\.', 1, 1, '', 1) end as cid"

query_dimension = """left(query, {query_breakdown_chars})"""

body = """
SELECT {dimension_replacements}
FROM query_profiles
WHERE 1=1 { user_name = 'user_name' } and query_start >= ( TIMESTAMP { 'issue_time' } - INTERVAL '{duration} hour' ) and query_start <= TIMESTAMP { 'issue_time' } {query ILIKE 'query_pattern'} group by {groupby_replacements} order by {order_by} 1 limit {num_items};
"""

client_breakdown=True
granularity='hour' # None
query_pattern = '%select%' # None
query_breakdown_chars=20
case_sensitive=True # False

if granularity is not None:
    if granularity == 'hour' or granularity == 'min' or granularity == 'day' or granularity == 'sec':
        pass
    else:
        raise ValueError("Invalid granularity. Must be 'hour', 'min', 'day', or'sec'.")

if granularity is not None and client_breakdown and query_pattern is not None:
    granularity_dimension = granularity_dimension.replace('{duration}', f"{granularity}")
    query_dimension = query_dimension.replace('{query_breakdown_chars}', f"{query_breakdown_chars}")
    d = {
        "dimension_replacements": granularity_dimension + ", " + client_breakdown_dimension + ", " + query_dimension,
        "groupby_replacements": "1, 2, 3",
        "query_pattern": query_pattern 
    }

    q = replace_conditions(body, d)
    print(q)