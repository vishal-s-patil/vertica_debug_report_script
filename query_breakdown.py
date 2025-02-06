from modules.helpers import replace_conditions

# client_breakdown=True # default false
# granularity='hour' # default None
# query_pattern = '%select%' # default None
# query_breakdown_chars=None # default None
# case_sensitive=True # default False
# num_items = 5
# duration = 3
# issue_time = None

def query_breakdown(client_breakdown, granularity, query_pattern, query_breakdown_chars, case_sensitive, num_items, duration, issue_time):

    granularity_dimension = """date_trunc('{duration}', query_start::timestamp)"""

    client_breakdown_dimension = "case when regexp_like(query, 'cid\\s*=\\s*\\d+') = true then regexp_substr(replace(query, ' ', ''), 'cid\\s*=\\s*(\\d+)', 1, 1, '', 1) else regexp_substr(query, 's\\_(\\d\\d+)\\.', 1, 1, '', 1) end as cid"

    query_dimension = """left(query, {query_breakdown_chars})"""

    body = """SELECT {dimension_replacements} FROM query_profiles WHERE 1=1 { user_name = 'user_name' } and query_start::timestamp >= ( TIMESTAMP { 'issue_time' } - INTERVAL '{duration} hour' ) and query_start::timestamp <= TIMESTAMP { 'issue_time' } {query ILIKE 'query_pattern'} group by {groupby_replacements} order by {order_by} 1 limit {num_items};"""

    d = {
        "issue_time": issue_time, 
        "num_items": num_items,
        "duration": duration
    }

    if case_sensitive:
        body = body.replace("{query ILIKE 'query_pattern'}", "{query LIKE 'query_pattern'}")

    if query_pattern is not None:
        d["query_pattern"] = query_pattern

    if granularity and not client_breakdown and not query_breakdown_chars:
        # only granularity
        granularity_dimension = granularity_dimension.replace('{duration}', f"{granularity}")
        d = {
            "dimension_replacements": granularity_dimension,
            "groupby_replacements": "1"
        }

        q = replace_conditions(body, d)
        return q

    elif not granularity and client_breakdown and not query_breakdown_chars:
        # only client_breakdown
        d = {
            "dimension_replacements": client_breakdown_dimension,
            "groupby_replacements": "1"
        }

        q = replace_conditions(body, d)
        return q

    elif granularity and client_breakdown and not query_breakdown_chars:
        # granularity and client_breakdown
        granularity_dimension = granularity_dimension.replace('{duration}', f"{granularity}")
        
        d = {
            "dimension_replacements": granularity_dimension + ', ' + client_breakdown_dimension,
            "groupby_replacements": "1, 2"
        }

        q = replace_conditions(body, d)
        return q

    elif granularity and client_breakdown and query_breakdown_chars:
        # granularity and client_breakdown and query_breakdown_chars
        granularity_dimension = granularity_dimension.replace('{duration}', f"{granularity}")
        query_dimension = query_dimension.replace('{query_breakdown_chars}', f"{query_breakdown_chars}")
        d = {
            "dimension_replacements": granularity_dimension + ', ' + client_breakdown_dimension + ', ' + query_dimension,
            "groupby_replacements": "1, 2, 3"
        }

        q = replace_conditions(body, d)
        return q

    else:
        granularity='hour' 
        query_breakdown_chars=20
        granularity_dimension = granularity_dimension.replace('{duration}', f"{granularity}")
        query_dimension = query_dimension.replace('{query_breakdown_chars}', f"{query_breakdown_chars}")

        d = {
            "dimension_replacements": granularity_dimension + ", " + client_breakdown_dimension + ", " + query_dimension,
            "groupby_replacements": "1, 2, 3"
        }

        print('d', d)
        print()
        q = replace_conditions(body, d)
        print()
        print()
        print('query after replacong', q)
        print()
        print()
        return q

if __name__ == "__main__":
    pass