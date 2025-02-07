from flask import Flask, request, jsonify
from modules.args_parser import get_args, pargse_args
from vertica_debug_report import execute_queries_from_json
from modules.redis import connect_to_redis, put_value, get_value
from datetime import datetime, timedelta
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/globalrefresh', methods=['GET'])
def greet():

    subcluster_name = request.args.get('subcluster_name')
    query_name = request.args.get('query_name', '')
    query_file_path = "queries.json"

    print('query_name', query_name)

    if query_name != '':
        filters, is_now, insights_only, with_insights, json_file_path, queries_to_execute = pargse_args(query_file_path, subcluster_name, True, query_name)
    else:
        filters, is_now, insights_only, with_insights, json_file_path, queries_to_execute = pargse_args(query_file_path, subcluster_name, True)

    insights_json = {}

    execute_queries_from_json(insights_json, json_file_path, filters, filters['verbose'], is_now, insights_only, with_insights, queries_to_execute)
    
    # hardcoded_insights_json = {"delete_vectors":[{"message":"No outliers in delete vector count","status":"OK"}],"error_messages":[{"message":"No Errors Found","status":"OK"}],"long_running_queries":[{"message":"No long running queries.","status":"OK"}],"query_count":[{"message":"total 4229 queries by 4 users in past 3.0 hours","status":"OK"}],"resource_queues":[{"message":"No Queries in Queue","status":"OK"}],"sessions":[{"message":"No Active Queries","status":"OK"}]}

    return insights_json

    '''
    redis_client = connect_to_redis()
    
    res_insights_json = get_value(redis_client, 'test')

    last_updated = datetime.strptime(res_insights_json['last_updated'], "%Y-%m-%d %H:%M:%S.%f")

    if datetime.now() - last_updated > timedelta(seconds=5):
        hardcoded_insights_json['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        # query here 
        put_value(redis_client, 'test', hardcoded_insights_json)
        return jsonify(hardcoded_insights_json)
    else:
        res_insights_json = get_value(redis_client, 'test')
        return res_insights_json
    '''
    
app.run(host='0.0.0.0', port=5000)