from flask import Flask, request, jsonify
from modules.args_parser import get_args, pargse_args
from vertica_debug_report import execute_queries_from_json

app = Flask(__name__)

@app.route('/globalrefresh', methods=['GET'])
def greet():
    
    subcluster_name = request.args.get('subcluster_name')
    query_file_path = "queries.json"

    filters, is_now, insights_only, with_insights, json_file_path, queries_to_execute = pargse_args(query_file_path, subcluster_name, True)

    insights_json = {}

    execute_queries_from_json(insights_json, json_file_path, filters, filters['verbose'], is_now, insights_only, with_insights, queries_to_execute)
    
    return jsonify(insights_json)


app.run(host='0.0.0.0', port=5500)