from flask import Flask, request, jsonify, render_template, request
from modules.args_parser import get_args, pargse_args
from vertica_debug_report import execute_queries_from_json
from modules.redis import connect_to_redis, put_value, get_value
from datetime import datetime, timedelta
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)


BASE_API_URL = "http://localhost:5500/globalrefresh"

def fetch_data(query_name=None):
    try:
        url = f"{BASE_API_URL}?subcluster_name=secondary_subcluster_1"
        if query_name:
            url += f"&query_name={query_name}"

        response = requests.get(url)
        response.raise_for_status()
        print(response)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(e)
        return {"error": str(e)}

@app.route("/")
def index():
    data = fetch_data()
    return render_template("dashboard.html", data=data)

@app.route("/refresh", methods=["GET"])
def refresh_query():
    query_name = request.args.get("query_name")
    if not query_name:
        return jsonify({"error": "Missing query_name"}), 400

    data = fetch_data(query_name=query_name)
    return render_template("dashboard.html", data=data)

# if __name__ == "__main__":
#     app.run(debug=True)

@app.route('/globalrefresh', methods=['GET'])
def greet():

    subcluster_name = request.args.get('subcluster_name')
    query_name = request.args.get('query_name', '')
    query_file_path = "queries.json"

    if query_name != '':
        filters, is_now, insights_only, with_insights, json_file_path, queries_to_execute = pargse_args(query_file_path, subcluster_name, True, query_name)
    else:
        filters, is_now, insights_only, with_insights, json_file_path, queries_to_execute = pargse_args(query_file_path, subcluster_name, True)
    
    insights_json = {}

    execute_queries_from_json(insights_json, json_file_path, filters, filters['verbose'], is_now, insights_only, with_insights, queries_to_execute)
    
    # hardcoded_insights_json = { "delete_vectors": { "insights": [ { "colour": "red", "display":True, "message": "4420 projections having > 1% of their total rows as deleted rows", "order": 10, "status": "FATAL", "last_updated": "2025-02-07 18:43:12.123" }, { "colour": "green", "display":True, "message": "No outliers in delete vector count", "order": 10, "status": "OK", "last_updated": "2025-02-07 18:43:12.123" } ] }, "error_messages": { "insights": [ { "colour": "green", "display":True, "message": "No Errors Found", "order": 4, "status": "OK", "last_updated": "2025-02-07 18:43:12.123" } ] }, "long_running_queries": { "insights": [ { "colour": "green", "display":True, "message": "No long running queries.", "order": 1, "status": "OK", "last_updated": "2025-02-07 18:43:12.123" } ] }, "query_count": { "insights": [ { "colour": "yellow", "display":True, "message": "> 2000 queries by users {'contact_summary_ds', 'upload'} in past 3.0 hours", "order": 8, "status": "WARN", "last_updated": "2025-02-07 18:43:12.123" }, { "colour": "green", "display":True, "message": "total 6305 queries by 4 users in past 3.0 hours", "order": 8, "status": "OK", "last_updated": "2025-02-07 18:43:12.123" } ] }, "resource_queues": { "insights": [ { "colour": "green", "display":True, "message": "No Queries in Queue", "order": 6, "status": "OK", "last_updated": "2025-02-07 18:43:12.123" } ] }, "sessions": { "insights": [ { "colour": "green", "display":True, "message": "No Active Queries", "order": 3, "status": "OK", "last_updated": "2025-02-07 18:43:12.123" } ] }, "last_updated": "2025-02-07 18:43:12.123" }

    return insights_json
    # hardcoded_insights_json = {"long_running_queries":{"insights":[{"colour":"sajbfjsdbfjabxshdrcisue","display":True,"message":"No long wjhgxfn running queries.","order":1,"status":"OK"}]}}

    # hardcoded_insights_json = {"delete_vectors":{"insights":[{"colour":"rvdshjhdbhjsdvhjed","display":True,"message":"abcd","order":10,"status":"FATAL", "last_updated": "2025-02-07 18:56:00.123"},{"colour":"hjbhbdshbhbsdhjbsdhj","display":True,"message":"abcd","order":10,"status":"OK", "last_updated": "2025-02-07 19:56:00.123"}]}}

    # hardcoded_insights_json = {"delete_vectors":{"insights":[{"colour":"rvdshjhdbhjsdvhjed","display":True,"message":"fegh","order":10,"status":"FATAL", "last_updated": "2025-02-07 18:49:00.123"},{"colour":"hjbhbdshbhbsdhjbsdhj","display":True,"message":"fegh","order":10,"status":"OK", "last_updated": "2025-02-07 18:49:00.123"}]}}

    redis_client = connect_to_redis()
    
    res_insights_json = get_value(redis_client, 'test')

    last_updated = datetime.strptime(res_insights_json['last_updated'], "%Y-%m-%d %H:%M:%S.%f")

    if query_name != '':
        # handle if redis is completely empty (query for all push new json, will not in ui as it does it when page us loaded)
        # query here
        if datetime.now() - datetime.strptime(res_insights_json[query_name]['insights'][0]['last_updated'], "%Y-%m-%d %H:%M:%S.%f") > timedelta(seconds=15):
            res_insights_json[query_name] = hardcoded_insights_json[query_name]
            for item in res_insights_json[query_name]['insights']:
                item['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            put_value(redis_client, 'test', res_insights_json)
            return jsonify(res_insights_json)
        else:
            return jsonify(res_insights_json)
    else:
        if datetime.now() - last_updated > timedelta(seconds=5):
            hardcoded_insights_json['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            for q_name, query_details in hardcoded_insights_json.items():
                if q_name != 'last_updated':
                    for item in query_details['insights']:
                        item['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            # query here 
            put_value(redis_client, 'test', hardcoded_insights_json)
            return jsonify(hardcoded_insights_json)
        else:
            res_insights_json = get_value(redis_client, 'test')
            return jsonify(res_insights_json)
    
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5500, debug=True)