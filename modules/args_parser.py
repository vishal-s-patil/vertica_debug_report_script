import argparse
import tabulate
import sys
from datetime import datetime

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

def get_args(help_flag):
    parser = MyArgumentParser(description="Args")
    # parser = argparse.ArgumentParser(description="Args")
    # parser.add_argument("--help", required=False, action="store_true", help="show all command line args with description")
        
    parser.add_argument("--subcluster-name", required=False if help_flag else False, 
        help="Subcluster name.")

    parser.add_argument("--inputfilepath", required=False if help_flag else False, 
        help="Path of input json file.", default="queries.json")

    parser.add_argument("--queries-to-execute", required=False, nargs="*", default=[], 
        help="Comma-separated list of query names to execute. If empty, all queries will be executed.")

    parser.add_argument("--pool-name", required=False, default=None, 
        help="Pool name")
    
    parser.add_argument("--user-name", required=False, default=None, 
        help="User name")

    parser.add_argument("--table-name", required=False, default=None, 
        help="Table name. wild card '%' can be used.")

    parser.add_argument("--verbose", required=False, action="store_true", 
        help="Enable verbose mode to display executed queries.")
    
    parser.add_argument("--duration-hours", required=False, default=3,
        help="Number of hours to look past from issue time.")
    
    parser.add_argument("--issue-time", required=False, 
        help="Execute queries from the issue time to the past 3 hours.", default=None)
    
    parser.add_argument("--num-items", required=False, 
        help="Number of rows to display.", default=5)
    
    parser.add_argument("--type", required=False, 
        help="", default=None)
    
    parser.add_argument("--granularity", required=False, 
        help="Truncate datetime by [hour|min|day]", default=None)
    
    parser.add_argument("--order-by", required=False, 
        help="To order the result with specified columns.", default=None)
    
    parser.add_argument("--snapshots", required=False, 
        help="Number of snapshots to display.", default=5)
    
    parser.add_argument("--issue-level", required=False, 
        help="To see result only with specified issue level [ok|warn|fatal]", default=None)
    
    parser.add_argument("--user-limit", required=False, 
        help="Number of user to display", default=5)

    parser.add_argument("--insights-only", required=False, action="store_true", 
        help="To see the insights without tables.")

    parser.add_argument("--with-insights", required=False, action="store_true", 
        help="To see the insights along with the result tables.")
    
    parser.add_argument("--schema-name", required=False, default=None, 
        help="Schema name")
    
    parser.add_argument("--projection-name", required=False, default=None, 
        help="Projection name. Wild card '%' is supported.")
    
    parser.add_argument("--txn-id", required=False, default=None, 
        help="Transaction ID")
    
    parser.add_argument("--statement-id", required=False, default=None, 
        help="Statement ID")
    
    parser.add_argument("--client-breakdown", required=False, action="store_true", 
        help="Client level breakdown.")
    
    parser.add_argument("--query-pattern", required=False, default=None, 
        help="Query pattern. Wild card '%' is supported.")
    
    parser.add_argument("--query-breakdown-chars", required=False, default=None, 
        help="Number of characters in query to display.")
    
    parser.add_argument("--case-sensitive", required=False, default=False, 
        help="If set, --query-pattern passed will be case sensitive while matching in the query.")

    if help_flag:
        parser.print_help()
        exit(0)
    
    return parser.parse_args()


def pargse_args(query_file_path=None, subcluster_name=None, insights_only=False, queries_to_execute=None):
    args = get_args()

    queries_to_execute = args.queries_to_execute if len(args.queries_to_execute) != 0 else [queries_to_execute] if queries_to_execute is not None else []
    if len(queries_to_execute) != 0:
        queries_to_execute = (queries_to_execute[0]).split(',')

    is_now = False
    if args.issue_time is None:
        is_now = True
        args.issue_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    json_file_path = args.inputfilepath if args.inputfilepath is not None else query_file_path
    type = args.type

    if type is not None:
        if queries_to_execute is not None and len(queries_to_execute) > 1:
            print(f"Multiple queries not allowed when --type is passed.")
            exit()

    pool_name = args.pool_name
    user_name = args.user_name

    if pool_name is None and user_name is not None:
        pool_name = user_name + "_pool"

    session_type_placeholder = "is not"
    session_type_placeholder_2 = None
    err_type = None

    query_name = None
    if type is not None:
        if len(queries_to_execute) != 0:
            query_name = queries_to_execute[0]

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
        "subcluster_name": args.subcluster_name if args.subcluster_name is not None else subcluster_name,
        "pool_name": pool_name,
        "user_name": user_name,
        "table_name": args.table_name,
        "issue_time": args.issue_time,
        "user_name": args.user_name,
        "duration": float(args.duration_hours),
        "num_items": int(args.num_items),
        "err_type": err_type,
        "granularity": 'hour' if args.granularity is None else args.granularity,
        "order_by": args.order_by,
        "snapshots": int(args.snapshots),
        "user_limit": int(args.user_limit),
        "issue_level": args.issue_level,
        "session_type": session_type_placeholder,
        "session_type_2": session_type_placeholder_2,
        "schema_name": args.schema_name,
        "projection_name": args.projection_name,
        "txn_id": args.txn_id,
        "statement_id": args.statement_id,
        "verbose": args.verbose
    }

    if filters['projection_name'] is None and filters['table_name'] is not None:
        filters['projection_name'] = filters['table_name'] + '_%'

    if filters['projection_name'] is not None and filters['schema_name'] is None:
        print("please provide schema name aswell.")
        exit()

    insights_only = args.insights_only if args.insights_only else insights_only
    with_insights = args.with_insights

    if (insights_only or with_insights) and query_name=="long_running_queries_raw":
        print("Use long_running_queries instead of long_running_queries_raw for insights.")
        exit()

    if (insights_only or with_insights) and query_name=="error_messages_raw":
        print("Use error_messages instead of error_messages_raw for insights.")
        exit()
    
    if filters['order_by'] is not None:
        filters['order_by'] = filters['order_by'] + ','

    return filters, is_now, insights_only, with_insights, json_file_path, queries_to_execute