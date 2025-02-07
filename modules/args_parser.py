import argparse
import tabulate
import sys

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

def get_args():
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
        help="Comma-separated list of query names to execute. If empty, all queries will be executed.")

    parser.add_argument("--pool-name", required=False, default=None, 
        help="Filter condition for queries with the 'pool_name' placeholder.")
    
    parser.add_argument("--user-name", required=False, default=None, 
        help="Filter condition for queries with the 'user_name' placeholder.")

    parser.add_argument("--table-name", required=False, default=None, 
        help="Filter condition for queries with the 'table_name' placeholder. Supports LIKE/ILIKE with % at the start, end, or both.")

    parser.add_argument("--verbose", required=False, action="store_true", 
        help="Enable verbose mode to display executed queries.")
    
    parser.add_argument("--duration-hours", required=False, default=3,
        help="Number of hours to look past from issue time.")
    
    parser.add_argument("--issue-time", required=False, 
        help="Get the result at a particular time duration.", default=None)
    
    parser.add_argument("--num-items", required=False, 
        help="Number of rows display.", default=5)
    
    parser.add_argument("--type", required=False, 
        help="", default=None)
    
    parser.add_argument("--granularity", required=False, 
        help="Truncate datetime by [hour|min|day]", default=None)
    
    parser.add_argument("--order-by", required=False, 
        help="To order by the result with specified order by columns.", default=None)
    
    parser.add_argument("--snapshots", required=False, 
        help="Number of snapshots to display", default=5)
    
    parser.add_argument("--issue-level", required=False, 
        help="To see result of a particular issue level [ok|warn|fatal]", default=None)
    
    parser.add_argument("--user-limit", required=False, 
        help="Number of user to display", default=5)

    parser.add_argument("--insights-only", required=False, action="store_true", 
        help="To see the insights without result table.")

    parser.add_argument("--with-insights", required=False, action="store_true", 
        help="To see the insights along with the result table.")
    
    parser.add_argument("--schema-name", required=False, default=None, 
        help="")
    
    parser.add_argument("--projection-name", required=False, default=None, 
        help="")
    
    parser.add_argument("--txn-id", required=False, default=None, 
        help="")
    
    parser.add_argument("--statement-id", required=False, default=None, 
        help="")
    
    parser.add_argument("--client-breakdown", required=False, action="store_true", 
        help="")
    
    parser.add_argument("--query-pattern", required=False, default=None, 
        help="")
    
    parser.add_argument("--query-breakdown-chars", required=False, default=None, 
        help="")
    
    parser.add_argument("--case-sensitive", required=False, default=False, 
        help="")

    if help_flag:
        parser.print_help()
        exit(0)
    
    return parser.parse_args()