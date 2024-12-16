import pprint
import argparse
import datetime as datetime
from pathlib import Path
from typing import Dict, List
from utils import (
    get_file_paths,
    get_owner,
    save_to_json,
    save_custom_json_format,
    table_exists_in_file,
    format_as_custom_json
)
from constants import DOWNSTREAM_DIR

Path.mkdir(DOWNSTREAM_DIR, exist_ok=True)

def get_args():
    # Parser to take in inputs from the user
    parser = argparse.ArgumentParser(description="Downstream search. Take in path to github repo and start node.")
    # Common options needed by the user
    parser.add_argument("--path", "-p", required=True, help="path from home to repo")
    parser.add_argument("--start", "-s", required=True, help="start model name")
    parser.add_argument("--max_depth", "-m", type=int, default=2, help="max depth for downstream search. Default is 100 to cover all downstream")
        
    args = parser.parse_args()

    return args

def get_all_downstream(folder_path: str, table_name: str, max_depth: int) -> Dict[str, List[str]]:
    print(f"Parsing till max depth: {max_depth}")
    some_files = get_file_paths(folder_path)
    
    owners_tables = {}
    
    tables_to_parse = [(table_name, 0)]
    parsed_tables = set()
    
    while tables_to_parse:
        table, current_depth = tables_to_parse.pop(0)
        parsed_tables.add(table)
        print(f"parsing: {table}")
        
        if current_depth >= max_depth:
            continue
        
        for file_path in some_files:
            if table_exists_in_file(file_path, table):

                downstream_table_name = file_path.split("/")[-1].replace(".sql", "")
                print(f"downstream_table: {downstream_table_name}")
                
                if downstream_table_name not in parsed_tables and all(downstream_table_name != t[0] for t in tables_to_parse):
                    tables_to_parse.append((downstream_table_name, current_depth + 1))
                
                owner = get_owner(file_path)

                if owner:
                    if owner not in owners_tables:
                        owners_tables[owner] = []
                    
                    if downstream_table_name not in owners_tables[owner]:
                        owners_tables[owner].append(downstream_table_name)
                            
    return {k: sorted(v) for k, v in owners_tables.items()}

get_args = get_args()
folder_path = get_args.path

output = get_all_downstream(folder_path=folder_path, table_name=get_args.start, max_depth=get_args.max_depth)

# Export results into `downstream_jsons` directory
table_name = get_args.start
max_depth = get_args.max_depth
current_datetime = str(datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%S"))

json_file_name = (
    str(DOWNSTREAM_DIR)
    + "/downstream_"
    + table_name
    + "_"
    + f"depth{max_depth}_"
    + current_datetime
)

downstream_json_file_name = json_file_name + ".json"
custom_downstream_json_file_name = json_file_name + "_custom.json"

# Save the output into json file
print("\n" + "#"*20 + "Printing json format")
pprint.pprint(output)
save_to_json(output, downstream_json_file_name)

# Save the custom formatted string to a file
print("\n" + "#"*20 + "Printing custom json format")
print(format_as_custom_json(output))
save_custom_json_format(output, custom_downstream_json_file_name)