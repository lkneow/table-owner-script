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
    find_tables_in_sql,
    format_as_custom_json
)
from constants import UPSTREAM_DIR

Path.mkdir(UPSTREAM_DIR, exist_ok=True)

def get_args():
    # Parser to take in inputs from the user
    parser = argparse.ArgumentParser(description="Upstream search. Take in path to github repo and start node.")
    # Common options needed by the user
    parser.add_argument("--path", "-p", required=True, help="path from home to repo")
    parser.add_argument("--start", "-s", required=True, help="start model name")
    parser.add_argument("--max_depth", "-m", type=int, default=2, help="max depth for upstream search. Default is 100 to cover all upstream")
        
    args = parser.parse_args()

    return args

def find_upstream_tables(folder_path: str, table_name: str, max_depth: int) -> Dict[str, List[str]]:
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
            if table in file_path:

                tables_found = find_tables_in_sql(file_path)
                print(tables_found)

                for found_table in tables_found:
                    if found_table not in parsed_tables and all(found_table != t[0] for t in tables_to_parse):
                        tables_to_parse.append((found_table, current_depth + 1))

                    table_file_path_sql = [x for x in some_files if found_table + ".sql" == x.split("/")[-1]] # check if table_name.sql exists in reporting layer
                    
                    if len(table_file_path_sql) != 0:
                        owner = get_owner(table_file_path_sql[0])
                        
                        if owner:
                            if owner not in owners_tables:
                                owners_tables[owner] = []
                            
                            if found_table not in owners_tables[owner]:
                                owners_tables[owner].append(found_table)
                    else:

                        owner = "curated_table"
                        
                        if owner:
                            if owner not in owners_tables:
                                owners_tables[owner] = []
                            
                            if found_table not in owners_tables[owner]:
                                owners_tables[owner].append(found_table)
                
    return {k: sorted(v) for k, v in owners_tables.items()}

get_args = get_args()
folder_path = get_args.path

output = find_upstream_tables(folder_path=folder_path, table_name=get_args.start, max_depth=get_args.max_depth)

# Export results into `upstream_jsons` directory
table_name = get_args.start
max_depth = get_args.max_depth
current_datetime = str(datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%S"))

json_file_name = (
    str(UPSTREAM_DIR)
    + "/upstream_"
    + table_name
    + "_"
    + f"depth{max_depth}_"
    + current_datetime
)

upstream_json_file_name = json_file_name + ".json"
custom_upstream_json_file_name = json_file_name + "_custom.json"

# Save the output into json file
print("\n" + "#"*20 + "Printing json format")
pprint.pprint(output)
save_to_json(output, upstream_json_file_name)

# Save the custom formatted string to a file
print("\n" + "#"*20 + "Printing custom json format")
print(format_as_custom_json(output))
save_custom_json_format(output, custom_upstream_json_file_name)