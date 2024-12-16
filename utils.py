import glob
import os
import re
import json
from typing import Union, List, Dict
from constants import UPSTREAM_PATTERNS


def get_file_paths(
    folder_path: Union[str, os.PathLike],
) -> List[Union[str, os.PathLike]]:
    parent = os.path.abspath(folder_path) + "/**/*.sql"
    all_files = glob.glob(parent, recursive=True)
    some_files = [  # excluding those compiled files from these folders
        x
        for x in all_files
        if "/dbt_logs" not in x
        and "/dbt_packages" not in x
        and "/dbt_target" not in x
        and "/target" not in x
    ]

    return some_files


def get_owner_from_file_path(file_path: Union[str, os.PathLike]) -> str:
    pattern = r"report\/(.*?\/models\/[^\/]+\/)"
    match = re.search(pattern, file_path)
    return match.group(1) if match else "Unsure of owner"


def get_owner_from_yml(yml_file_path: Union[str, os.PathLike]) -> str:
    """
    Reads each yml/yaml file line by line
    
    first looks for user_email, then slack_group
    
    usually in the yml/yaml slack_group is above user_email
    """
    with open(yml_file_path, "r") as f:
        lines = f.readlines()
        owner = None
        for line in lines:
            if "user_email" in line:
                owner = line.replace("user_email: ", "").strip()
                break
            elif "slack_group" in line:
                owner = line.replace("slack_group: ", "").strip()
                break
    if owner:
        match = re.search(r"^([\w\.]+)@[\w]+\.[\w]{2,3}(\.[\w]{2,3})?", owner)
        if match:
            owner = match.group(1)
    return "@" + owner


def get_owner(file_path: Union[str, os.PathLike]) -> str:
    """
    Somehow there are .yaml files instead of all being .yml. Havent found a good way to check both
    Also some teams don't have individual yml files for each sql file. Not sure how to parse those,
    decided to just take the folder if the individual yml file doesn't exist.

    For example:

    ~/datahub-airflow/dags/pandata/transform/report/apac/models/pandata_analytics_foundation/datamart/table.sql

    takes apac/models/pandata_analytics_foundation
    """
    yml_file_path = file_path.replace(".sql", ".yml")
    yaml_file_path = file_path.replace(".sql", ".yaml")

    if os.path.isfile(yml_file_path):
        return get_owner_from_yml(yml_file_path)
    elif os.path.isfile(yaml_file_path):
        return get_owner_from_yml(yaml_file_path)
    else:
        return get_owner_from_file_path(file_path)

def table_exists_in_file(
    file_path: Union[str, os.PathLike], search_string: str
) -> bool:
    """
    A helper function to make sure we're looking for tables in the following lines containing
    {{ project_id() }}.pandata_*.table
    {{ ref('table') }}
    {{ source('*', 'table') }}
    """
    pattern1 = re.compile(
        r"\{\{ project_id\(\) \}\}\.pandata_[^\.\'\"]+\." + re.escape(search_string)
    )
    pattern2 = re.compile(
        r"\{\{\s*ref\([\'\"]" + re.escape(search_string) + r"[\'\"]\)\s*\}\}"
    )
    pattern3 = re.compile(
        r"\{\{\s*source\([\'\"]([^']+)[\'\"],\s*[\'\"]"
        + re.escape(search_string)
        + r"[\'\"]\)\s*\}\}"
    )

    with open(file_path, "r") as file:
        for line in file:
            if pattern1.search(line) or pattern2.search(line) or pattern3.search(line):
                return True

    return False


def find_tables_in_sql(file_path: Union[str, os.PathLike]) -> List[str]:
    tables_found = set()
    with open(file_path, "r") as f:
        for line in f:
            for pattern, group in UPSTREAM_PATTERNS:
                match = pattern.search(line)
                if match:
                    table_name = match.group(group)
                    tables_found.add(table_name)
                    break  # Exit the loop once a match is found
    return list(tables_found)


def column_exists_in_file(file_path):
    pattern1 = "is_own_delivery"
    # pattern2 = "is_failed_order_vendor"
    # pattern3 = "is_failed_order_foodpanda"
    with open(file_path, "r") as file:
        for line in file:
            if pattern1 in line:  # or pattern2 in line or pattern3 in line:
                return True
    return False


def collect_innermost_values(data):
    values_set = set()

    def traverse(data):
        if isinstance(data, dict):
            for key, value in data.items():
                traverse(value)
        elif isinstance(data, list):
            for item in data:
                values_set.add(item)

    traverse(data)
    return values_set


def sort_innermost_lists(data):
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = sort_innermost_lists(value)
    elif isinstance(data, list):
        data = sorted(data)
    return data

def format_as_custom_json(owners_tables: Dict[str, List[str]], indent: int = 4) -> str:
    """
    Almost like json but without quotes. To facilitate copy pasting onto slack
    """

    def format_dict(d, level=0):
        indent_str = " " * (level * indent)
        lines = []
        for key, value in d.items():
            if isinstance(value, dict):
                value_str = format_dict(value, level + 1)
            elif isinstance(value, list):
                value_str = (
                    "[\n"
                    + ",\n".join(f"{indent_str}{' ' * indent}{item}" for item in value)
                    + f"\n{indent_str}]"
                )
            else:
                value_str = str(value)
            lines.append(f"{indent_str}{key}: {value_str}")
        return "\n".join(lines)

    return format_dict(owners_tables)


def format_dict_without_quotes(data, indent=0): # not needed
    formatted_str = ""
    indent_str = " " * indent

    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list):
                formatted_str += f"{indent_str}{key}:"
            else:
                formatted_str += f"{indent_str}{key}: \n"
            formatted_str += format_dict_without_quotes(value, indent + 1)
            formatted_str += f"{indent_str}\n"
    elif isinstance(data, list):
        formatted_str += f"{indent_str}[\n"
        for item in data:
            formatted_str += f"{indent_str}  {item},\n"
        formatted_str += f"{indent_str}]"
    else:
        formatted_str += f"{indent_str}{data}\n"

    return formatted_str


def save_to_json(owners_tables: Dict[str, List[str]], file_path: str) -> None:
    with open(file_path, "w") as f:
        json.dump(owners_tables, f, indent=4)


def save_custom_json_format(owners_tables: Dict[str, List[str]], file_path: str) -> None:
    custom_json = format_as_custom_json(owners_tables)
    with open(file_path, "w") as f:
        f.write(custom_json)