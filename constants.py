import re
from pathlib import Path

UPSTREAM_PATTERNS = [
    (re.compile(r'\{\{ project_id\(\) \}\}\.pandata_[a-z]+\.([a-zA-Z0-9_]+)'), 1),
    (re.compile(r'\{\{\s*ref\([\'\"]([a-zA-Z0-9_]+)[\'\"]\)\s*\}\}'), 1),
    (re.compile(r"\{\{\s*source\([\'\"]([a-z0-9_]+)[\'\"],\s*[\'\"]([a-zA-Z0-9_]+)[\'\"]\)\s*\}\}"), 1),
    (re.compile(r'\{\{ project_id\(\) \}\}\.curated_data_shared[^\.\'\"]+\.([a-zA-Z0-9_]+)'), 1)
]

DOWNSTREAM_DIR = Path("./downstream_jsons")
UPSTREAM_DIR = Path("./upstream_jsons")