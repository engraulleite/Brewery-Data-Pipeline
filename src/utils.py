import os
import re
from datetime import datetime
import pytz
from pathlib import Path

def get_timezone_aware_date(timezone_str="America/Sao_Paulo") -> str:
    tz = pytz.timezone(timezone_str)
    return datetime.now(tz).strftime("%Y-%m-%d")

def get_latest_date_folder(base_path: str) -> str:
    try:
        folders = [
            f.name for f in Path(base_path).iterdir()
            if f.is_dir() and is_valid_date(f.name)
        ]
        if not folders:
            raise FileNotFoundError(f"No valid folders found in {base_path}")
        latest_folder = max(folders, key=lambda d: datetime.strptime(d, "%Y-%m-%d"))
        return latest_folder
    except Exception as e:
        raise RuntimeError(f"Error identifying most recent folder: {e}")

def is_valid_date(folder_name: str) -> bool:
    return re.fullmatch(r"\d{4}-\d{2}-\d{2}", folder_name) is not None