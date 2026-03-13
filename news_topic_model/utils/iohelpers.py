


import json
import os
from pathlib import Path
from contextlib import contextmanager
import pandas as pd
import datetime
import time

@contextmanager
def open_file(path, mode="r", encoding="utf-8"):
    """Context manager to safely open and close files."""
    f = open(path, mode, encoding=encoding)
    try:
        yield f
    finally:
        f.close()

def ensure_dir(path: str | Path) -> Path:
    """Ensure a directory exists and return its Path."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p

def writer_jsonl(base_out: str):
    """
    Create a JSONL file for writing in a bronze_jsonl/{day} subfolder of base_out.
    Returns (file_object, file_path).
    """
    day = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    folder = Path(base_out) / "bronze_jsonl" / day
    ensure_dir(folder)
    timestamp = int(time.time())
    path = folder / f"part-{timestamp}.jsonl"
    f = open(path, "a", encoding="utf-8")
    return f, path


# write_parquet now ONLY returns a new file path to write into (it does not write)
def write_parquet(base_out: str | Path) -> Path:
    """Return a new Parquet file path under the daily silver sink.

    The folder structure is: {base_out}/silver_parquet/ingest_date=YYYY-MM-DD
    This function creates the folder if needed and returns a unique file path
    like `part-<unix_ts>.parquet`. It does **not** write anything.
    """
    day = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    folder = Path(base_out) / "silver_parquet" / f"ingest_date={day}"
    ensure_dir(folder)
    ts = int(time.time())
    return folder / f"part-{ts}.parquet"

def read_jsonl(path: str | Path) -> list[dict]:
    """Read all records from a JSONL file."""
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            records.append(json.loads(line))
    return records

def atomic_write(path: str | Path, data: str, encoding="utf-8") -> None:
    """Atomically write text to a file by using a temp file and renaming."""
    path = Path(path)
    ensure_dir(path.parent)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding=encoding) as f:
        f.write(data)
    os.replace(tmp_path, path)

__all__ = ["open_file", "ensure_dir", "write_jsonl", "read_jsonl", "atomic_write", "write_parquet", "writer_jsonl"]


