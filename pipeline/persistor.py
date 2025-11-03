"""
Takes dictionaries from an asyncio.Queue and persists them to disk.
- JSONL: stream line-by-line until a None sentinel is received, then close
- Parquet: write once at the end from an in-memory buffer
"""
import asyncio
import json, tempfile, os
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from src.storage.storage import Storage
from src.utils.logger import get_logger


DATA_DIR = Path(__file__).parent.parent / "data"

logger = get_logger("pipeline.persistor")
def ensure_data_dir() -> Path:
    """Ensure the data directory exists and return its Path."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR

def parquet_persist(data: list[dict], path: str | Path) -> None:
    """Persist a list of dictionaries to a Parquet file."""
    table = pa.Table.from_pylist(data)
    path = Path(path)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        pq.write_table(table, tmp_path)
    os.replace(tmp_path, path)
    logger.info("persistor: wrote %d records to %s", len(data), path)


# item  dir_path-> int : 1
# write files to jsonl and parquet directory and return count 
# use flusher to flush every 50 records
async def file_writer(content_queue: asyncio.Queue, dir_path: str | Path, write_parquet: bool) -> int:

    json_path = dir_path / "articles.jsonl"
    parquet_path = dir_path / "articles.parquet"
    store = Storage(dir_path/"articles.db")
    store.create_table()
    parquet_buffer = []
    written_articles = 0
    FLUSH_EVERY = 50


    logger.info("persistor: writing articles to %s", json_path)

    with open(json_path, mode="a", encoding="utf-8") as jf:
        while True:
            item = await content_queue.get()
            try:
                if item is None:
                    logger.info("persistor: received sentinel, stopping writer")
                    break
                try:
                    # Upsert metadata to SQLite
                    store.upsert(
                        url=item.get("url"),
                        publisher_name=item.get("publisher_name"),
                        date=item.get("published_date"),
                        content_id=item.get("content_id"),
                    )
                    # Append to Parquet buffer if needed
                    if write_parquet:
                        parquet_buffer.append(item)
                    # Write to JSONL
                    jf.write(json.dumps(item, ensure_ascii=False))
                    jf.write("\n")
                    written_articles += 1
                    if FLUSH_EVERY > 0 and (written_articles % FLUSH_EVERY) == 0:
                        jf.flush()

                except Exception as e:
                    logger.warning("persistor: error writing item %s (%s)", item, e)
                    continue

            finally:
                content_queue.task_done()
    store.close()
        
    if write_parquet and parquet_buffer:
        parquet_persist(parquet_buffer, parquet_path)

    return written_articles


async def persistor(content_queue: asyncio.Queue, write_parquet: bool = True) -> None:
    """Consume content_queue and persist messages to disk.
    
    Streams JSONL line by line to DATA_DIR/articles.jsonl and closes when a None is received.
    If write_parquet=True, also collects records in memory and writes a Parquet file at the end.
    """
    dir_path = ensure_data_dir()

    written_articles = await file_writer(content_queue, dir_path, write_parquet)


    logger.info("persistor: persisted %d articles to %s", written_articles, dir_path)
