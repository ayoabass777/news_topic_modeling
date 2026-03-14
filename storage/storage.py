import sqlite3

from src.utils.logger import get_logger


logger = get_logger("pipeline.storage")

schema = """
CREATE TABLE IF NOT EXISTS meta (
    url TEXT PRIMARY KEY NOT NULL,
    publisher_name TEXT,
    date DATETIME,
    content_id TEXT UNIQUE,
    bucket TEXT,
    league TEXT,
    signal_type TEXT,
    source_type TEXT,
    source_feed TEXT,
    inserted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

upsert_query = """
INSERT INTO meta (
    url,
    publisher_name,
    date,
    content_id,
    bucket,
    league,
    signal_type,
    source_type,
    source_feed
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(url) DO UPDATE SET
    publisher_name=excluded.publisher_name,
    date=excluded.date,
    content_id=excluded.content_id,
    bucket=excluded.bucket,
    league=excluded.league,
    signal_type=excluded.signal_type,
    source_type=excluded.source_type,
    source_feed=excluded.source_feed;
"""


class Storage:
    """
    A storage class to handle database operations:
    - Connect to the database
    - Create tables
    - Upsert records
    - Close the database connection
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=3000")

    def create_table(self) -> None:
        with self.conn:
            self.conn.executescript(schema)
            self._ensure_columns()
        logger.info("storage: table ready at %s", self.db_path)

    def _ensure_columns(self) -> None:
        expected_columns = {
            "bucket": "TEXT",
            "league": "TEXT",
            "signal_type": "TEXT",
            "source_type": "TEXT",
            "source_feed": "TEXT",
        }
        current_columns = {
            row[1] for row in self.conn.execute("PRAGMA table_info(meta)")
        }
        for column, column_type in expected_columns.items():
            if column not in current_columns:
                self.conn.execute(f"ALTER TABLE meta ADD COLUMN {column} {column_type}")

    def upsert(
        self,
        url: str,
        publisher_name: str,
        date: str,
        content_id: str,
        bucket: str | None = None,
        league: str | None = None,
        signal_type: str | None = None,
        source_type: str | None = None,
        source_feed: str | None = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                upsert_query,
                (
                    url,
                    publisher_name,
                    date,
                    content_id,
                    bucket,
                    league,
                    signal_type,
                    source_type,
                    source_feed,
                ),
            )

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            logger.info("storage: connection closed")
