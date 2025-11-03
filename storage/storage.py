import sqlite3



schema = """
CREATE TABLE IF NOT EXISTS meta (
    url TEXT PRIMARY KEY NOT NULL,
    publisher_name TEXT,
    date DATETIME,
    content_id TEXT UNIQUE,
    inserted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
    """

insert_or_ignore_query = """
INSERT INTO meta (url, publisher_name, date, content_id)
    VALUES (?, ?, ?, ?)
    ON CONFLICT DO NOTHING;
"""


class Storage:
    """
    A storage class to handle database operations:
    - Connect to the database
    - Create tables
    - Insert-or-ignore (idempotent upsert) records
    - Close the database connection
    """

    def __init__(self, db_path:str):
        # Initialize the Storage class with the database path and establish a connection.
        # Minimal safety/perf setting per first principles
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=3000")

    def create_table(self) -> None:
        # Create the messages table if it doesn't exist.
        with self.conn:
            self.conn.executescript(schema)
        print("Table created or already exists.")

    def upsert(self, url: str, publisher_name: str, date: str, content_id: str) -> None:
        with self.conn:
            self.conn.execute(
                insert_or_ignore_query,(url, publisher_name, date, content_id)
            )
        
    def close(self) -> None:
        # Close the database connection.
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

        
