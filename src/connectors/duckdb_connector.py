import pandas as pd
import duckdb

from .base import BaseConnector


class DuckDBConnector(BaseConnector):
    """Connect to an existing DuckDB database file."""

    def __init__(self):
        self.conn = None

    def connect(self, db_path: str = ":memory:", **kwargs) -> None:
        self.conn = duckdb.connect(db_path)

    def execute_query(self, query: str) -> pd.DataFrame:
        return self.conn.execute(query).fetchdf()

    def list_tables(self) -> list[str]:
        result = self.conn.execute("SHOW TABLES").fetchdf()
        return result["name"].tolist()

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        return self.conn.execute(f"DESCRIBE {table_name}").fetchdf()

    def get_table_preview(self, table_name: str, limit: int = 10) -> pd.DataFrame:
        return self.conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchdf()
