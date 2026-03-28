import pandas as pd
import duckdb

from .base import BaseConnector


class CSVConnector(BaseConnector):
    """Load CSV/Parquet files into DuckDB for SQL querying."""

    def __init__(self):
        self.conn = duckdb.connect()
        self._tables: dict[str, str] = {}

    def connect(self, **kwargs) -> None:
        pass  # DuckDB is in-memory, always connected

    def load_file(self, file_path: str, table_name: str | None = None) -> str:
        if table_name is None:
            table_name = file_path.split("/")[-1].split(".")[0]
            table_name = table_name.replace("-", "_").replace(" ", "_").lower()

        if file_path.endswith(".parquet"):
            self.conn.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')"
            )
        else:
            self.conn.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}')"
            )

        self._tables[table_name] = file_path
        return table_name

    def load_dataframe(self, df: pd.DataFrame, table_name: str) -> str:
        self.conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df"
        )
        self._tables[table_name] = "dataframe"
        return table_name

    def execute_query(self, query: str) -> pd.DataFrame:
        return self.conn.execute(query).fetchdf()

    def list_tables(self) -> list[str]:
        return list(self._tables.keys())

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        return self.conn.execute(f"DESCRIBE {table_name}").fetchdf()

    def get_table_preview(self, table_name: str, limit: int = 10) -> pd.DataFrame:
        return self.conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchdf()

    def get_table_stats(self, table_name: str) -> dict:
        row_count = self.conn.execute(f"SELECT COUNT(*) as cnt FROM {table_name}").fetchone()[0]
        schema = self.get_table_schema(table_name)
        numeric_cols = schema[schema["column_type"].isin(
            ["INTEGER", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL", "HUGEINT", "SMALLINT", "TINYINT"]
        )]["column_name"].tolist()

        stats = {"row_count": row_count, "column_count": len(schema), "columns": {}}

        for col in schema["column_name"]:
            null_count = self.conn.execute(
                f'SELECT COUNT(*) FROM {table_name} WHERE "{col}" IS NULL'
            ).fetchone()[0]
            distinct_count = self.conn.execute(
                f'SELECT COUNT(DISTINCT "{col}") FROM {table_name}'
            ).fetchone()[0]
            stats["columns"][col] = {
                "type": schema[schema["column_name"] == col]["column_type"].values[0],
                "nulls": int(null_count),
                "null_pct": round(null_count / row_count * 100, 1) if row_count > 0 else 0,
                "distinct": int(distinct_count),
            }

        if numeric_cols:
            for col in numeric_cols:
                agg = self.conn.execute(
                    f'SELECT MIN("{col}"), MAX("{col}"), AVG("{col}"), MEDIAN("{col}") FROM {table_name}'
                ).fetchone()
                stats["columns"][col].update({
                    "min": agg[0], "max": agg[1],
                    "avg": round(float(agg[2]), 2) if agg[2] else None,
                    "median": agg[3],
                })

        return stats
