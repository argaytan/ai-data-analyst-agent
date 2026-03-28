from abc import ABC, abstractmethod
import pandas as pd


class BaseConnector(ABC):
    """Base class for all data connectors.

    Extend this to add Snowflake, Databricks, Azure SQL, etc.
    """

    @abstractmethod
    def connect(self, **kwargs) -> None:
        pass

    @abstractmethod
    def execute_query(self, query: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def list_tables(self) -> list[str]:
        pass

    @abstractmethod
    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_table_preview(self, table_name: str, limit: int = 10) -> pd.DataFrame:
        pass
