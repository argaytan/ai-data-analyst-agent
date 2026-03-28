import json
import anthropic
import pandas as pd

from ..connectors.csv_connector import CSVConnector
from ..transforms.profiler import DataProfiler
from ..transforms.dbt_generator import DbtModelGenerator


SYSTEM_PROMPT = """You are an expert AI Data Analyst Agent built by a senior data engineer.
You help users explore, analyze, and transform their data using SQL, profiling, and dbt.

You have access to the following tools to interact with the user's data:

IMPORTANT RULES:
- Always use the tools to answer data questions — never guess or make up data.
- When writing SQL, use DuckDB syntax.
- Explain your analysis in clear, business-friendly language.
- When you find issues (nulls, duplicates, outliers), suggest how to fix them.
- When asked to create dbt models, generate production-ready code.
- Be proactive: suggest follow-up analyses the user might find valuable.
"""

TOOLS = [
    {
        "name": "run_sql",
        "description": "Execute a SQL query against the loaded data using DuckDB. Returns results as a table. Use this to answer questions about the data, aggregate, filter, join, etc.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The SQL query to execute (DuckDB syntax)",
                },
                "explanation": {
                    "type": "string",
                    "description": "Brief explanation of what this query does and why",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "profile_table",
        "description": "Run a comprehensive data quality profile on a table. Returns statistics, null counts, distributions, and quality scores for every column.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to profile",
                },
            },
            "required": ["table_name"],
        },
    },
    {
        "name": "generate_dbt_model",
        "description": "Generate a dbt staging model (SQL + YAML schema) for a table. Includes tests based on data profiling.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to generate dbt model for",
                },
            },
            "required": ["table_name"],
        },
    },
    {
        "name": "list_tables",
        "description": "List all available tables in the current session.",
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "describe_table",
        "description": "Show the schema (column names, types) and a preview of a table.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to describe",
                },
            },
            "required": ["table_name"],
        },
    },
]


class DataAnalystAgent:
    def __init__(self, api_key: str):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.connector = CSVConnector()
        self.profiler = DataProfiler()
        self.dbt_gen = DbtModelGenerator()
        self.conversation: list[dict] = []
        self.model = "claude-sonnet-4-20250514"

    def load_file(self, file_path: str, table_name: str | None = None) -> str:
        return self.connector.load_file(file_path, table_name)

    def load_dataframe(self, df: pd.DataFrame, table_name: str) -> str:
        return self.connector.load_dataframe(df, table_name)

    def _handle_tool_call(self, tool_name: str, tool_input: dict) -> dict:
        """Execute a tool call and return the result."""
        try:
            if tool_name == "run_sql":
                df = self.connector.execute_query(tool_input["query"])
                return {
                    "sql": tool_input["query"],
                    "explanation": tool_input.get("explanation", ""),
                    "result": df.to_string(index=False),
                    "row_count": len(df),
                    "columns": list(df.columns),
                    "_dataframe": df,
                }

            elif tool_name == "profile_table":
                table = tool_input["table_name"]
                df = self.connector.execute_query(f"SELECT * FROM {table}")
                profile = self.profiler.profile(df)
                quality = self.profiler.quality_score(profile)
                return {
                    "profile": profile,
                    "quality_score": quality,
                    "table": table,
                }

            elif tool_name == "generate_dbt_model":
                table = tool_input["table_name"]
                schema = self.connector.get_table_schema(table)
                stats = self.connector.get_table_stats(table)
                sql_model = self.dbt_gen.generate_staging_model(table, schema)
                yaml_schema = self.dbt_gen.generate_schema_yaml(table, schema, stats)
                source_yaml = self.dbt_gen.generate_source_yaml(table)
                return {
                    "sql_model": sql_model,
                    "schema_yaml": yaml_schema,
                    "source_yaml": source_yaml,
                    "table": table,
                }

            elif tool_name == "list_tables":
                tables = self.connector.list_tables()
                table_info = {}
                for t in tables:
                    count = self.connector.execute_query(f"SELECT COUNT(*) as cnt FROM {t}").iloc[0, 0]
                    cols = len(self.connector.get_table_schema(t))
                    table_info[t] = {"rows": int(count), "columns": cols}
                return {"tables": table_info}

            elif tool_name == "describe_table":
                table = tool_input["table_name"]
                schema = self.connector.get_table_schema(table)
                preview = self.connector.get_table_preview(table, limit=5)
                return {
                    "schema": schema.to_string(index=False),
                    "preview": preview.to_string(index=False),
                    "table": table,
                }

            else:
                return {"error": f"Unknown tool: {tool_name}"}

        except Exception as e:
            return {"error": str(e)}

    def _build_context(self) -> str:
        """Build context about available tables."""
        tables = self.connector.list_tables()
        if not tables:
            return "No tables loaded yet."

        context_parts = ["Available tables:"]
        for t in tables:
            schema = self.connector.get_table_schema(t)
            count = self.connector.execute_query(f"SELECT COUNT(*) FROM {t}").iloc[0, 0]
            cols = ", ".join(f"{r['column_name']} ({r['column_type']})" for _, r in schema.iterrows())
            context_parts.append(f"- {t} ({count} rows): {cols}")

        return "\n".join(context_parts)

    def chat(self, user_message: str) -> dict:
        """Send a message and get a response. Returns dict with text, tool_results, etc."""
        context = self._build_context()
        system = f"{SYSTEM_PROMPT}\n\nCurrent data context:\n{context}"

        self.conversation.append({"role": "user", "content": user_message})

        all_tool_results = []
        messages = list(self.conversation)

        # Agentic loop: keep calling until we get a final text response
        while True:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                system=system,
                tools=TOOLS,
                messages=messages,
            )

            if response.stop_reason == "tool_use":
                # Process all tool calls in this response
                assistant_content = response.content
                tool_results_content = []

                for block in assistant_content:
                    if block.type == "tool_use":
                        result = self._handle_tool_call(block.name, block.input)
                        # Store for UI (keep dataframes separate)
                        display_result = {k: v for k, v in result.items() if k != "_dataframe"}
                        all_tool_results.append({
                            "tool": block.name,
                            "input": block.input,
                            "result": display_result,
                            "dataframe": result.get("_dataframe"),
                        })
                        # Build tool_result for API
                        tool_results_content.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": json.dumps(display_result, default=str),
                        })

                messages.append({"role": "assistant", "content": assistant_content})
                messages.append({"role": "user", "content": tool_results_content})

            else:
                # Final response — extract text
                text = ""
                for block in response.content:
                    if hasattr(block, "text"):
                        text += block.text

                # Update conversation history
                self.conversation = messages
                self.conversation.append({"role": "assistant", "content": response.content})

                return {
                    "text": text,
                    "tool_results": all_tool_results,
                }
