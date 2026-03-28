# AI Data Analyst Agent

An intelligent AI agent that lets you **talk to your data in natural language**. Built with Claude API, DuckDB, and Streamlit — designed for enterprise data teams.

## What It Does

- **Natural Language SQL** — Ask questions, get SQL + results instantly
- **Data Profiling** — Automated quality checks, null detection, distribution analysis
- **dbt Model Generation** — Auto-generate staging models, schema YAML, and source configs
- **Multi-Source Architecture** — Modular connectors for CSV, DuckDB, Snowflake, Databricks, Azure SQL
- **Auto-Visualization** — Charts generated from query results

## Architecture

```
┌─────────────────────────────────────────────────┐
│              Streamlit UI (Demo Layer)           │
├─────────────────────────────────────────────────┤
│           AI Agent Core (Claude API)             │
│       Tool Use: SQL, Profile, dbt Generate       │
├──────────┬──────────┬───────────┬───────────────┤
│   CSV/   │  DuckDB  │ Snowflake │  Databricks/  │
│  Parquet │          │ Azure SQL │  PySpark      │
├──────────┴──────────┴───────────┴───────────────┤
│      Transform Layer: Profiling + dbt Gen        │
└─────────────────────────────────────────────────┘
```

## Quick Start

```bash
# Clone and install
pip install -r requirements.txt

# Set your API key
export ANTHROPIC_API_KEY=your-key-here

# Generate sample data
python scripts/generate_sample_data.py

# Run the app
streamlit run app.py
```

## Example Queries

| Query | What Happens |
|-------|-------------|
| "What are the top 5 products by revenue?" | Generates SQL, runs query, shows chart |
| "Profile the customers table" | Full data quality report with scores |
| "Generate a dbt model for orders" | Creates staging SQL + schema YAML |
| "Show monthly revenue trends by country" | Time series analysis with visualization |
| "Find data quality issues" | Cross-table quality audit |

## Tech Stack

- **AI**: Claude API (Anthropic) with tool use
- **Query Engine**: DuckDB (in-process, blazing fast)
- **UI**: Streamlit
- **Data**: Pandas, PyArrow
- **Visualization**: Plotly
- **Architecture**: Modular connector pattern (extensible to any data source)

## Extending Connectors

Add new data sources by implementing `BaseConnector`:

```python
from src.connectors.base import BaseConnector

class SnowflakeConnector(BaseConnector):
    def connect(self, **kwargs):
        # Your Snowflake connection logic
        pass
    # ... implement remaining methods
```

## About the Builder

Senior Data Engineer & AI Agent Builder with expertise in:
- Data pipelines (PySpark, Airflow, dbt)
- Cloud platforms (Snowflake, Databricks, Azure)
- AI/ML integration and agent systems

**Available for consulting and custom implementations.**

## License

MIT
