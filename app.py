import os
import json
import streamlit as st
import pandas as pd
import plotly.express as px

from src.agent import DataAnalystAgent
from src.transforms.profiler import DataProfiler

# --- Page Config ---
st.set_page_config(
    page_title="AI Data Analyst Agent",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Custom CSS for a professional look ---
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #666;
        margin-top: -10px;
        margin-bottom: 30px;
    }
    .tool-card {
        background: #f8f9fa;
        border-left: 4px solid #667eea;
        padding: 12px 16px;
        border-radius: 0 8px 8px 0;
        margin: 8px 0;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 20px;
        border-radius: 12px;
        text-align: center;
    }
    .stChatMessage {
        border-radius: 12px;
    }
</style>
""", unsafe_allow_html=True)

# --- Sidebar ---
with st.sidebar:
    st.markdown("### Configuration")

    # Read API key from Streamlit secrets (cloud) or env var (local)
    default_key = ""
    if hasattr(st, "secrets") and "ANTHROPIC_API_KEY" in st.secrets:
        default_key = st.secrets["ANTHROPIC_API_KEY"]
    else:
        default_key = os.environ.get("ANTHROPIC_API_KEY", "")

    api_key = st.text_input(
        "Anthropic API Key",
        type="password",
        value=default_key,
        help="Get your key at console.anthropic.com",
    )

    st.markdown("---")
    st.markdown("### Load Data")

    # File uploader
    uploaded_files = st.file_uploader(
        "Upload CSV or Parquet files",
        type=["csv", "parquet"],
        accept_multiple_files=True,
    )

    # Load sample data option
    use_sample = st.checkbox("Use sample e-commerce data", value=True)

    st.markdown("---")
    st.markdown("### Connector Architecture")
    st.markdown("""
    **Available Connectors:**
    - CSV / Parquet (active)
    - DuckDB (active)
    - Snowflake (extensible)
    - Databricks / PySpark (extensible)
    - Azure SQL / PostgreSQL (extensible)

    *Contact me to add your data source!*
    """)

    st.markdown("---")
    st.markdown(
        "<div style='text-align:center; color:#999; font-size:0.85rem;'>"
        "Built with Claude API + DuckDB + Streamlit<br>"
        "by a Senior Data Engineer & AI Agent Builder"
        "</div>",
        unsafe_allow_html=True,
    )

# --- Initialize Agent ---
if "agent" not in st.session_state:
    st.session_state.agent = None
if "messages" not in st.session_state:
    st.session_state.messages = []
if "tables_loaded" not in st.session_state:
    st.session_state.tables_loaded = []


def init_agent():
    if not api_key:
        return None
    agent = DataAnalystAgent(api_key=api_key)

    # Load sample data
    if use_sample:
        sample_dir = os.path.join(os.path.dirname(__file__), "data", "sample")
        if os.path.exists(sample_dir):
            for f in os.listdir(sample_dir):
                if f.endswith(".csv"):
                    path = os.path.join(sample_dir, f)
                    name = agent.load_file(path)
                    if name not in st.session_state.tables_loaded:
                        st.session_state.tables_loaded.append(name)

    # Load uploaded files
    if uploaded_files:
        for f in uploaded_files:
            df = pd.read_csv(f) if f.name.endswith(".csv") else pd.read_parquet(f)
            name = f.name.split(".")[0].replace("-", "_").replace(" ", "_").lower()
            agent.load_dataframe(df, name)
            if name not in st.session_state.tables_loaded:
                st.session_state.tables_loaded.append(name)

    return agent


# --- Main UI ---
st.markdown('<p class="main-header">AI Data Analyst Agent</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="sub-header">'
    "Ask questions about your data in natural language. "
    "I'll write SQL, profile quality, and generate dbt models."
    "</p>",
    unsafe_allow_html=True,
)

# Initialize or reinitialize agent
if api_key and (st.session_state.agent is None or use_sample or uploaded_files):
    st.session_state.agent = init_agent()

# Show loaded tables
if st.session_state.tables_loaded:
    cols = st.columns(len(st.session_state.tables_loaded))
    for i, table in enumerate(st.session_state.tables_loaded):
        with cols[i]:
            if st.session_state.agent:
                try:
                    count = st.session_state.agent.connector.execute_query(
                        f"SELECT COUNT(*) as cnt FROM {table}"
                    ).iloc[0, 0]
                    st.metric(label=table, value=f"{count:,} rows")
                except Exception:
                    st.metric(label=table, value="loaded")

    st.markdown("---")

# Example queries
if not st.session_state.messages:
    st.markdown("#### Try asking:")
    example_cols = st.columns(2)
    examples = [
        "What are the top 5 products by revenue?",
        "Profile the customers table for data quality",
        "Show monthly revenue trends with a breakdown by country",
        "Generate a dbt staging model for the orders table",
        "Which customer segment has the highest average order value?",
        "Find any data quality issues across all tables",
    ]
    for i, ex in enumerate(examples):
        with example_cols[i % 2]:
            if st.button(f"💡 {ex}", key=f"ex_{i}", use_container_width=True):
                st.session_state.messages.append({"role": "user", "content": ex})
                st.rerun()

# Chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

        # Show tool results if available
        if "tool_results" in msg:
            for tr in msg["tool_results"]:
                tool_name = tr["tool"]
                result = tr["result"]

                with st.expander(f"🔧 Tool: {tool_name}", expanded=False):
                    if tool_name == "run_sql":
                        st.code(result.get("sql", ""), language="sql")
                        if tr.get("dataframe") is not None:
                            df = tr["dataframe"]
                            st.dataframe(df, use_container_width=True)

                            # Auto-generate chart for numeric results
                            numeric_cols = df.select_dtypes(include="number").columns
                            non_numeric = df.select_dtypes(exclude="number").columns
                            if len(numeric_cols) >= 1 and len(non_numeric) >= 1 and len(df) <= 50:
                                try:
                                    fig = px.bar(
                                        df, x=non_numeric[0], y=numeric_cols[0],
                                        title=result.get("explanation", ""),
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                                except Exception:
                                    pass

                    elif tool_name == "profile_table":
                        profile = result.get("profile", {})
                        quality = result.get("quality_score", {})

                        qcol1, qcol2, qcol3 = st.columns(3)
                        qcol1.metric("Rows", f"{profile.get('rows', 0):,}")
                        qcol2.metric("Columns", profile.get("columns", 0))
                        qcol3.metric("Quality Score", f"{quality.get('overall', 0)}%")

                        if "column_profiles" in profile:
                            profile_df = pd.DataFrame(profile["column_profiles"]).T
                            st.dataframe(profile_df, use_container_width=True)

                    elif tool_name == "generate_dbt_model":
                        st.markdown("**SQL Model:**")
                        st.code(result.get("sql_model", ""), language="sql")
                        st.markdown("**Schema YAML:**")
                        st.code(result.get("schema_yaml", ""), language="yaml")
                        st.markdown("**Source YAML:**")
                        st.code(result.get("source_yaml", ""), language="yaml")

                    else:
                        st.json(result)

# Chat input
if prompt := st.chat_input("Ask about your data..."):
    if not api_key:
        st.error("Please enter your Anthropic API key in the sidebar.")
    elif st.session_state.agent is None:
        st.error("Agent not initialized. Please check your API key.")
    else:
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Analyzing your data..."):
                try:
                    response = st.session_state.agent.chat(prompt)

                    # Show tool results
                    for tr in response.get("tool_results", []):
                        tool_name = tr["tool"]
                        result = tr["result"]

                        with st.expander(f"🔧 Tool: {tool_name}", expanded=True):
                            if tool_name == "run_sql":
                                st.code(result.get("sql", ""), language="sql")
                                if tr.get("dataframe") is not None:
                                    df = tr["dataframe"]
                                    st.dataframe(df, use_container_width=True)
                                    numeric_cols = df.select_dtypes(include="number").columns
                                    non_numeric = df.select_dtypes(exclude="number").columns
                                    if len(numeric_cols) >= 1 and len(non_numeric) >= 1 and len(df) <= 50:
                                        try:
                                            fig = px.bar(
                                                df, x=non_numeric[0], y=numeric_cols[0],
                                                title=result.get("explanation", ""),
                                            )
                                            st.plotly_chart(fig, use_container_width=True)
                                        except Exception:
                                            pass

                            elif tool_name == "profile_table":
                                profile = result.get("profile", {})
                                quality = result.get("quality_score", {})
                                qcol1, qcol2, qcol3 = st.columns(3)
                                qcol1.metric("Rows", f"{profile.get('rows', 0):,}")
                                qcol2.metric("Columns", profile.get("columns", 0))
                                qcol3.metric("Quality Score", f"{quality.get('overall', 0)}%")
                                if "column_profiles" in profile:
                                    profile_df = pd.DataFrame(profile["column_profiles"]).T
                                    st.dataframe(profile_df, use_container_width=True)

                            elif tool_name == "generate_dbt_model":
                                st.markdown("**SQL Model:**")
                                st.code(result.get("sql_model", ""), language="sql")
                                st.markdown("**Schema YAML:**")
                                st.code(result.get("schema_yaml", ""), language="yaml")

                            else:
                                st.json(result)

                    # Show text response
                    st.markdown(response["text"])

                    # Save to messages
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": response["text"],
                        "tool_results": response.get("tool_results", []),
                    })

                except Exception as e:
                    st.error(f"Error: {str(e)}")
