import pandas as pd


class DataProfiler:
    """Generate data quality profiles — the kind of work a data engineer does daily."""

    @staticmethod
    def profile(df: pd.DataFrame) -> dict:
        profile = {
            "rows": len(df),
            "columns": len(df.columns),
            "memory_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
            "duplicated_rows": int(df.duplicated().sum()),
            "column_profiles": {},
        }

        for col in df.columns:
            col_profile = {
                "dtype": str(df[col].dtype),
                "nulls": int(df[col].isnull().sum()),
                "null_pct": round(df[col].isnull().sum() / len(df) * 100, 1) if len(df) > 0 else 0,
                "unique": int(df[col].nunique()),
                "unique_pct": round(df[col].nunique() / len(df) * 100, 1) if len(df) > 0 else 0,
            }

            if pd.api.types.is_numeric_dtype(df[col]):
                col_profile.update({
                    "min": float(df[col].min()) if not df[col].isnull().all() else None,
                    "max": float(df[col].max()) if not df[col].isnull().all() else None,
                    "mean": round(float(df[col].mean()), 2) if not df[col].isnull().all() else None,
                    "std": round(float(df[col].std()), 2) if not df[col].isnull().all() else None,
                })
            elif pd.api.types.is_string_dtype(df[col]):
                col_profile["top_values"] = df[col].value_counts().head(5).to_dict()
                col_profile["avg_length"] = round(df[col].dropna().str.len().mean(), 1) if not df[col].isnull().all() else None

            profile["column_profiles"][col] = col_profile

        return profile

    @staticmethod
    def quality_score(profile: dict) -> dict:
        scores = {}
        for col, p in profile["column_profiles"].items():
            score = 100
            if p["null_pct"] > 0:
                score -= min(p["null_pct"], 50)
            if p["unique_pct"] == 100 and profile["rows"] > 1:
                pass  # likely a key, good
            elif p["unique"] == 1:
                score -= 20  # constant column
            scores[col] = round(score, 1)

        overall = round(sum(scores.values()) / len(scores), 1) if scores else 0
        return {"overall": overall, "columns": scores}
