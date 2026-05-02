"""
Stats Service
=============
CSV se column-wise statistics nikalta hai — AI analysis ke liye
"""

import pandas as pd
import numpy as np
import io
from typing import Dict, Any


def compute_col_stats(csv_text: str, max_sample_rows: int = 100) -> Dict[str, Any]:
    """
    CSV text se column statistics compute karo.
    Returns dict jo AI prompt mein use hoga.
    """
    try:
        df = pd.read_csv(io.StringIO(csv_text))
    except Exception as e:
        return {}

    stats = {}
    for col in df.columns:
        series = df[col]
        col_stat: Dict[str, Any] = {
            "dtype":        str(series.dtype),
            "null_count":   int(series.isnull().sum()),
            "null_pct":     round(series.isnull().mean() * 100, 1),
            "unique_count": int(series.nunique()),
            "total_count":  len(series),
        }

        # Numeric columns
        if pd.api.types.is_numeric_dtype(series):
            col_stat.update({
                "mean":   round(float(series.mean()), 2) if not series.isnull().all() else None,
                "median": round(float(series.median()), 2) if not series.isnull().all() else None,
                "std":    round(float(series.std()), 2) if not series.isnull().all() else None,
                "min":    round(float(series.min()), 2) if not series.isnull().all() else None,
                "max":    round(float(series.max()), 2) if not series.isnull().all() else None,
                "zeros":  int((series == 0).sum()),
                "negatives": int((series < 0).sum()),
            })
            # Outlier check (IQR)
            try:
                Q1, Q3 = series.quantile(0.25), series.quantile(0.75)
                IQR    = Q3 - Q1
                out    = int(((series < Q1 - 1.5*IQR) | (series > Q3 + 1.5*IQR)).sum())
                col_stat["outlier_count"] = out
            except Exception:
                pass

        # Categorical / text columns
        else:
            try:
                top = series.value_counts().head(5)
                col_stat["top_values"] = {str(k): int(v) for k, v in top.items()}
                col_stat["avg_length"] = round(series.dropna().astype(str).str.len().mean(), 1)
            except Exception:
                pass

            # Date detection
            date_kws = ["date","time","dt","created","updated","dob","birth"]
            if any(k in col.lower() for k in date_kws):
                col_stat["likely_date"] = True

        stats[col] = col_stat

    # Sample rows (first 5, as CSV)
    sample_csv = df.head(5).to_csv(index=False)

    return {
        "col_stats":  stats,
        "sample_csv": sample_csv,
        "total_rows": len(df),
        "total_cols": len(df.columns),
    }
