



import os
import pandas as pd
import numpy as np

DATA_DIR = os.getcwd()
OUTPUT_DIR = os.path.join(os.getcwd(), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Tables to profile ─────────────────────────────────────────────────────────
TABLES = {
    "user_referrals":         "user_referrals.csv",
    "user_referral_logs":     "user_referral_logs.csv",
    "user_logs":              "user_logs.csv",
    "lead_logs":              "lead_log.csv",
    "user_referral_statuses": "user_referral_statuses.csv",
    "referral_rewards":       "referral_rewards.csv",
    "paid_transactions":      "paid_transactions.csv",
}



def profile_dataframe(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    #Compute per-column profiling statistics for a single DataFrame.
    #Returns a DataFrame with one row per column containing:- data_type, null_count, null_pct, distinct_count,min_value, max_value, sample_values
    rows = []
    total = len(df)

    for col in df.columns:
        series = df[col]

        null_count    = int(series.isna().sum())
        null_pct      = round(null_count / total * 100, 2) if total > 0 else 0
        distinct_count = int(series.nunique(dropna=True))

        # Min / max (works for numeric, dates stored as strings, and plain strings)
        non_null = series.dropna()
        try:
            min_val = non_null.min()
            max_val = non_null.max()
        except Exception:
            min_val = max_val = "N/A"

        # Grab up to 3 distinct non-null sample values
        samples = list(non_null.unique()[:3])
        sample_str = " | ".join(str(s) for s in samples)

        rows.append({
            "table_name":     table_name,
            "column_name":    col,
            "data_type":      str(series.dtype),
            "total_rows":     total,
            "null_count":     null_count,
            "null_pct":       null_pct,
            "distinct_count": distinct_count,
            "min_value":      str(min_val),
            "max_value":      str(max_val),
            "sample_values":  sample_str,
        })

    return pd.DataFrame(rows)



def run_profiling():
    all_profiles = []

    print("=" * 60)
    print("DATA PROFILING REPORT")
    print("=" * 60)

    for table_name, filename in TABLES.items():
        filepath = os.path.join(DATA_DIR, filename)

        if not os.path.exists(filepath):
            print(f"\n[SKIP] {filename} not found in {DATA_DIR}/")
            continue

        df = pd.read_csv(filepath, low_memory=False)
        profile = profile_dataframe(table_name, df)
        all_profiles.append(profile)

        # ── Console summary ───────────────────────────────────────────────────
        print(f"\n{'─'*60}")
        print(f"Table : {table_name}")
        print(f"Rows  : {len(df):,}   |   Columns: {len(df.columns)}")
        print(f"{'─'*60}")
        print(profile[["column_name","data_type","null_count","null_pct","distinct_count"]].to_string(index=False))

    if not all_profiles:
        print("\nNo CSV files found. Run generate_sample_data.py first.")
        return

    combined = pd.concat(all_profiles, ignore_index=True)
    csv_path = os.path.join(OUTPUT_DIR, "profiling_report.csv")
    combined.to_csv(csv_path, index=False)
    print(f"\n\n✓ Combined CSV  → {csv_path}")



run_profiling()