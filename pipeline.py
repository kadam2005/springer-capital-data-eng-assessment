

import os
import re
import pandas as pd
import numpy as np
from zoneinfo import ZoneInfo
from datetime import datetime, timezone

DATA_DIR   = os.path.join(os.getcwd(), "data")
OUTPUT_DIR = os.path.join(os.getcwd(), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_data() -> dict:
    files = {
        "user_referrals":          "user_referrals.csv",
        "user_referral_logs":      "user_referral_logs.csv",
        "user_logs":               "user_logs.csv",
        "lead_logs":               "lead_log.csv",
        "user_referral_statuses":  "user_referral_statuses.csv",
        "referral_rewards":        "referral_rewards.csv",
        "paid_transactions":       "paid_transactions.csv",
    }
    tables = {}
    for name, filename in files.items():
        path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing: {path}")
        df = pd.read_csv(path, low_memory=False)
        df.replace("null", pd.NA, inplace=True)
        tables[name] = df
        print(f"  Loaded {name:30s} -> {len(df):>4} rows")
    return tables

NO_INITCAP = {"homeclub","transaction_location","preferred_location","referrer_homeclub"}
_ID_LIKE    = re.compile(r"(^id$|_id$|user_id|phone|name)", re.IGNORECASE)

def _initcap_ok(col):
    return col not in _NO_INITCAP and not _ID_LIKE.search(col)

BOOL_MAP = {
    "true":True,"false":False,"True":True,"False":False,
    "TRUE":True,"FALSE":False,True:True,False:False,1:True,0:False
}

TIMESTAMP_COLS = {
    "user_referrals":         ["referral_at","updated_at"],
    "user_referral_logs":     ["created_at"],
    "lead_logs":              ["created_at"],
    "user_referral_statuses": ["created_at"],
    "referral_rewards":       ["created_at"],
    "paid_transactions":      ["transaction_at"],
}

def clean_data(tables: dict) -> dict:
    for name, df in tables.items():
        # Parse timestamps as UTC-aware
        for col in TIMESTAMP_COLS.get(name, []):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        # membership_expired_date is date-only string
        if name == "user_logs" and "membership_expired_date" in df.columns:
            df["membership_expired_date"] = pd.to_datetime(
                df["membership_expired_date"], errors="coerce", utc=False
            ).dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT")

        # Boolean columns
        if name == "user_logs" and "is_deleted" in df.columns:
            df["is_deleted"] = df["is_deleted"].map(BOOL_MAP)
        if name == "user_referral_logs" and "is_reward_granted" in df.columns:
            df["is_reward_granted"] = df["is_reward_granted"].map(BOOL_MAP)

        # Parse "10 days" -> 10.0
        if name == "referral_rewards" and "reward_value" in df.columns:
            df["num_reward_days"] = (
                df["reward_value"].astype(str)
                .str.extract(r"(\d+\.?\d*)")[0].astype(float)
            )

        # Initcap eligible string columns
        for col in df.select_dtypes(include=["object"]).columns:
            if _initcap_ok(col):
                df[col] = df[col].apply(
                    lambda v: v.strip().title() if isinstance(v, str) else v
                )
        tables[name] = df
    return tables

def _to_local(ts, tz_str):
    if pd.isna(ts) or not isinstance(tz_str, str) or not tz_str.strip():
        return pd.NaT
    try:
        return ts.astimezone(ZoneInfo(tz_str))
    except Exception:
        return pd.NaT

def adjust_timestamps(tables: dict) -> dict:
    ul = tables["user_logs"]
    pt = tables["paid_transactions"]
    ll = tables["lead_logs"]
    ur = tables["user_referrals"]

    ul["membership_expired_date_local"] = ul.apply(
        lambda r: _to_local(r["membership_expired_date"], r.get("timezone_homeclub")), axis=1)

    pt["transaction_at_local"] = pt.apply(
        lambda r: _to_local(r["transaction_at"], r.get("timezone_transaction")), axis=1)

    ll["created_at_local"] = ll.apply(
        lambda r: _to_local(r["created_at"], r.get("timezone_location")), axis=1)

    tz_map = ul.set_index("user_id")["timezone_homeclub"].to_dict()
    ur["_tz"] = ur["referrer_id"].map(tz_map)
    ur["referral_at_local"] = ur.apply(lambda r: _to_local(r["referral_at"], r["_tz"]), axis=1)
    ur["updated_at_local"]  = ur.apply(lambda r: _to_local(r["updated_at"],  r["_tz"]), axis=1)
    ur.drop(columns=["_tz"], inplace=True)

    url = tables["user_referral_logs"]
    ref_tz = ur[["referral_id","referrer_id"]].copy()
    ref_tz["_tz"] = ref_tz["referrer_id"].map(tz_map)
    url = url.merge(ref_tz[["referral_id","_tz"]],
                    left_on="user_referral_id", right_on="referral_id", how="left")
    url.drop(columns=["referral_id"], inplace=True)
    url["created_at_local"] = url.apply(lambda r: _to_local(r["created_at"], r["_tz"]), axis=1)
    url.drop(columns=["_tz"], inplace=True)

    tables["user_logs"]           = ul
    tables["paid_transactions"]   = pt
    tables["lead_logs"]           = ll
    tables["user_referrals"]      = ur
    tables["user_referral_logs"]  = url
    return tables

def dedup_referral_logs(url: pd.DataFrame) -> pd.DataFrame:
    """
    Multiple log entries exist per referral (audit trail).
    Keep ONE row per user_referral_id:
      Priority 1: row that has source_transaction_id (reward event)
      Priority 2: most recent created_at
    """
    url = url.copy()
    url["_has_txn"] = url["source_transaction_id"].notna().astype(int)
    deduped = (
        url.sort_values(["_has_txn","created_at"], ascending=[False,False])
           .groupby("user_referral_id", as_index=False).first()
    )
    deduped.drop(columns=["_has_txn"], inplace=True)
    return deduped

def join_tables(tables: dict) -> pd.DataFrame:
    ur  = tables["user_referrals"]
    url = dedup_referral_logs(tables["user_referral_logs"])
    ul  = tables["user_logs"]
    urs = tables["user_referral_statuses"]
    rr  = tables["referral_rewards"]
    pt  = tables["paid_transactions"]
    ll  = tables["lead_logs"]

    # 1. referrals + best log
    df = ur.merge(
        url[["user_referral_id","source_transaction_id","created_at_local","is_reward_granted"]],
        left_on="referral_id", right_on="user_referral_id", how="left"
    )

    # 2. referrer info (deduplicate user_logs by keeping latest row per user_id)
    ul_latest = ul.sort_values("id").groupby("user_id", as_index=False).last()
    referrer_info = ul_latest[[
        "user_id","name","phone_number","homeclub",
        "membership_expired_date_local","is_deleted"
    ]].rename(columns={
        "user_id":                       "_ruid",
        "name":                          "referrer_name",
        "phone_number":                  "referrer_phone_number",
        "homeclub":                      "referrer_homeclub",
        "membership_expired_date_local": "referrer_membership_expired",
        "is_deleted":                    "referrer_is_deleted",
    })
    df = df.merge(referrer_info, left_on="referrer_id", right_on="_ruid", how="left")
    df.drop(columns=["_ruid"], inplace=True)

    # 3. referral status
    df = df.merge(
        urs[["id","description"]].rename(columns={"id":"_sid","description":"referral_status"}),
        left_on="user_referral_status_id", right_on="_sid", how="left"
    ).drop(columns=["_sid"])

    # 4. reward details
    df = df.merge(
        rr[["id","num_reward_days"]].rename(columns={"id":"_rid"}),
        left_on="referral_reward_id", right_on="_rid", how="left"
    ).drop(columns=["_rid"])

    # 5. transaction details
    df["_txn_key"] = df["transaction_id"].fillna(df["source_transaction_id"])
    df = df.merge(
        pt[["transaction_id","transaction_status","transaction_at_local",
            "transaction_location","transaction_type"]].rename(columns={"transaction_id":"_ptid"}),
        left_on="_txn_key", right_on="_ptid", how="left"
    ).drop(columns=["_txn_key","_ptid"])

    # 6. lead source category (referee_id -> lead_id)
    ll_dedup = ll.groupby("lead_id", as_index=False).first()
    ll_slim = ll_dedup[["lead_id","source_category"]].rename(
        columns={"lead_id":"_llid","source_category":"lead_source_category"})
    df = df.merge(ll_slim, left_on="referee_id", right_on="_llid", how="left")
    df.drop(columns=["_llid"], inplace=True)

    assert len(df) == 46, f"Expected 46 rows, got {len(df)}"
    return df

def derive_source_category(df: pd.DataFrame) -> pd.DataFrame:
    def _map(row):
        src = str(row.get("referral_source") or "")
        if "Draft Transaction" in src:   return "Offline"
        if "User Sign Up" in src:        return "Online"
        if "Lead" in src:
            cat = row.get("lead_source_category")
            return cat if pd.notna(cat) else "Unknown"
        return None
    df["referral_source_category"] = df.apply(_map, axis=1)
    return df

def apply_fraud_logic(df: pd.DataFrame) -> pd.DataFrame:
    """
    is_business_logic_valid = True when:

    Condition A (Successful referral - ALL must pass):
      1. reward_days > 0
      2. status == 'Berhasil'
      3. has transaction_id
      4. transaction_status == 'PAID'
      5. transaction_type == 'NEW'
      6. transaction_at > referral_at
      7. transaction same calendar month as referral
      8. referrer membership not expired
      9. referrer account not deleted
      10. is_reward_granted == True

    Condition B (Pending/Failed - no reward assigned):
      1. status in ('Menunggu', 'Tidak Berhasil')
      2. reward_days is null or 0
    """

    def _same_month(ts1, ts2):
        try:
            return ts1.year == ts2.year and ts1.month == ts2.month
        except Exception:
            return False

    def evaluate(row):
        status         = str(row.get("referral_status") or "")
        reward_days    = row.get("num_reward_days")
        txn_id         = row.get("transaction_id") or row.get("source_transaction_id")
        txn_status     = str(row.get("transaction_status") or "")
        txn_type       = str(row.get("transaction_type") or "")
        txn_at         = row.get("transaction_at_local")
        ref_at         = row.get("referral_at_local")
        mem_exp        = row.get("referrer_membership_expired")
        is_deleted     = row.get("referrer_is_deleted")
        reward_granted = row.get("is_reward_granted")

        has_reward = pd.notna(reward_days) and float(reward_days) > 0
        has_txn    = pd.notna(txn_id) and str(txn_id).strip() not in ("","nan")

        if status == "Berhasil":
            txn_after  = pd.notna(txn_at) and pd.notna(ref_at) and txn_at > ref_at
            same_month = pd.notna(txn_at) and pd.notna(ref_at) and _same_month(txn_at, ref_at)
            mem_ok     = pd.isna(mem_exp) or (pd.notna(ref_at) and mem_exp > ref_at)
            return all([
                has_reward,                          # 1
                True,                                # 2 already Berhasil
                has_txn,                             # 3
                txn_status.upper() == "PAID",        # 4
                txn_type.upper() == "NEW",           # 5
                txn_after,                           # 6
                same_month,                          # 7
                mem_ok,                              # 8
                not bool(is_deleted),                # 9
                bool(reward_granted) is True,        # 10
            ])

        if status in ("Menunggu", "Tidak Berhasil"):
            return not has_reward

        return False

    def invalid_reason(row):
        if row["is_business_logic_valid"]:
            return ""
        status      = str(row.get("referral_status") or "")
        reward_days = row.get("num_reward_days")
        txn_id      = row.get("transaction_id") or row.get("source_transaction_id")
        txn_status  = str(row.get("transaction_status") or "")
        txn_type    = str(row.get("transaction_type") or "")
        txn_at      = row.get("transaction_at_local")
        ref_at      = row.get("referral_at_local")
        reward_granted = row.get("is_reward_granted")

        has_reward = pd.notna(reward_days) and float(reward_days or 0) > 0
        has_txn    = pd.notna(txn_id) and str(txn_id).strip() not in ("","nan")
        reasons = []

        if status == "Berhasil":
            if not has_reward:                       reasons.append("no reward value")
            if not has_txn:                          reasons.append("no transaction id")
            if txn_status.upper() != "PAID":         reasons.append(f"txn_status={txn_status}")
            if txn_type.upper() != "NEW":            reasons.append(f"txn_type={txn_type}")
            if pd.notna(txn_at) and pd.notna(ref_at) and txn_at <= ref_at:
                                                     reasons.append("transaction before referral")
            if not bool(reward_granted):             reasons.append("reward not granted")
        elif status in ("Menunggu","Tidak Berhasil"):
            if has_reward:                           reasons.append("reward on pending/failed referral")
        else:
            reasons.append(f"unknown status: {status}")

        return "; ".join(reasons) if reasons else "multiple conditions"

    df["is_business_logic_valid"] = df.apply(evaluate, axis=1)
    df["invalid_reason"]          = df.apply(invalid_reason, axis=1)
    return df

def build_report(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    df.insert(0, "referral_details_id", df.index + 1)

    col_map = {
        "referral_details_id":      "referral_details_id",
        "referral_id":              "referral_id",
        "referral_source":          "referral_source",
        "referral_source_category": "referral_source_category",
        "referral_at_local":        "referral_at",
        "referrer_id":              "referrer_id",
        "referrer_name":            "referrer_name",
        "referrer_phone_number":    "referrer_phone_number",
        "referrer_homeclub":        "referrer_homeclub",
        "referee_id":               "referee_id",
        "referee_name":             "referee_name",
        "referee_phone":            "referee_phone",
        "referral_status":          "referral_status",
        "num_reward_days":          "num_reward_days",
        "transaction_id":           "transaction_id",
        "transaction_status":       "transaction_status",
        "transaction_at_local":     "transaction_at",
        "transaction_location":     "transaction_location",
        "transaction_type":         "transaction_type",
        "updated_at_local":         "updated_at",
        "created_at_local":         "reward_granted_at",
        "is_business_logic_valid":  "is_business_logic_valid",
    }

    available = {k: v for k, v in col_map.items() if k in df.columns}
    report = df[list(available.keys())].rename(columns=available).copy()

    # Types
    report["referral_details_id"] = report["referral_details_id"].astype("Int64")
    report["num_reward_days"] = pd.to_numeric(
        report["num_reward_days"], errors="coerce").fillna(0).astype("Int64")

    str_cols = [
        "referral_id","referral_source","referral_source_category",
        "referrer_id","referrer_name","referrer_phone_number","referrer_homeclub",
        "referee_id","referee_name","referee_phone","referral_status",
        "transaction_id","transaction_status","transaction_location","transaction_type",
    ]
    for c in str_cols:
        if c in report.columns:
            report[c] = report[c].fillna("").astype(str).str.replace("^nan$","",regex=True)

    for dt_col in ["referral_at","transaction_at","updated_at","reward_granted_at"]:
        if dt_col in report.columns:
            report[dt_col] = pd.to_datetime(
                report[dt_col], errors="coerce", utc=False
            ).apply(lambda t: t.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(t) else "")

    for c in ["referral_status","transaction_status","transaction_type","referral_source"]:
        if c in report.columns:
            report[c] = report[c].apply(lambda v: v.title() if isinstance(v,str) and v else v)

    report.sort_values("referral_details_id", inplace=True)
    report.reset_index(drop=True, inplace=True)
    return report

def build_report(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    df.insert(0, "referral_details_id", df.index + 1)

    col_map = {
        "referral_details_id":      "referral_details_id",
        "referral_id":              "referral_id",
        "referral_source":          "referral_source",
        "referral_source_category": "referral_source_category",
        "referral_at_local":        "referral_at",
        "referrer_id":              "referrer_id",
        "referrer_name":            "referrer_name",
        "referrer_phone_number":    "referrer_phone_number",
        "referrer_homeclub":        "referrer_homeclub",
        "referee_id":               "referee_id",
        "referee_name":             "referee_name",
        "referee_phone":            "referee_phone",
        "referral_status":          "referral_status",
        "num_reward_days":          "num_reward_days",
        "transaction_id":           "transaction_id",
        "transaction_status":       "transaction_status",
        "transaction_at_local":     "transaction_at",
        "transaction_location":     "transaction_location",
        "transaction_type":         "transaction_type",
        "updated_at_local":         "updated_at",
        "created_at_local":         "reward_granted_at",
        "is_business_logic_valid":  "is_business_logic_valid",
    }

    available = {k: v for k, v in col_map.items() if k in df.columns}
    report = df[list(available.keys())].rename(columns=available).copy()

    # Types
    report["referral_details_id"] = report["referral_details_id"].astype("Int64")
    report["num_reward_days"] = pd.to_numeric(
        report["num_reward_days"], errors="coerce").fillna(0).astype("Int64")

    str_cols = [
        "referral_id","referral_source","referral_source_category",
        "referrer_id","referrer_name","referrer_phone_number","referrer_homeclub",
        "referee_id","referee_name","referee_phone","referral_status",
        "transaction_id","transaction_status","transaction_location","transaction_type",
    ]
    for c in str_cols:
        if c in report.columns:
            report[c] = report[c].fillna("").astype(str).str.replace("^nan$","",regex=True)

    for dt_col in ["referral_at","transaction_at","updated_at","reward_granted_at"]:
        if dt_col in report.columns:
            report[dt_col] = pd.to_datetime(
                report[dt_col], errors="coerce", utc=False
            ).apply(lambda t: t.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(t) else "")

    for c in ["referral_status","transaction_status","transaction_type","referral_source"]:
        if c in report.columns:
            report[c] = report[c].apply(lambda v: v.title() if isinstance(v,str) and v else v)

    report.sort_values("referral_details_id", inplace=True)
    report.reset_index(drop=True, inplace=True)
    return report









