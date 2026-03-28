# Springer Capital — Referral Program Data Pipeline

A Python/Pandas pipeline that processes 7 source CSV tables for the referral program,
detects potential fraud using business rules, and outputs a 46-row fraud detection report.

---

## Repository Structure

```
referral_pipeline/
├── data/                          ← Place all 7 source CSV files here
│   ├── user_referrals.csv
│   ├── user_referral_logs.csv
│   ├── user_logs.csv
│   ├── lead_log.csv               ← Note: filename is lead_log (not lead_logs)
│   ├── user_referral_statuses.csv
│   ├── referral_rewards.csv
│   └── paid_transactions.csv
├── output/                        ← Generated reports appear here
│   ├── referral_report.csv        ← Main 46-row fraud detection output
│   ├── data_dictionary.xlsx       ← Business-user column reference
│   ├── profiling_report.csv       ← Per-column data profiling (CSV)
│   └── profiling_report.xlsx      ← Per-column data profiling (Excel)
├── pipeline.py                    ← Main ETL + fraud detection script
├── profiling.py                   ← Data profiling script
├── build_data_dictionary.py       ← Generates the Excel data dictionary
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Quick Start — Local Python

**Requirements:** Python 3.11+

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Place your 7 real CSV files in the /data folder

# 3. Run data profiling
python profiling.py
# Output: output/profiling_report.csv and output/profiling_report.xlsx

# 4. Run the main pipeline
python pipeline.py
# Output: output/referral_report.csv

# 5. (Optional) Regenerate the data dictionary
python build_data_dictionary.py
# Output: output/data_dictionary.xlsx
```

---

## Docker

### Build the image

```bash
# Place your real CSVs in ./data/ before building
docker build -t referral-pipeline .
```

### Run the pipeline (exports report to host)

```bash
docker run --rm \
  -v "$(pwd)/output:/app/output" \
  referral-pipeline
```

### Run data profiling instead

```bash
docker run --rm \
  -v "$(pwd)/output:/app/output" \
  referral-pipeline \
  python profiling.py
```

### Mount custom data without rebuilding image

```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/output:/app/output" \
  referral-pipeline
```

---

## Output Files

| File | Description |
|------|-------------|
| `output/referral_report.csv` | 46-row fraud detection report with `is_business_logic_valid` |
| `output/data_dictionary.xlsx` | Column definitions for business users (3 sheets) |
| `output/profiling_report.csv` | Null counts and distinct counts for all source tables |
| `output/profiling_report.xlsx` | Same as above, one sheet per source table |

---

## Pipeline Steps

| Step | Module | What it does |
|------|--------|-------------|
| 1 | `load_data()` | Reads all 7 CSVs; converts string "null" → NaN |
| 2 | `clean_data()` | Parses timestamps (UTC, full ms precision), booleans, extracts reward days from strings, applies initcap to free-text columns |
| 3 | `adjust_timestamps()` | Converts UTC → local time using each table's timezone column; joins user_logs for tables without one |
| 4 | `dedup_referral_logs()` | Collapses 96-row audit log → 1 row per referral (priority: row with transaction ID, then latest) |
| 5 | `join_tables()` | Merges all 7 tables into 46-row flat dataset; asserts no duplicates |
| 6 | `derive_source_category()` | Maps referral_source → Online / Offline / Lead's category |
| 7 | `apply_fraud_logic()` | Sets `is_business_logic_valid` per 9 business rules; adds `invalid_reason` |
| 8 | `build_report()` | Selects final columns, fills nulls, formats datetimes, exports CSV |

---

## Fraud Detection Logic

`is_business_logic_valid = TRUE` when **Condition A** (successful) or **Condition B** (pending/failed) is met:

**Condition A — Successful referral (all 9 must pass):**
1. Reward days > 0
2. Status = "Berhasil"
3. Transaction ID exists
4. Transaction status = "PAID"
5. Transaction type = "NEW"
6. Transaction occurred **after** the referral (millisecond precision)
7. Transaction in the **same calendar month** as the referral
8. Referrer membership **not expired** at referral date
9. Referrer account is **not deleted**

**Condition B — Pending/Failed referral:**
1. Status is "Menunggu" or "Tidak Berhasil"
2. No reward assigned (reward = 0 or null)

`is_business_logic_valid = FALSE` flags potential fraud per these spec rules:
- **Rule 1:** Reward > 0 but status is not "Berhasil"
- **Rule 2:** Reward > 0 but no transaction linked
- **Rule 3:** Paid transaction exists but no reward assigned
- **Rule 4:** Status = "Berhasil" but reward is null/0
- **Rule 5:** Transaction occurred before the referral was created

---

## Environment Variables / Cloud Storage

No credentials are required for local CSV processing.

If you extend the pipeline to read from or write to cloud storage, set credentials
as **environment variables only** — never hardcode them in scripts.

```bash
# AWS S3 example
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=ap-southeast-1

# GCP example
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

Pass them into Docker:
```bash
docker run --rm \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -v "$(pwd)/output:/app/output" \
  referral-pipeline
```

---

## Python Version

Tested with **Python 3.11**. Requires Python 3.9+ for `zoneinfo` (stdlib).
