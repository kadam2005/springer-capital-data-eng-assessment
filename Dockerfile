# ── Base image ─────────────────────────────────────────────────────────────
# Python 3.11 slim keeps the image small while supporting zoneinfo (stdlib)
FROM python:3.11-slim

# ── Working directory inside the container ──────────────────────────────────
WORKDIR /app

# ── Install OS-level timezone data (needed by zoneinfo) ────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata && \
    rm -rf /var/lib/apt/lists/*

# ── Python dependencies ─────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Copy application code ───────────────────────────────────────────────────
COPY pipeline.py          .
COPY profiling.py         .
COPY generate_sample_data.py .

# ── Copy source data (replace with real CSVs before building) ───────────────
COPY data/ ./data/

# ── Output directory (will be mounted as a volume at runtime) ───────────────
RUN mkdir -p /app/output

# ── Default command: run the full pipeline ──────────────────────────────────
# To run profiling instead:
#   docker run ... python profiling.py
CMD ["python", "pipeline.py"]
