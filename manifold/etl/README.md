# Manifold Decision Data ETL

This project defines a reproducible, modular ETL pipeline for collecting, transforming, and storing Manifold Markets user data in PostgreSQL. The initial milestone focuses on syncing the `/v0/users` endpoint so downstream teams can experiment with per-user decision modeling.

## Architecture Overview

| Stage | Description |
| ----- | ----------- |
| Extract | Page through Manifold's `/v0/users` endpoint using a shared rate-limited HTTP client. |
| Transform | Normalize payloads, add ingestion timestamps, flatten key attributes (e.g., `id`, `username`, `numBets`), and retain the raw JSON blob for long-term reference. |
| Load | Upsert into staging (`users_raw`) and cleaned (`users_clean`) tables in PostgreSQL hosted on GCP. |

Future milestones will expand the same patterns to bets and markets once the user ingestion pipeline is battle-tested.

## Repository Layout

```
etl/
├── README.md
├── requirements.txt
├── .env.example
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── extract/
│   │   ├── __init__.py
│   │   └── users.py
│   ├── transform/
│   │   ├── __init__.py
│   │   └── normalize.py
│   ├── load/
│   │   ├── __init__.py
│   │   └── postgres.py
│   └── utils/
│       ├── __init__.py
│       ├── manifold.py
│       └── rate_limit.py
└── scripts/
    └── init_db.sql
```

## Getting Started

1. **Create a virtual environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   - Copy `.env.example` to `.env` and update values:
     ```ini
     MANIFOLD_API_KEY=your_api_key_or_leave_blank
     POSTGRES_URI=postgresql://user:password@host:5432/manifolddb
     LOG_DIR=logs
     ```

4. **Initialize the database schema**
   ```bash
   psql "$POSTGRES_URI" -f scripts/init_db.sql
   ```

5. **Run the pipeline**
   ```bash
   # From the etl/ directory
   python -m src.main
   ```

   Useful flags include `--user-limit` to cap the run size and `--chunk-size` to control UPSERT batch sizes.

## Logging

Each ETL invocation writes to `logs/etl_YYYYMMDD.log`. Logs include request counts, rate-limit sleeps, and database upsert summaries.

## Development Guidelines

- Keep ingestions idempotent; UPSERTs keyed on the source `id` avoid duplicates.
- Respect API limits; the shared `rate_limit` utility coordinates sleeps across services.
- All timestamps are recorded in UTC.
- Add unit tests for extraction and transformation logic to ensure regressions are caught.
- Expand new entities (bets, markets, etc.) by mirroring the extract → transform → load structure once ready.
