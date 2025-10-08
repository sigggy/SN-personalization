# Manifold Decision Data ETL

This project defines a reproducible, modular ETL pipeline for collecting, transforming, and storing Manifold Markets user and bet data in PostgreSQL. The current milestone paginates the `/v0/users` endpoint—which returns the full `FullUser` payload—and produces a cleaned, query-friendly representation ready for analytics.

## Architecture Overview

| Stage | Description |
| ----- | ----------- |
| Extract | Page through `/v0/users`; requests use automatic retries with exponential backoff to respect API throttling. |
| Transform | Normalize payloads via Pydantic models, add ingestion timestamps, and enforce required fields. |
| Load | Upsert into cleaned tables (`users_clean`, `bets_clean`) in PostgreSQL hosted on GCP. |

Future milestones can extend the same patterns to additional endpoints once the core ingestion pipeline is battle-tested.

## Repository Layout

```
etl/
├── README.md
├── requirements.txt
├── .env.example
├── config.py
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── extract/
│   │   ├── bets.py
│   │   ├── __init__.py
│   │   └── users.py
│   ├── load/
│   │   ├── __init__.py
│   │   └── postgres.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── bet.py
│   │   └── user.py
│   ├── transform/
│   │   ├── __init__.py
│   │   └── normalize.py
│   └── utils/
│       ├── __init__.py
│       └── manifold.py
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
   - Copy `.env.example` to `.env` and update `POSTGRES_URI`.

4. **Initialize the database schema**
   ```bash
   psql "$POSTGRES_URI" -f scripts/init_db.sql
   ```

5. **Run the pipeline**
   ```bash
   # From the etl/ directory
   python -m src.main
   ```

   Both user and bet stages run by default. Use `--users-only` or `--bets-only` to run a single stage.

### Runtime configuration

Tune collection limits and chunk sizes via `config.py`. Key settings include:

- `USER_LIMIT` – cap the number of users ingested (set to `None` for no limit).
- `USER_PAGE_SIZE` and `CHUNK_SIZE` – control API pagination and upsert batch sizes for users.
- `BET_USER_CHUNK_SIZE`, `BET_WORKER_COUNT`, `THRESHOLD`, and `LIMIT` – manage bet ingestion workload, concurrency, and API paging.
- `LOG_DIR` – directory where ETL log files are written.

## Logging

Each ETL invocation writes to `logs/etl_YYYYMMDD.log`. Logs include request counts, rate-limit sleeps, validation failures, and database upsert summaries.

## Development Guidelines

- Keep ingestions idempotent; UPSERTs keyed on the source `id` avoid duplicates.
- Respect API limits; the client already backs off on 429s and transient failures.
- All timestamps are recorded in UTC.
- Add unit tests for extraction and transformation logic to ensure regressions are caught.
- Use the Pydantic models in `etl/src/models` when you introduce new cleaned tables so transformation stays consistent.
- Expand new entities (bets, markets, etc.) by mirroring the extract → transform → load structure once ready.
