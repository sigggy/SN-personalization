BEGIN;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS users_raw (
    id TEXT PRIMARY KEY,
    json_data JSONB NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users_clean (
    id TEXT PRIMARY KEY,
    username TEXT,
    name TEXT,
    created_time TIMESTAMPTZ,
    is_bot BOOLEAN,
    is_banned BOOLEAN,
    total_bets INTEGER,
    total_profit NUMERIC,
    total_volume NUMERIC,
    last_login TIMESTAMPTZ,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_clean_username ON users_clean (username);

COMMIT;
