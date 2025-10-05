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
    username TEXT NOT NULL,
    name TEXT NOT NULL,
    avatar_url TEXT,
    bio TEXT,
    website TEXT,
    banner_url TEXT,
    discord_handle TEXT,
    twitter_handle TEXT,
    created_time TIMESTAMPTZ NOT NULL,
    last_updated_time TIMESTAMPTZ,
    last_login TIMESTAMPTZ,
    sweepstakes_verified_time TIMESTAMPTZ,
    is_bot BOOLEAN NOT NULL DEFAULT FALSE,
    is_admin BOOLEAN NOT NULL DEFAULT FALSE,
    is_trustworthy BOOLEAN NOT NULL DEFAULT FALSE,
    is_banned BOOLEAN,
    is_banned_from_mana BOOLEAN,
    is_banned_from_sweepcash BOOLEAN,
    is_advanced_trader BOOLEAN,
    id_verified BOOLEAN,
    verified_phone BOOLEAN,
    sweepstakes_verified BOOLEAN,
    kyc_status TEXT,
    referred_by_user_id TEXT,
    total_bets INTEGER,
    total_profit NUMERIC,
    total_volume NUMERIC,
    current_betting_streak INTEGER,
    follower_count INTEGER,
    creator_traders JSONB NOT NULL,
    next_loan_cached NUMERIC,
    resolved_profit_adjustment NUMERIC,
    balance NUMERIC NOT NULL,
    cash_balance NUMERIC,
    spice_balance NUMERIC,
    total_deposits NUMERIC NOT NULL,
    total_cash_deposits NUMERIC,
    url TEXT,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_clean_username ON users_clean (username);
CREATE INDEX IF NOT EXISTS idx_users_clean_referred_by ON users_clean (referred_by_user_id);

COMMIT;
