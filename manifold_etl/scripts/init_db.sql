BEGIN;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Cleaned user table
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
    collected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_clean_username ON users_clean (username);
CREATE INDEX IF NOT EXISTS idx_users_clean_referred_by ON users_clean (referred_by_user_id);

-- Cleaned bet table
CREATE TABLE IF NOT EXISTS bets_clean (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    contract_id TEXT NOT NULL,
    answer_id TEXT,
    created_time TIMESTAMPTZ NOT NULL,
    updated_time TIMESTAMPTZ,
    amount NUMERIC NOT NULL,
    loan_amount NUMERIC,
    outcome TEXT NOT NULL,
    shares NUMERIC NOT NULL,
    prob_before NUMERIC NOT NULL,
    prob_after NUMERIC NOT NULL,
    liquidity_fee NUMERIC,
    creator_fee NUMERIC,
    platform_fee NUMERIC,
    is_api BOOLEAN,
    is_redemption BOOLEAN NOT NULL,
    challenge_slug TEXT,
    reply_to_comment_id TEXT,
    bet_group_id TEXT,
    limit_prob NUMERIC,
    is_cancelled BOOLEAN,
    order_amount NUMERIC,
    is_filled BOOLEAN,
    expires_at TIMESTAMPTZ,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bets_clean_user ON bets_clean (user_id);
CREATE INDEX IF NOT EXISTS idx_bets_clean_contract ON bets_clean (contract_id);

COMMIT;
