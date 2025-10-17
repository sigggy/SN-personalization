"""PostgreSQL loading utilities for the Manifold ETL pipeline."""

from __future__ import annotations

import logging
from typing import Dict, Iterator, List, Mapping, Optional, Sequence, Tuple, Type

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    create_engine,
    func,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeMeta, declarative_base

logger = logging.getLogger(__name__)


Base: DeclarativeMeta = declarative_base()


class UserClean(Base):
    __tablename__ = "users_clean"
    __table_args__ = (
        Index("idx_users_clean_username", "username"),
        Index("idx_users_clean_referred_by", "referred_by_user_id"),
    )

    id = Column(String, primary_key=True)
    username = Column(String, nullable=False)
    name = Column(String, nullable=False)
    avatar_url = Column(Text)
    bio = Column(Text)
    website = Column(Text)
    banner_url = Column(Text)
    discord_handle = Column(String)
    twitter_handle = Column(String)
    created_time = Column(DateTime(timezone=True), nullable=False)
    last_updated_time = Column(DateTime(timezone=True))
    last_login = Column(DateTime(timezone=True))
    sweepstakes_verified_time = Column(DateTime(timezone=True))
    is_bot = Column(Boolean, nullable=False, server_default=text("false"))
    is_admin = Column(Boolean, nullable=False, server_default=text("false"))
    is_trustworthy = Column(Boolean, nullable=False, server_default=text("false"))
    is_banned = Column(Boolean)
    is_banned_from_mana = Column(Boolean)
    is_banned_from_sweepcash = Column(Boolean)
    is_advanced_trader = Column(Boolean)
    id_verified = Column(Boolean)
    verified_phone = Column(Boolean)
    sweepstakes_verified = Column(Boolean)
    kyc_status = Column(String)
    referred_by_user_id = Column(String)
    total_profit = Column(Numeric)
    total_volume = Column(Numeric)
    current_betting_streak = Column(Integer)
    follower_count = Column(Integer)
    creator_traders = Column(JSONB, nullable=False)
    next_loan_cached = Column(Numeric)
    resolved_profit_adjustment = Column(Numeric)
    balance = Column(Numeric, nullable=False)
    cash_balance = Column(Numeric)
    spice_balance = Column(Numeric)
    total_deposits = Column(Numeric, nullable=False)
    total_cash_deposits = Column(Numeric)
    url = Column(Text)
    collected_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class BetClean(Base):
    __tablename__ = "bets_clean"
    __table_args__ = (
        Index("idx_bets_clean_user", "user_id"),
        Index("idx_bets_clean_contract", "contract_id"),
    )

    id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False)
    contract_id = Column(String, nullable=False)
    answer_id = Column(String)
    created_time = Column(DateTime(timezone=True), nullable=False)
    updated_time = Column(DateTime(timezone=True))
    amount = Column(Numeric, nullable=False)
    loan_amount = Column(Numeric)
    outcome = Column(String, nullable=False)
    shares = Column(Numeric, nullable=False)
    prob_before = Column(Numeric, nullable=False)
    prob_after = Column(Numeric, nullable=False)
    liquidity_fee = Column(Numeric)
    creator_fee = Column(Numeric)
    platform_fee = Column(Numeric)
    is_api = Column(Boolean)
    is_redemption = Column(Boolean, nullable=False)
    challenge_slug = Column(String)
    reply_to_comment_id = Column(String)
    bet_group_id = Column(String)
    limit_prob = Column(Numeric)
    is_cancelled = Column(Boolean)
    order_amount = Column(Numeric)
    is_filled = Column(Boolean)
    expires_at = Column(DateTime(timezone=True))
    collected_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class ContractClean(Base):
    __tablename__ = "contracts_clean"
    __table_args__ = (
        Index("idx_contracts_clean_creator", "creator_id"),
        Index("idx_contracts_clean_created_time", text("created_time DESC")),
        Index("idx_contracts_clean_is_resolved", "is_resolved"),
        Index(
            "idx_contracts_clean_group_slugs",
            "group_slugs",
            postgresql_using="gin",
        ),
    )

    id = Column(String, primary_key=True)
    slug = Column(String, unique=True)
    creator_id = Column(String, nullable=False)
    question = Column(Text, nullable=False)
    description = Column(Text)
    visibility = Column(String)
    token = Column(String)
    outcome_type = Column(String)
    mechanism = Column(String)
    volume = Column(Numeric)
    unique_bettor_count = Column(Integer)
    view_count = Column(Numeric)
    popularity_score = Column(Numeric)
    is_resolved = Column(Boolean)
    resolution = Column(String)
    resolution_probability = Column(Numeric)
    resolution_time = Column(DateTime(timezone=True))
    group_slugs = Column(ARRAY(Text))
    created_time = Column(DateTime(timezone=True), nullable=False)
    close_time = Column(DateTime(timezone=True))
    last_bet_time = Column(DateTime(timezone=True))
    last_comment_time = Column(DateTime(timezone=True))
    is_love = Column(Boolean)
    featured_label = Column(String)
    is_ranked = Column(Boolean)
    creator_username = Column(String)
    creator_name = Column(String)
    collected_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class CommentClean(Base):
    __tablename__ = "comments_clean"
    __table_args__ = (
        Index("idx_comments_clean_user", "user_id"),
        Index("idx_comments_clean_contract", "contract_id"),
        Index("idx_comments_clean_created_time", text("created_time DESC")),
    )

    id = Column(String, primary_key=True)
    comment_type = Column(String, nullable=False)
    user_id = Column(String, nullable=False)
    contract_id = Column(String)
    reply_to_comment_id = Column(String)
    created_time = Column(DateTime(timezone=True), nullable=False)
    content = Column(JSONB, nullable=False)
    text = Column(Text)
    likes = Column(Integer)
    dislikes = Column(Integer)
    hidden = Column(Boolean)
    pinned = Column(Boolean)
    deleted = Column(Boolean)
    edited_time = Column(DateTime(timezone=True))
    is_api = Column(Boolean)
    user_username = Column(String, nullable=False)
    user_name = Column(String, nullable=False)
    user_avatar_url = Column(Text)
    bet_id = Column(String)
    bettor_id = Column(String)
    bettor_username = Column(String)
    bet_amount = Column(Numeric)
    bet_outcome = Column(String)
    visibility = Column(String)
    bounty_awarded = Column(Numeric)
    collected_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


CLEAN_MODELS: Dict[str, Type[Base]] = {
    "users_clean": UserClean,
    "bets_clean": BetClean,
    "contracts_clean": ContractClean,
    "comments_clean": CommentClean,
}

CLEAN_TABLES = {name: model.__table__ for name, model in CLEAN_MODELS.items()}


class PostgresLoader:
    """Load and upsert Manifold data into PostgreSQL."""

    def __init__(self, uri: str, *, echo: bool = False) -> None:
        self.engine: Engine = create_engine(uri, echo=echo, future=True)
        logger.debug("Connected to Postgres at %s", uri)

    def upsert_clean(self, table_name: str, records: Sequence[Mapping]) -> int:
        if not records:
            return 0
        table = CLEAN_TABLES[table_name]
        stmt = insert(table).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=[table.c.id],
            set_={
                col.name: getattr(stmt.excluded, col.name)
                for col in table.columns
                if col.name != "id"
            },
        )
        with self.engine.begin() as conn:
            result = conn.execute(stmt)
        rowcount = result.rowcount or len(records)
        logger.info("Upserted %s rows into %s", rowcount, table_name)
        return rowcount

    def fetch_user_ids(self, *, limit: int | None = None) -> List[str]:
        table = CLEAN_TABLES["users_clean"]
        stmt = select(table.c.id).order_by(table.c.id)
        if limit:
            stmt = stmt.limit(limit)
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).scalars().all()
        return list(rows)

    def fetch_top_users_by_bet_count(
        self, *, limit: int
    ) -> List[Mapping[str, object]]:
        """Return top users ordered by bet count for downstream stages."""
        if limit <= 0:
            return []

        users = CLEAN_TABLES["users_clean"]
        bets = CLEAN_TABLES["bets_clean"]

        bet_count = func.count(bets.c.id)
        stmt = (
            select(
                users.c.id.label("user_id"),
                users.c.username,
                func.date_part("year", users.c.created_time).label("join_year"),
                bet_count.label("bet_count"),
            )
            .join(bets, bets.c.user_id == users.c.id)
            .group_by(users.c.id, users.c.username, users.c.created_time)
            .order_by(bet_count.desc(), users.c.id)
            .limit(limit)
        )

        with self.engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()

        return rows

    def get_column_count(self, table_name: str) -> int:
        """Return number of columns for the given cleaned table."""
        table = CLEAN_TABLES[table_name]
        return len(table.columns)

    def stream_user_chunks(
        self,
        chunk_size: int,
        *,
        start_username: Optional[str] = None,
    ) -> Iterator[List[Tuple[str, str]]]:
        """Yield chunks of (user_id, username) tuples, optionally resuming by username."""
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")

        table = CLEAN_TABLES["users_clean"]
        stmt = select(table.c.id, table.c.username).order_by(table.c.id)

        with self.engine.connect() as conn:
            effective_stmt = stmt
            if start_username:
                start_id_stmt = (
                    select(table.c.id)
                    .where(table.c.username == start_username)
                    .order_by(table.c.id)
                    .limit(1)
                )
                start_id = conn.execute(start_id_stmt).scalar_one_or_none()
                if start_id is not None:
                    logger.info(
                        "Resuming bet ingestion from user_id %s (username %s)",
                        start_id,
                        start_username,
                    )
                    effective_stmt = effective_stmt.where(table.c.id >= start_id)
                else:
                    logger.warning(
                        "Start username %s not found; resuming from next username alphabetically",
                        start_username,
                    )
                    effective_stmt = effective_stmt.where(
                        table.c.username > start_username
                    )

            result = conn.execution_options(stream_results=True).execute(effective_stmt)
            batch: List[Tuple[str, str]] = []
            for row in result:
                batch.append((row.id, row.username))
                if len(batch) >= chunk_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

    def ensure_schema(self) -> None:
        """Create tables if they do not exist."""
        Base.metadata.create_all(self.engine, checkfirst=True)

    def execute_sql(self, sql: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(sql))
