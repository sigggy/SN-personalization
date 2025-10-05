"""PostgreSQL loading utilities for the Manifold ETL pipeline."""

from __future__ import annotations

import logging
from typing import List, Mapping, Sequence

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


metadata = MetaData()

users_raw = Table(
    "users_raw",
    metadata,
    Column("id", String, primary_key=True),
    Column("json_data", JSONB, nullable=False),
    Column("collected_at", DateTime(timezone=True), nullable=False),
)

users_clean = Table(
    "users_clean",
    metadata,
    Column("id", String, primary_key=True),
    Column("username", String),
    Column("name", String),
    Column("created_time", DateTime(timezone=True)),
    Column("is_bot", Boolean),
    Column("is_banned", Boolean),
    Column("total_bets", Integer),
    Column("total_profit", Numeric),
    Column("total_volume", Numeric),
    Column("last_login", DateTime(timezone=True)),
    Column("collected_at", DateTime(timezone=True), nullable=False),
)

RAW_TABLES = {
    "users_raw": users_raw,
}

CLEAN_TABLES = {
    "users_clean": users_clean,
}


class PostgresLoader:
    """Load and upsert Manifold user data into PostgreSQL."""

    def __init__(self, uri: str, *, echo: bool = False) -> None:
        self.engine: Engine = create_engine(uri, echo=echo, future=True)
        logger.debug("Connected to Postgres at %s", uri)

    def upsert_raw(self, table_name: str, records: Sequence[Mapping]) -> int:
        if not records:
            return 0
        table = RAW_TABLES[table_name]
        stmt = insert(table).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=[table.c.id],
            set_={
                "json_data": stmt.excluded.json_data,
                "collected_at": stmt.excluded.collected_at,
            },
        )
        with self.engine.begin() as conn:
            result = conn.execute(stmt)
        rowcount = result.rowcount or len(records)
        logger.info("Upserted %s rows into %s", rowcount, table_name)
        return rowcount

    def upsert_clean(self, table_name: str, records: Sequence[Mapping]) -> int:
        if not records:
            return 0
        table = CLEAN_TABLES[table_name]
        stmt = insert(table).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=[table.c.id],
            set_={col.name: getattr(stmt.excluded, col.name) for col in table.columns if col.name != "id"},
        )
        with self.engine.begin() as conn:
            result = conn.execute(stmt)
        rowcount = result.rowcount or len(records)
        logger.info("Upserted %s rows into %s", rowcount, table_name)
        return rowcount

    def fetch_user_ids(self, *, limit: int | None = None) -> List[str]:
        stmt = select(users_clean.c.id).order_by(users_clean.c.id)
        if limit:
            stmt = stmt.limit(limit)
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).scalars().all()
        return list(rows)

    def ensure_schema(self) -> None:
        """Create tables if they do not exist."""
        metadata.create_all(self.engine, checkfirst=True)

    def execute_sql(self, sql: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(sql))
