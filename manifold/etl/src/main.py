"""ETL orchestrator for Manifold Markets user data."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

from dotenv import load_dotenv

from .extract import users as users_extract
from .load.postgres import PostgresLoader
from .transform.normalize import normalize_user, prepare_records
from .utils.manifold import ManifoldClient

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 200


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manifold user ETL pipeline")
    parser.add_argument("--user-limit", type=int, help="Max number of users to ingest")
    parser.add_argument(
        "--user-page-size", type=int, default=500, help="Page size when fetching users"
    )
    parser.add_argument(
        "--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ...)"
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="Number of list results to accumulate before writing to the database",
    )
    return parser.parse_args()


def configure_logging(log_dir: Path, level: str) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"etl_{datetime.now(timezone.utc):%Y%m%d}.log"

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, mode="a", encoding="utf-8"),
        ],
    )

    logger.info("Logging to %s", log_path)


def _chunked(iterable: Iterable, size: int) -> Iterable[List]:
    chunk: List = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def run_users_stage(
    client: ManifoldClient,
    loader: PostgresLoader,
    *,
    limit: int | None,
    page_size: int,
    chunk_size: int,
) -> None:
    logger.info("Starting user ingestion")
    total = 0
    for batch in _chunked(users_extract.stream_users(client, page_size=page_size), chunk_size):
        if limit is not None and total >= limit:
            break

        if limit is not None and total + len(batch) > limit:
            batch = batch[: limit - total]

        collected_at = datetime.now(timezone.utc)
        raw_records, clean_records = prepare_records(
            batch, normalize_user, collected_at=collected_at
        )
        loader.upsert_raw("users_raw", raw_records)
        loader.upsert_clean("users_clean", clean_records)

        total += len(batch)
        logger.info("Ingested %s users (cumulative)", total)

    logger.info("User ingestion completed (%s records)", total)


def main() -> None:
    load_dotenv()

    args = parse_args()

    api_key = os.getenv("MANIFOLD_API_KEY")
    postgres_uri = os.getenv("POSTGRES_URI")
    log_dir = Path(os.getenv("LOG_DIR", "logs"))

    if not postgres_uri:
        raise SystemExit("POSTGRES_URI must be configured (see .env)")

    configure_logging(log_dir, args.log_level)

    loader = PostgresLoader(postgres_uri)
    loader.ensure_schema()

    with ManifoldClient(api_key=api_key) as client:
        run_users_stage(
            client,
            loader,
            limit=args.user_limit,
            page_size=args.user_page_size,
            chunk_size=args.chunk_size,
        )


if __name__ == "__main__":
    main()
