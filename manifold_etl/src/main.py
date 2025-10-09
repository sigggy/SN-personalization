"""ETL orchestrator for Manifold Markets data."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from dotenv import load_dotenv

import config

from .extract import bets as bets_extract
from .extract import users as users_extract
from .load.postgres import PostgresLoader
from .transform.normalize import normalize_bet, normalize_user, prepare_records
from .utils.manifold import ManifoldClient

logger = logging.getLogger(__name__)



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manifold ETL pipeline")
    parser.add_argument(
        "--users-only",
        action="store_true",
        help="Run only the user ingestion stage",
    )
    parser.add_argument(
        "--bets-only",
        action="store_true",
        help="Run only the bet ingestion stage",
    )
    parser.add_argument(
        "--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ...)"
    )
    parser.add_argument(
        "--bet-start-username",
        help="Resume bet ingestion starting from this username",
    )
    return parser.parse_args()


def configure_logging(log_dir: Path, level: str) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"etl_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.log"

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


def run_users_stage(client: ManifoldClient, loader: PostgresLoader) -> None:
    logger.info("Starting user ingestion")
    limit = config.USER_LIMIT
    page_size = config.USER_PAGE_SIZE
    chunk_size = config.CHUNK_SIZE

    total = 0
    for batch in _chunked(users_extract.stream_users(client, page_size=page_size), chunk_size):
        if limit is not None and total >= limit:
            break

        if limit is not None and total + len(batch) > limit:
            batch = batch[: limit - total]

        collected_at = datetime.now(timezone.utc)
        clean_records = prepare_records(
            batch, normalize_user, collected_at=collected_at
        )
        loader.upsert_clean("users_clean", clean_records)

        total += len(batch)
        logger.info("Ingested %s users (cumulative)", total)

    logger.info("User ingestion completed (%s records)", total)


def run_bets_stage(
    client: ManifoldClient,
    loader: PostgresLoader,
    *,
    start_username: Optional[str] = None,
) -> None:
    logger.info("Starting bet ingestion")
    chunk_size = config.BET_USER_CHUNK_SIZE
    total_bets = 0
    if start_username:
        logger.info("Resuming bet ingestion at or after username '%s'", start_username)

    for user_chunk in loader.stream_user_chunks(
        chunk_size, start_username=start_username
    ):
        bet_payloads, processed_users, processed_usernames = bets_extract.process_bet_chunk(
            client,
            user_chunk,
            worker_count=config.BET_WORKER_COUNT,
        )
        if not bet_payloads:
            continue

        collected_at = datetime.now(timezone.utc)
        clean_records = [
            normalize_bet(payload, collected_at) for payload in bet_payloads
        ]
        loader.upsert_clean("bets_clean", clean_records)

        total_bets += len(clean_records)
        logger.info(
            "Processed %s qualifying users (%s); upserted %s bets this batch (cumulative bets: %s)",
            processed_users,
            ", ".join(processed_usernames),
            len(clean_records),
            total_bets,
        )

    logger.info("Bet ingestion completed (%s total bets)", total_bets)




def run_stages(args: argparse.Namespace, loader: PostgresLoader) -> None:
    if args.users_only and args.bets_only:
        raise SystemExit("Use either --users-only or --bets-only, not both.")

    run_users = not args.bets_only
    run_bets = not args.users_only

    client_kwargs = {
        "max_retries": config.API_MAX_RETRIES,
        "backoff_factor": config.API_BACKOFF_FACTOR,
        "timeout": config.API_TIMEOUT,
    }

    if run_users:
        with ManifoldClient(**client_kwargs) as client:
            run_users_stage(
                client,
                loader,
            )

    if run_bets:
        with ManifoldClient(**client_kwargs) as client:
            run_bets_stage(
                client,
                loader,
                start_username=args.bet_start_username,
            )


def main() -> None:
    load_dotenv()

    args = parse_args()

    postgres_uri = os.getenv("POSTGRES_URI")
    log_dir = Path(config.LOG_DIR)

    if not postgres_uri:
        raise SystemExit("POSTGRES_URI must be configured (see .env)")

    configure_logging(log_dir, args.log_level)

    loader = PostgresLoader(postgres_uri)
    loader.ensure_schema()

    run_stages(args, loader)


if __name__ == "__main__":
    main()
