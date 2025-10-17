"""ETL orchestrator for Manifold Markets data."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Mapping, Optional

from dotenv import load_dotenv
from sqlalchemy.exc import ProgrammingError

import config

from .extract import bets as bets_extract
from .extract import comments as comments_extract
from .extract import contracts as contracts_extract
from .extract import users as users_extract
from .load.postgres import PostgresLoader
from .transform.normalize import (
    normalize_bet,
    normalize_comment,
    normalize_contract,
    normalize_user,
    prepare_records,
)
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
        "--contracts-only",
        action="store_true",
        help="Run only the contract update stage",
    )
    parser.add_argument(
        "--comments-only",
        action="store_true",
        help="Run only the comment update stage",
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
    total_scanned_users = 0
    total_qualifying_users = 0
    total_failed_bets = 0

    max_params_per_statement = getattr(config, "BET_MAX_PARAMS_PER_STATEMENT", None)
    bet_column_count = loader.get_column_count("bets_clean")
    if (
        max_params_per_statement is not None
        and max_params_per_statement > 0
        and bet_column_count
    ):
        max_records_per_upsert = max(1, max_params_per_statement // bet_column_count)
    else:
        max_records_per_upsert = None

    if start_username:
        logger.info("Resuming bet ingestion at or after username '%s'", start_username)

    for user_chunk in loader.stream_user_chunks(
        chunk_size, start_username=start_username
    ):
        total_scanned_users += len(user_chunk)
        bet_payloads, processed_users, processed_usernames = bets_extract.process_bet_chunk(
            client,
            user_chunk,
            worker_count=config.BET_WORKER_COUNT,
        )
        total_qualifying_users += processed_users
        if not bet_payloads:
            logger.info(
                (
                    "Processed %s qualifying users this batch; "
                    "cumulative qualifying users: %s; total users scanned: %s; "
                    "no bets upserted this batch (cumulative bets: %s)"
                ),
                processed_users,
                total_qualifying_users,
                total_scanned_users,
                total_bets,
            )
            continue

        collected_at = datetime.now(timezone.utc)
        clean_records = [
            normalize_bet(payload, collected_at) for payload in bet_payloads
        ]
        if (
            max_records_per_upsert
            and len(clean_records) > max_records_per_upsert
        ):
            pending_batches = list(_chunked(clean_records, max_records_per_upsert))
        else:
            pending_batches = [clean_records]

        bets_upserted_this_batch = 0
        failed_bets_this_batch = 0

        while pending_batches:
            record_batch = pending_batches.pop()
            try:
                loader.upsert_clean("bets_clean", record_batch)
            except ProgrammingError as exc:
                if len(record_batch) == 1:
                    failed_bet_id = record_batch[0].get("id")
                    logger.exception(
                        "Failed to upsert bet %s due to ProgrammingError; skipping.",
                        failed_bet_id,
                    )
                    failed_bets_this_batch += 1
                    total_failed_bets += 1
                    continue

                split_point = max(1, len(record_batch) // 2)
                first_half = record_batch[:split_point]
                second_half = record_batch[split_point:]

                logger.warning(
                    "ProgrammingError for %s bets; retrying as batches of %s and %s. Error: %s",
                    len(record_batch),
                    len(first_half),
                    len(second_half),
                    exc,
                )

                if second_half:
                    pending_batches.append(second_half)
                if first_half:
                    pending_batches.append(first_half)
            else:
                bets_upserted_this_batch += len(record_batch)

        total_bets += bets_upserted_this_batch
        processed_usernames_display = (
            ", ".join(processed_usernames) if processed_usernames else "n/a"
        )
        logger.info(
            (
                "Processed %s qualifying users this batch (%s); "
                "cumulative qualifying users: %s; total users scanned: %s; "
                "upserted %s bets this batch (cumulative bets: %s); "
                "failed bet upserts this batch: %s (cumulative failed: %s)"
            ),
            processed_users,
            processed_usernames_display,
            total_qualifying_users,
            total_scanned_users,
            bets_upserted_this_batch,
            total_bets,
            failed_bets_this_batch,
            total_failed_bets,
        )

    logger.info("Bet ingestion completed (%s total bets)", total_bets)


def _resolve_target_users(
    loader: PostgresLoader, *, limit: int, stage: str
) -> List[Mapping[str, object]]:
    if limit <= 0:
        logger.info("Configured %s user limit is non-positive; skipping stage", stage)
        return []

    users = loader.fetch_top_users_by_bet_count(limit=limit)
    if not users:
        logger.warning("No eligible users found for %s stage.", stage)
    else:
        logger.info(
            "Selected %s users for %s stage (limit=%s)",
            len(users),
            stage,
            limit,
        )
    return users


def run_contracts_stage(client: ManifoldClient, loader: PostgresLoader) -> None:
    logger.info("Starting contract update")
    limit = config.CONTRACT_USER_LIMIT
    target_users = _resolve_target_users(loader, limit=limit, stage="contract update")
    if not target_users:
        return

    total_contracts = 0
    for user in target_users:
        user_id = user["user_id"]
        username = user["username"]
        bet_count = user.get("bet_count")

        contracts = contracts_extract.fetch_contracts_for_user(client, user_id)
        if not contracts:
            logger.info("No contracts to update for user %s", username)
            continue

        collected_at = datetime.now(timezone.utc)
        clean_records = prepare_records(
            contracts, normalize_contract, collected_at=collected_at
        )
        if not clean_records:
            continue

        loader.upsert_clean("contracts_clean", clean_records)
        total_contracts += len(clean_records)
        logger.info(
            "Upserted %s contracts for user %s (bet_count=%s)",
            len(clean_records),
            username,
            bet_count,
        )

    logger.info(
        "Contract update completed (%s contracts across %s users)",
        total_contracts,
        len(target_users),
    )


def run_comments_stage(client: ManifoldClient, loader: PostgresLoader) -> None:
    logger.info("Starting comment update")
    limit = config.COMMENT_USER_LIMIT
    target_users = _resolve_target_users(loader, limit=limit, stage="comment update")
    if not target_users:
        return

    total_comments = 0
    for user in target_users:
        user_id = user["user_id"]
        username = user["username"]
        bet_count = user.get("bet_count")

        comments = comments_extract.fetch_comments_for_user(client, user_id)
        if not comments:
            logger.info("No comments to update for user %s", username)
            continue

        collected_at = datetime.now(timezone.utc)
        clean_records = prepare_records(
            comments, normalize_comment, collected_at=collected_at
        )
        if not clean_records:
            continue

        loader.upsert_clean("comments_clean", clean_records)
        total_comments += len(clean_records)
        logger.info(
            "Upserted %s comments for user %s (bet_count=%s)",
            len(clean_records),
            username,
            bet_count,
        )

    logger.info(
        "Comment update completed (%s comments across %s users)",
        total_comments,
        len(target_users),
    )


def run_stages(args: argparse.Namespace, loader: PostgresLoader) -> None:
    exclusive_flags = {
        "users": args.users_only,
        "bets": args.bets_only,
        "contracts": args.contracts_only,
        "comments": args.comments_only,
    }
    enabled = [name for name, flag in exclusive_flags.items() if flag]
    if len(enabled) > 1:
        raise SystemExit(
            "Use at most one of --users-only, --bets-only, --contracts-only, or --comments-only."
        )

    run_users = run_bets = run_contracts = run_comments = True
    if enabled:
        selected = enabled[0]
        run_users = selected == "users"
        run_bets = selected == "bets"
        run_contracts = selected == "contracts"
        run_comments = selected == "comments"

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

    if run_contracts:
        with ManifoldClient(**client_kwargs) as client:
            run_contracts_stage(client, loader)

    if run_comments:
        with ManifoldClient(**client_kwargs) as client:
            run_comments_stage(client, loader)


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
