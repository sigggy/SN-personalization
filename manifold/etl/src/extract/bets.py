from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, List, Sequence, Tuple
from urllib.parse import urlparse

import requests

import config
from src.models.bet import Bet
from src.utils.manifold import ManifoldClient


LIMIT = config.LIMIT
THRESHOLD = config.THRESHOLD
API_URL = config.API_URL

BET_ENDPOINT = urlparse(API_URL).path.strip("/").split("/")[-1] or "bets"


def _response_message(resp: requests.Response) -> str:
    try:
        payload = resp.json()
        if isinstance(payload, dict) and "message" in payload:
            return str(payload["message"])
    except ValueError:
        pass
    return resp.text


def quick_check(
    client: ManifoldClient,
    user_id: str,
    username: str,
    threshold: int = THRESHOLD,
) -> bool:
    """Return True if user likely has >= threshold bets."""
    try:
        data = client.get_json(
            BET_ENDPOINT,
            params={"userId": user_id, "limit": threshold + 1},
        )
    except requests.HTTPError as exc:
        resp = exc.response
        if resp is not None and resp.status_code == 404:
            print(f"[Username: {username}] user not found ({_response_message(resp)})")
            return False
        message = _response_message(resp) if resp is not None else str(exc)
        print(f"Error fetching bets for {user_id}: {message}")
        return False
    except Exception as exc:  # pragma: no cover - network failures
        print(f"Error fetching bets for {user_id}: {exc}")
        return False
    print(
        f"[User Bets {len(data)} vs Threshold {THRESHOLD}] [Username: {username}]",
        end=" ",
    )
    return len(data) >= threshold


def _fetch_bet_pages(client: ManifoldClient, user_id: str) -> Iterable[list]:
    before = None
    while True:
        params = {"userId": user_id, "limit": LIMIT}
        if before:
            params["before"] = before
        try:
            data = client.get_json(BET_ENDPOINT, params=params)
        except requests.HTTPError as exc:
            resp = exc.response
            if resp is not None and resp.status_code == 404:
                print(f"User {user_id} not found while fetching bets ({_response_message(resp)})")
                break
            message = _response_message(resp) if resp is not None else str(exc)
            print(f"Error fetching bets for {user_id}: {message}")
            break
        except Exception as exc:  # pragma: no cover - network failures
            print(f"Error fetching bets for {user_id}: {exc}")
            break
        if not data:
            break
        yield data
        if len(data) < LIMIT:
            break
        before = data[-1].get("id")
        if not before:
            break


def fetch_all_bet_payloads(client: ManifoldClient, user_id: str) -> List[dict]:
    """Fetch and return all bet payloads for a user."""
    payloads: List[dict] = []
    for page in _fetch_bet_pages(client, user_id):
        payloads.extend(page)
    return payloads


def _process_users(
    client: ManifoldClient,
    user_chunk: Sequence[Tuple[str, str]],
) -> Tuple[List[dict], int]:
    payloads: List[dict] = []
    processed_users = 0

    for user_id, username in user_chunk:
        try:
            if not quick_check(client, user_id, username):
                print("below threshold ❌ — skipped")
                continue

            print("qualifies ✅ — fetching full bet history")
            bet_payloads = fetch_all_bet_payloads(client, user_id)
            if not bet_payloads:
                continue

            payloads.extend(bet_payloads)
            processed_users += 1
        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"ERROR: {exc}")

    return payloads, processed_users


def process_bet_chunk(
    client: ManifoldClient,
    user_chunk: Sequence[Tuple[str, str]],
    *,
    worker_count: int = 1,
) -> Tuple[List[dict], int]:
    """Fetch bet payloads for the provided user chunk.

    Returns a tuple of (bet_payloads, qualifying_user_count).
    """
    user_list = list(user_chunk)
    worker_count = max(1, worker_count)

    if worker_count == 1 or len(user_list) <= 1:
        payloads, processed_users = _process_users(client, user_list)
    else:
        payloads: List[dict] = []
        processed_users = 0
        sub_chunk_size = max(1, (len(user_list) + worker_count - 1) // worker_count)
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [
                executor.submit(_process_users, client, user_list[i : i + sub_chunk_size])
                for i in range(0, len(user_list), sub_chunk_size)
            ]

            for future in as_completed(futures):
                batch_payloads, batch_processed = future.result()
                if batch_payloads:
                    payloads.extend(batch_payloads)
                processed_users += batch_processed

    if payloads:
        print(f"\nCollected {len(payloads)} bet payloads from active users.")
    return payloads, processed_users
