"""Typed representations of Manifold user records."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class UserClean(BaseModel):
    id: str
    username: str
    name: str
    avatar_url: Optional[str] = Field(default=None, alias="avatarUrl")
    created_time: datetime = Field(alias="createdTime")
    balance: float
    total_deposits: float = Field(alias="totalDeposits")
    creator_traders: dict = Field(alias="creatorTraders")

    bio: Optional[str] = None
    website: Optional[str] = None
    banner_url: Optional[str] = Field(default=None, alias="bannerUrl")
    discord_handle: Optional[str] = Field(default=None, alias="discordHandle")
    twitter_handle: Optional[str] = Field(default=None, alias="twitterHandle")
    last_updated_time: Optional[datetime] = Field(default=None, alias="lastUpdatedTime")
    last_login: Optional[datetime] = Field(default=None, alias="lastBetTime")
    sweepstakes_verified_time: Optional[datetime] = Field(
        default=None, alias="sweepstakesVerifiedTime"
    )
    is_bot: bool = Field(default=False, alias="isBot")
    is_admin: bool = Field(default=False, alias="isAdmin")
    is_trustworthy: bool = Field(default=False, alias="isTrustworthy")
    is_banned: Optional[bool] = Field(default=None, alias="isBanned")
    is_banned_from_mana: Optional[bool] = Field(default=None, alias="isBannedFromMana")
    is_banned_from_sweepcash: Optional[bool] = Field(
        default=None, alias="isBannedFromSweepcash"
    )
    is_advanced_trader: Optional[bool] = Field(default=None, alias="isAdvancedTrader")
    id_verified: Optional[bool] = Field(default=None, alias="idVerified")
    verified_phone: Optional[bool] = Field(default=None, alias="verifiedPhone")
    sweepstakes_verified: Optional[bool] = Field(default=None, alias="sweepstakesVerified")
    kyc_status: Optional[str] = Field(default=None, alias="kycDocumentStatus")
    referred_by_user_id: Optional[str] = Field(default=None, alias="referredByUserId")
    total_profit: Optional[float] = Field(default=None, alias="profit_allTime")
    total_volume: Optional[float] = Field(default=None, alias="profit_sinceCreation")
    current_betting_streak: Optional[int] = Field(default=None, alias="currentBettingStreak")
    follower_count: Optional[int] = Field(default=None, alias="followerCountCached")
    next_loan_cached: Optional[float] = Field(default=None, alias="nextLoanCached")
    resolved_profit_adjustment: Optional[float] = Field(
        default=None, alias="resolvedProfitAdjustment"
    )
    cash_balance: Optional[float] = Field(default=None, alias="cashBalance")
    spice_balance: Optional[float] = Field(default=None, alias="spiceBalance")
    total_cash_deposits: Optional[float] = Field(default=None, alias="totalCashDeposits")
    url: Optional[str] = Field(default=None, alias="url")
    collected_at: datetime

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True

    @classmethod
    def from_payload(cls, user: dict, *, collected_at: datetime) -> "UserClean":
        profit_cached = user.get("profitCached") or {}
        creator_traders = _require_dict(user.get("creatorTraders"), "creatorTraders")
        data = {
            "id": _require_string(user.get("id"), "id"),
            "username": _require_string(user.get("username"), "username"),
            "name": _require_string(user.get("name") or user.get("displayName"), "name"),
            "avatarUrl": _normalize_string(user.get("avatarUrl")),
            "createdTime": _require_datetime(user.get("createdTime"), "createdTime"),
            "balance": float(_require_number(user.get("balance"), "balance")),
            "totalDeposits": float(_require_number(user.get("totalDeposits"), "totalDeposits")),
            "creatorTraders": creator_traders,
            "bio": _normalize_string(user.get("bio")),
            "website": _normalize_string(user.get("website")),
            "bannerUrl": _normalize_string(user.get("bannerUrl")),
            "discordHandle": _normalize_string(user.get("discordHandle")),
            "twitterHandle": _normalize_string(user.get("twitterHandle")),
            "lastUpdatedTime": _convert_ms(user.get("lastUpdatedTime")),
            "lastBetTime": _convert_ms(user.get("lastBetTime")),
            "sweepstakesVerifiedTime": _convert_ms(user.get("sweepstakesVerifiedTime")),
            "isBot": _normalize_bool(user.get("isBot"), default=False),
            "isAdmin": _normalize_bool(user.get("isAdmin"), default=False),
            "isTrustworthy": _normalize_bool(user.get("isTrustworthy"), default=False),
            "isBanned": _normalize_bool(user.get("isBanned")),
            "isBannedFromMana": _normalize_bool(user.get("isBannedFromMana")),
            "isBannedFromSweepcash": _normalize_bool(user.get("isBannedFromSweepcash")),
            "isAdvancedTrader": _normalize_bool(user.get("isAdvancedTrader")),
            "idVerified": _normalize_bool(user.get("idVerified")),
            "verifiedPhone": _normalize_bool(user.get("verifiedPhone")),
            "sweepstakesVerified": _normalize_bool(user.get("sweepstakesVerified")),
            "kycDocumentStatus": _normalize_string(user.get("kycDocumentStatus")),
            "referredByUserId": _normalize_string(user.get("referredByUserId")),
            "profit_allTime": profit_cached.get("allTime"),
            "profit_sinceCreation": profit_cached.get("sinceCreation"),
            "currentBettingStreak": user.get("currentBettingStreak"),
            "followerCountCached": user.get("followerCountCached"),
            "nextLoanCached": user.get("nextLoanCached"),
            "resolvedProfitAdjustment": user.get("resolvedProfitAdjustment"),
            "cashBalance": user.get("cashBalance"),
            "spiceBalance": user.get("spiceBalance"),
            "totalCashDeposits": user.get("totalCashDeposits"),
            "url": _normalize_string(user.get("url")),
            "collected_at": collected_at,
        }
        return cls(**data)

    def to_db_dict(self) -> dict:
        return self.model_dump(by_alias=False)


def _require_string(value, field: str) -> str:
    if value is None:
        raise ValueError(f"Missing required field '{field}'")
    value_str = str(value).strip()
    if not value_str:
        raise ValueError(f"Field '{field}' cannot be empty")
    return value_str


def _require_number(value, field: str) -> float:
    if value is None:
        raise ValueError(f"Missing required field '{field}'")
    return float(value)


def _require_dict(value, field: str) -> dict:
    if value is None:
        raise ValueError(f"Missing required dict field '{field}'")
    if not isinstance(value, dict):
        raise ValueError(f"Field '{field}' must be a dict")
    return value


def _require_datetime(value, field: str) -> datetime:
    converted = _convert_ms(value)
    if converted is None:
        raise ValueError(f"Missing required timestamp field '{field}'")
    return converted


def _convert_ms(value: Optional[int | float]) -> Optional[datetime]:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _normalize_bool(value, *, default: Optional[bool] = None) -> Optional[bool]:
    if value is None:
        return default
    return bool(value)


def _normalize_string(value) -> Optional[str]:
    if value is None:
        return None
    value_str = str(value).strip()
    return value_str or None
