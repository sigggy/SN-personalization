"""Typed representations of Manifold contract (market) records."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from pydantic import BaseModel, Field


def _convert_ms(value: Optional[int | float | datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _ensure_group_slugs(value) -> Optional[List[str]]:
    if value is None:
        return None
    if isinstance(value, list):
        return [str(item) for item in value]
    # Some API variants return comma-separated strings; normalize.
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",") if item.strip()]
        return items or None
    raise ValueError(f"Expected list or string for groupSlugs, got {type(value)!r}")


class ContractClean(BaseModel):
    """Clean representation of a contract ready for database insertion."""

    id: str
    slug: Optional[str] = None
    creator_id: str = Field(alias="creatorId")
    question: str
    description: Optional[str] = None
    visibility: str
    token: str
    outcome_type: Optional[str] = Field(default=None, alias="outcomeType")
    mechanism: Optional[str] = None
    volume: Optional[float] = None
    unique_bettor_count: Optional[int] = Field(default=None, alias="uniqueBettorCount")
    view_count: Optional[float] = Field(default=None, alias="viewCount")
    popularity_score: Optional[float] = Field(default=None, alias="popularityScore")
    is_resolved: Optional[bool] = Field(default=None, alias="isResolved")
    resolution: Optional[str] = None
    resolution_probability: Optional[float] = Field(
        default=None, alias="resolutionProbability"
    )
    resolution_time: Optional[datetime] = Field(default=None, alias="resolutionTime")
    group_slugs: Optional[List[str]] = Field(default=None, alias="groupSlugs")
    created_time: datetime = Field(alias="createdTime")
    close_time: Optional[datetime] = Field(default=None, alias="closeTime")
    last_bet_time: Optional[datetime] = Field(default=None, alias="lastBetTime")
    last_comment_time: Optional[datetime] = Field(
        default=None, alias="lastCommentTime"
    )
    is_love: Optional[bool] = Field(default=None, alias="isLove")
    featured_label: Optional[str] = Field(default=None, alias="featuredLabel")
    is_ranked: Optional[bool] = Field(default=None, alias="isRanked")
    creator_username: Optional[str] = Field(default=None, alias="creatorUsername")
    creator_name: Optional[str] = Field(default=None, alias="creatorName")
    collected_at: datetime

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True

    @classmethod
    def from_payload(cls, payload: dict, *, collected_at: datetime) -> "ContractClean":
        data = dict(payload)
        for field in ("createdTime", "closeTime", "lastBetTime", "lastCommentTime", "resolutionTime"):
            if field in data:
                data[field] = _convert_ms(data.get(field))

        data["groupSlugs"] = _ensure_group_slugs(data.get("groupSlugs"))
        data["volume"] = float(data["volume"]) if data.get("volume") is not None else None
        if data.get("popularityScore") is not None:
            data["popularityScore"] = float(data["popularityScore"])
        if data.get("viewCount") is not None:
            data["viewCount"] = float(data["viewCount"])
        if data.get("resolutionProbability") is not None:
            data["resolutionProbability"] = float(data["resolutionProbability"])

        data["collected_at"] = (
            collected_at if collected_at.tzinfo else collected_at.replace(tzinfo=timezone.utc)
        )

        return cls.model_validate(data)

    def to_db_dict(self) -> dict:
        return self.model_dump(by_alias=False)
