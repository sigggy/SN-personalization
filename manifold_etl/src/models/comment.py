"""Typed representations of Manifold comment records."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


def _convert_ms(value: Optional[int | float | datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _ensure_dict(value: Any, field: str) -> Dict:
    if value is None:
        raise ValueError(f"Missing required field '{field}'")
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Field '{field}' must be valid JSON") from exc
        if isinstance(parsed, dict):
            return parsed
        raise ValueError(f"Field '{field}' JSON must decode to object")
    raise ValueError(f"Field '{field}' must be a JSON object")


def _build_doc_from_text(text_value: Optional[str]) -> Dict:
    text_str = (text_value or "").strip()
    if not text_str:
        return {"type": "doc", "content": []}
    return {
        "type": "doc",
        "content": [
            {
                "type": "paragraph",
                "content": [
                    {
                        "type": "text",
                        "text": text_str,
                    }
                ],
            }
        ],
    }


class CommentClean(BaseModel):
    """Clean representation of a comment ready for database insertion."""

    id: str
    comment_type: str = Field(alias="commentType")
    user_id: str = Field(alias="userId")
    contract_id: Optional[str] = Field(default=None, alias="contractId")
    reply_to_comment_id: Optional[str] = Field(default=None, alias="replyToCommentId")
    created_time: datetime = Field(alias="createdTime")
    content: Dict
    text: Optional[str] = None
    likes: Optional[int] = None
    dislikes: Optional[int] = None
    hidden: Optional[bool] = None
    pinned: Optional[bool] = None
    deleted: Optional[bool] = None
    edited_time: Optional[datetime] = Field(default=None, alias="editedTime")
    is_api: Optional[bool] = Field(default=None, alias="isApi")
    user_username: str = Field(alias="userUsername")
    user_name: str = Field(alias="userName")
    user_avatar_url: Optional[str] = Field(default=None, alias="userAvatarUrl")
    bet_id: Optional[str] = Field(default=None, alias="betId")
    bettor_id: Optional[str] = Field(default=None, alias="bettorId")
    bettor_username: Optional[str] = Field(default=None, alias="bettorUsername")
    bet_amount: Optional[float] = Field(default=None, alias="betAmount")
    bet_outcome: Optional[str] = Field(default=None, alias="betOutcome")
    visibility: Optional[str] = None
    bounty_awarded: Optional[float] = Field(default=None, alias="bountyAwarded")
    collected_at: datetime

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True

    @classmethod
    def from_payload(cls, payload: dict, *, collected_at: datetime) -> "CommentClean":
        data = dict(payload)
        for field in ("createdTime", "editedTime"):
            if field in data:
                data[field] = _convert_ms(data.get(field))

        # Normalize legacy comments that only provide ``text``.
        raw_content = data.get("content")
        raw_text = data.get("text")
        if raw_content is None:
            data["content"] = _build_doc_from_text(raw_text)
        else:
            data["content"] = _ensure_dict(raw_content, "content")

        if raw_text is not None:
            data["text"] = str(raw_text)

        if data.get("betAmount") is not None:
            data["betAmount"] = float(data["betAmount"])
        if data.get("bountyAwarded") is not None:
            data["bountyAwarded"] = float(data["bountyAwarded"])

        data["collected_at"] = (
            collected_at if collected_at.tzinfo else collected_at.replace(tzinfo=timezone.utc)
        )

        return cls.model_validate(data)

    def to_db_dict(self) -> dict:
        return self.model_dump(by_alias=False)
