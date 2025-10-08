from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class Fees(BaseModel):
    liquidityFee: Optional[float] = None
    creatorFee: Optional[float] = None
    platformFee: Optional[float] = None


class Bet(BaseModel):
    id: str
    userId: str
    contractId: str

    answerId: Optional[str] = None
    createdTime: datetime = Field(..., alias="createdTime")
    updatedTime: Optional[datetime] = Field(None, alias="updatedTime")

    amount: float
    loanAmount: Optional[float] = None
    outcome: str
    shares: float

    probBefore: float
    probAfter: float
    fees: Fees

    isApi: Optional[bool] = None
    isRedemption: bool

    challengeSlug: Optional[str] = None
    replyToCommentId: Optional[str] = None
    betGroupId: Optional[str] = None

    # Partial<LimitProps> fields
    limitProb: Optional[float] = None
    isCancelled: Optional[bool] = None
    orderAmount: Optional[float] = None
    isFilled: Optional[bool] = None
    expiresAt: Optional[datetime] = None

    @classmethod
    def from_payload(cls, data: dict) -> "Bet":
        """
        Create a Bet instance from raw Manifold API JSON,
        converting millisecond timestamps into datetime objects.
        """
        # Convert millisecond timestamps → datetime
        for key in ("createdTime", "updatedTime", "expiresAt"):
            if key in data and isinstance(data[key], (int, float)):
                data[key] = datetime.fromtimestamp(data[key] / 1000, tz=timezone.utc)

        # Convert nested fees object if missing
        if "fees" not in data:
            data["fees"] = {}

        # Create validated Pydantic model
        return cls.model_validate(data)


class BetClean(BaseModel):
    """Clean representation of a bet ready for database insertion."""

    id: str
    user_id: str
    contract_id: str
    answer_id: Optional[str] = None
    created_time: datetime
    updated_time: Optional[datetime] = None
    amount: float
    loan_amount: Optional[float] = None
    outcome: str
    shares: float
    prob_before: float
    prob_after: float
    liquidity_fee: Optional[float] = None
    creator_fee: Optional[float] = None
    platform_fee: Optional[float] = None
    is_api: Optional[bool] = None
    is_redemption: bool
    challenge_slug: Optional[str] = None
    reply_to_comment_id: Optional[str] = None
    bet_group_id: Optional[str] = None
    limit_prob: Optional[float] = None
    is_cancelled: Optional[bool] = None
    order_amount: Optional[float] = None
    is_filled: Optional[bool] = None
    expires_at: Optional[datetime] = None
    collected_at: datetime

    @classmethod
    def from_bet(cls, bet: Bet, *, collected_at: datetime) -> "BetClean":
        """Instantiate ``BetClean`` from an already-normalized ``Bet`` model."""

        def _ensure_tz(dt: Optional[datetime]) -> Optional[datetime]:
            if dt is None:
                return None
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

        return cls(
            id=bet.id,
            user_id=bet.userId,
            contract_id=bet.contractId,
            answer_id=bet.answerId,
            created_time=_ensure_tz(bet.createdTime), # type: ignore
            updated_time=_ensure_tz(bet.updatedTime),
            amount=bet.amount,
            loan_amount=bet.loanAmount,
            outcome=bet.outcome,
            shares=bet.shares,
            prob_before=bet.probBefore,
            prob_after=bet.probAfter,
            liquidity_fee=bet.fees.liquidityFee,
            creator_fee=bet.fees.creatorFee,
            platform_fee=bet.fees.platformFee,
            is_api=bet.isApi,
            is_redemption=bet.isRedemption,
            challenge_slug=bet.challengeSlug,
            reply_to_comment_id=bet.replyToCommentId,
            bet_group_id=bet.betGroupId,
            limit_prob=bet.limitProb,
            is_cancelled=bet.isCancelled,
            order_amount=bet.orderAmount,
            is_filled=bet.isFilled,
            expires_at=_ensure_tz(bet.expiresAt),
            collected_at=_ensure_tz(collected_at), # type: ignore
        )

    def to_db_dict(self) -> dict:
        return self.model_dump()
