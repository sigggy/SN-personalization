"""Domain models for the ETL pipeline."""

from .bet import Bet, BetClean
from .user import UserClean

__all__ = ["Bet", "BetClean", "UserClean"]
