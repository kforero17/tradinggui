"""Signal history: label each recommendation NEW or STILL_VALID against the previous run.

A signal is identified by its ``signal_key`` (``ticker|direction|ENTRY`` for
entries, ``account|ticker|direction|EXIT|ADD|HOLD`` for positions). A key seen
in the previous run is STILL_VALID and keeps that run's ``first_seen_at``,
which is also the key of its journal decision. A previous non-HOLD key that is
gone now has EXPIRED.

HOLD rows are routine, so they are never NEW-noisy: in the very first run
(no previous run) they are NEW like everything else, and after that a HOLD is
always STILL_VALID, carrying ``first_seen_at`` forward when the key was seen
before and starting it now otherwise. HOLD rows never expire.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from enum import Enum
from typing import Any

from .recommendations import ENTRY_ACTIONS, Recommendation, RecommendationAction

__all__ = [
    "SignalStatus",
    "DiffedRecommendation",
    "is_entry_row",
    "expired_signals",
    "diff_runs",
    "to_rows",
]

_ENTRY_ACTION_NAMES = frozenset(a.name for a in ENTRY_ACTIONS)
_HOLD = RecommendationAction.HOLD.name


class SignalStatus(str, Enum):
    NEW = "NEW"
    STILL_VALID = "STILL_VALID"


DiffedRecommendation = tuple[Recommendation, SignalStatus, datetime]


def is_entry_row(row: Mapping[str, Any]) -> bool:
    """A stored ENTER/WATCH row (as opposed to a position action)."""
    return str(row["action"]) in _ENTRY_ACTION_NAMES


def expired_signals(
    previous: Sequence[Mapping[str, Any]] | None,
    current_keys: Iterable[str],
    *,
    include_entries: bool = True,
) -> list[dict[str, Any]]:
    """Previous non-HOLD rows whose key is missing now.

    With ``include_entries=False`` (a run that didn't scan for entries) previous
    ENTER/WATCH rows are never expired, since their absence says nothing.
    """
    if not previous:
        return []
    current = set(current_keys)
    return [
        dict(row)
        for row in previous
        if row["action"] != _HOLD
        and row["signal_key"] not in current
        and (include_entries or not is_entry_row(row))
    ]


def diff_runs(
    previous: Sequence[Mapping[str, Any]] | None,
    current: Sequence[Recommendation],
    *,
    now: datetime,
    include_entries: bool = True,
) -> tuple[list[DiffedRecommendation], list[dict[str, Any]]]:
    """Label ``current`` against ``previous`` stored rows; also return the expired previous rows.

    ``previous`` is None for the first run ever. New signals get ``now`` as
    ``first_seen_at``. See the module docstring for HOLD handling.
    """
    seen: dict[str, datetime] = {}
    for row in previous or ():
        seen.setdefault(row["signal_key"], row["first_seen_at"])

    diffed: list[DiffedRecommendation] = []
    for rec in current:
        if rec.signal_key in seen:
            diffed.append((rec, SignalStatus.STILL_VALID, seen[rec.signal_key]))
        elif rec.action is RecommendationAction.HOLD and previous is not None:
            diffed.append((rec, SignalStatus.STILL_VALID, now))
        else:
            diffed.append((rec, SignalStatus.NEW, now))
    expired = expired_signals(
        previous, (r.signal_key for r in current), include_entries=include_entries
    )
    return diffed, expired


def _clean(value: Any) -> Any:
    """NaN/inf to None and tuples to lists, so stored JSON is valid for the browser."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {k: _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    if isinstance(value, Enum):
        return value.name
    return value


def to_rows(diffed: Iterable[DiffedRecommendation]) -> list[dict[str, Any]]:
    """``db.create_recommendation_run`` row dicts, NaN replaced by None."""
    return [
        {
            "signal_key": rec.signal_key,
            "ticker": rec.ticker,
            "account": rec.account,
            "direction": rec.direction.name,
            "action": rec.action.name,
            "status": status.value,
            "first_seen_at": first_seen_at,
            "trigger_price": _clean(rec.trigger_price),
            "stop_price": _clean(rec.stop_price),
            "shares": rec.shares,
            "risk_dollars": _clean(rec.risk_dollars),
            "risk_pct_equity": _clean(rec.risk_pct_equity),
            "distance_n": _clean(rec.distance_n),
            "trend_score": _clean(rec.trend_score),
            "rs_rank": _clean(rec.rs_rank),
            "asset_type": rec.asset_type,
            "is_late": rec.is_late,
            "reasons": list(rec.reasons),
            "blocked_by": list(rec.blocked_by),
            "extra": _clean(rec.extra),
        }
        for rec, status, first_seen_at in diffed
    ]
