"""Ranked trend-following actions: what to exit, add to, enter and watch.

Pure logic over results computed elsewhere: ``turtle.evaluate_position`` for
held positions and ``turtle.scan_breakouts`` for candidates. The service layer
(``recommendation_service``) loads prices, accounts and holdings and persists
the run.

Rules applied by ``build_recommendations``:

* Every held (account, ticker) position becomes one EXIT_STOP, EXIT_CHANNEL,
  ADD_UNIT or HOLD recommendation for that account.
* A triggered breakout becomes ENTER; an untriggered one within 1N becomes
  WATCH. Tickers held in either direction anywhere are left out (adds cover the
  same direction; the opposite direction is never recommended), and so are
  System 1 breakouts skipped after a winner.
* One risk budget across accounts: open risk plus each new unit's risk must
  stay within ``max_heat_pct`` of total equity, admitted in priority and rank
  order. The book's unit limits are checked with the new unit added, units
  aggregated by ticker across accounts. Over-budget or over-limit entries are
  kept, with the reason in ``blocked_by``.
* Shorts go only to ``allow_short`` accounts. Longs go to the first account
  with enough cash, Roth accounts first.
"""

from __future__ import annotations

import math
import re
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, NamedTuple

import pandas as pd

from .indicators import adx, ema, pct_from_high, pct_from_low, rate_of_change
from .turtle import (
    BreakoutSignal,
    Direction,
    PositionAction,
    PositionStatus,
    TurtleConfig,
    unit_limit_breaches,
)

__all__ = [
    "RecommendationAction",
    "ACTION_PRIORITY",
    "ENTRY_ACTIONS",
    "POSITION_ACTIONS",
    "Recommendation",
    "TrendFeatures",
    "TrendScore",
    "AccountInfo",
    "AccountPosition",
    "BLOCKED_HEAT",
    "BLOCKED_NO_CASH",
    "BLOCKED_NO_SHORT_ACCOUNT",
    "BLOCKED_ZERO_SHARES",
    "BLOCKED_TREND_FILTER",
    "BLOCKED_INSUFFICIENT_CASH",
    "WEIGHT_RELATIVE_STRENGTH",
    "WEIGHT_ADX",
    "WEIGHT_PROXIMITY",
    "WEIGHT_TREND_FILTER",
    "entry_signal_key",
    "position_signal_key",
    "trend_features",
    "trend_scores",
    "close_above_sma",
    "market_regime",
    "entry_action",
    "build_recommendations",
]


class RecommendationAction(Enum):
    """Actions in priority order: the declaration order is the ranking."""

    EXIT_STOP = "EXIT_STOP"
    EXIT_CHANNEL = "EXIT_CHANNEL"
    ADD_UNIT = "ADD_UNIT"
    ENTER = "ENTER"
    WATCH = "WATCH"
    HOLD = "HOLD"


ACTION_PRIORITY: dict[RecommendationAction, int] = {
    a: i for i, a in enumerate(RecommendationAction)
}
ENTRY_ACTIONS: frozenset[RecommendationAction] = frozenset(
    {RecommendationAction.ENTER, RecommendationAction.WATCH}
)
POSITION_ACTIONS: dict[PositionAction, RecommendationAction] = {
    PositionAction.EXIT_STOP: RecommendationAction.EXIT_STOP,
    PositionAction.EXIT_CHANNEL: RecommendationAction.EXIT_CHANNEL,
    PositionAction.ADD_UNIT: RecommendationAction.ADD_UNIT,
    PositionAction.HOLD: RecommendationAction.HOLD,
}
# Signal-key family per action; both exits share one, so a stop exit that becomes a
# channel exit (or the reverse) is the same signal.
_KEY_FAMILY: dict[RecommendationAction, str] = {
    RecommendationAction.EXIT_STOP: "EXIT",
    RecommendationAction.EXIT_CHANNEL: "EXIT",
    RecommendationAction.ADD_UNIT: "ADD",
    RecommendationAction.HOLD: "HOLD",
    RecommendationAction.ENTER: "ENTRY",
    RecommendationAction.WATCH: "ENTRY",
}

# blocked_by messages; the heat message carries detail after "heat budget: ".
BLOCKED_HEAT = "heat budget"
BLOCKED_NO_CASH = "no account with sufficient cash"
BLOCKED_NO_SHORT_ACCOUNT = "no short-enabled account"
BLOCKED_ZERO_SHARES = "unit size rounds to 0 shares"
BLOCKED_TREND_FILTER = "trend filter against (EMA25/EMA350)"
BLOCKED_INSUFFICIENT_CASH = "insufficient cash in account"

# Untriggered breakouts within this many N of their entry level are WATCH.
WATCH_MAX_DISTANCE_N = 1.0
# Stop-limit hint on an entry ticket: this many N beyond the stop trigger.
LIMIT_HINT_N = 0.25
ROTH_MARKER = "roth"

# Trend-feature lookbacks, in trading days.
ROC_6M_DAYS = 126
ROC_12M_DAYS = 252
EXTREME_LOOKBACK_DAYS = 252
TREND_FAST_EMA = 25
TREND_SLOW_EMA = 350
ADX_LOOKBACK = 14
REGIME_SMA_DAYS = 200
BREADTH_RISK_ON = 0.5

# trend_score weights; they sum to 100.
WEIGHT_RELATIVE_STRENGTH = 40.0  # x rs_rank: mean 6m/12m ROC percentile (0..1)
WEIGHT_ADX = 20.0  # all or nothing: ADX above ADX_TREND_THRESHOLD
WEIGHT_PROXIMITY = (
    20.0  # 1 at the 52-week extreme, falling to 0 at PROXIMITY_RANGE away
)
WEIGHT_TREND_FILTER = 20.0  # EMA25 above EMA350 (long) or below (short)
ADX_TREND_THRESHOLD = 25.0
PROXIMITY_RANGE = 0.25


@dataclass(frozen=True)
class Recommendation:
    """One ranked action. ``shares`` is a whole-share count (the exact count is in ``extra``)."""

    ticker: str
    action: RecommendationAction
    account: str | None
    direction: Direction
    signal_key: str
    trigger_price: float | None
    stop_price: float | None
    shares: int
    risk_dollars: float
    risk_pct_equity: float
    distance_n: float | None
    trend_score: float | None
    rs_rank: float | None
    asset_type: str | None = None
    is_late: bool = False
    reasons: tuple[str, ...] = ()
    blocked_by: tuple[str, ...] = ()
    # Order-ticket and position details; excluded from hashing because it is a dict.
    extra: dict[str, Any] = field(default_factory=dict, hash=False, compare=False)

    @property
    def blocked(self) -> bool:
        return bool(self.blocked_by)

    @property
    def is_entry(self) -> bool:
        return self.action in ENTRY_ACTIONS


@dataclass(frozen=True)
class TrendFeatures:
    """Trend-strength inputs for one ticker and direction; NaN where history is too short."""

    ticker: str
    direction: Direction
    last_close: float
    adx: float
    roc_6m: float
    roc_12m: float
    # 52-week high for a long, low for a short, and the close's fractional distance from it (>= 0).
    extreme_52w: float
    distance_from_extreme: float
    ema_aligned: bool


class TrendScore(NamedTuple):
    score: float  # 0..100
    rs_rank: float | None  # 0..1, None without any ROC history


@dataclass(frozen=True)
class AccountInfo:
    name: str
    cash: float
    allow_short: bool
    equity: float = 0.0

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> AccountInfo:
        """From a ``db.get_accounts()`` row."""
        return cls(
            name=str(row["name"]),
            cash=float(row.get("cash") or 0.0),
            allow_short=bool(row.get("allow_short")),
            equity=float(row.get("equity") or 0.0),
        )


@dataclass(frozen=True)
class AccountPosition:
    """An evaluated position and the account that holds it."""

    account: str
    status: PositionStatus


def entry_signal_key(ticker: str, direction: Direction) -> str:
    """Shared by ENTER and WATCH, so a WATCH that triggers keeps its key."""
    return f"{ticker}|{direction.name}|ENTRY"


def position_signal_key(
    account: str, ticker: str, direction: Direction, action: RecommendationAction
) -> str:
    return f"{account}|{ticker}|{direction.name}|{_KEY_FAMILY[action]}"


def _last(series: pd.Series) -> float:
    if series.empty:
        return math.nan
    value = series.iloc[-1]
    return float(value) if pd.notna(value) else math.nan


def _finite(value: float | None) -> bool:
    return value is not None and math.isfinite(value)


def trend_features(
    ohlc: pd.DataFrame, direction: Direction, ticker: str = ""
) -> TrendFeatures:
    """ADX, 6/12-month ROC, distance from the 52-week extreme and EMA25/350 alignment.

    The extreme is the 52-week high for a long and the low for a short.
    """
    close = ohlc["close"]
    if direction is Direction.LONG:
        extreme = _last(ohlc["high"].rolling(EXTREME_LOOKBACK_DAYS).max())
        distance = -_last(pct_from_high(ohlc, EXTREME_LOOKBACK_DAYS))
    else:
        extreme = _last(ohlc["low"].rolling(EXTREME_LOOKBACK_DAYS).min())
        distance = _last(pct_from_low(ohlc, EXTREME_LOOKBACK_DAYS))
    fast = _last(ema(close, TREND_FAST_EMA))
    slow = _last(ema(close, TREND_SLOW_EMA))
    if math.isnan(fast) or math.isnan(slow):
        aligned = False
    else:
        aligned = fast > slow if direction is Direction.LONG else fast < slow
    return TrendFeatures(
        ticker=ticker,
        direction=direction,
        last_close=_last(close),
        adx=_last(adx(ohlc, ADX_LOOKBACK)),
        roc_6m=_last(rate_of_change(close, ROC_6M_DAYS)),
        roc_12m=_last(rate_of_change(close, ROC_12M_DAYS)),
        extreme_52w=extreme,
        distance_from_extreme=max(distance, 0.0)
        if not math.isnan(distance)
        else math.nan,
        ema_aligned=aligned,
    )


def _percentiles(values: Mapping[tuple[str, Direction], float]) -> pd.Series:
    """Percentile rank (0..1] of each finite value; NaN stays NaN."""
    series = pd.Series(values, dtype=float)
    return series.rank(pct=True, na_option="keep")


def trend_scores(
    features_by_ticker: Mapping[tuple[str, Direction], TrendFeatures],
) -> dict[tuple[str, Direction], TrendScore]:
    """Score each (ticker, direction) 0..100 and rank its relative strength 0..1.

    Relative strength is the mean of the cross-sectional percentiles of 6- and
    12-month ROC, computed per direction; shorts rank by weakness (most
    negative ROC is strongest). The score adds ``WEIGHT_RELATIVE_STRENGTH`` x
    rs_rank, ``WEIGHT_ADX`` when ADX > 25, ``WEIGHT_PROXIMITY`` scaled by
    closeness to the 52-week extreme, and ``WEIGHT_TREND_FILTER`` when the
    EMA filter agrees with the direction.
    """
    scores: dict[tuple[str, Direction], TrendScore] = {}
    for direction in Direction:
        group = {k: f for k, f in features_by_ticker.items() if k[1] is direction}
        if not group:
            continue
        sign = direction.value
        ranks = pd.concat(
            [
                _percentiles({k: sign * f.roc_6m for k, f in group.items()}),
                _percentiles({k: sign * f.roc_12m for k, f in group.items()}),
            ],
            axis=1,
        ).mean(axis=1, skipna=True)
        for key, feature in group.items():
            rank = float(ranks.get(key, math.nan))
            rs_rank = rank if math.isfinite(rank) else None
            score = WEIGHT_RELATIVE_STRENGTH * (rs_rank or 0.0)
            if _finite(feature.adx) and feature.adx > ADX_TREND_THRESHOLD:
                score += WEIGHT_ADX
            if _finite(feature.distance_from_extreme):
                closeness = 1.0 - feature.distance_from_extreme / PROXIMITY_RANGE
                score += WEIGHT_PROXIMITY * min(max(closeness, 0.0), 1.0)
            if feature.ema_aligned:
                score += WEIGHT_TREND_FILTER
            scores[key] = TrendScore(round(score, 2), rs_rank)
    return scores


def close_above_sma(ohlc: pd.DataFrame, period: int = REGIME_SMA_DAYS) -> bool | None:
    """Whether the last close is above its ``period``-day simple average; None if too short."""
    if len(ohlc) < period:
        return None
    sma = _last(ohlc["close"].rolling(period).mean())
    close = _last(ohlc["close"])
    if math.isnan(sma) or math.isnan(close):
        return None
    return close > sma


def market_regime(
    benchmark_ohlc: pd.DataFrame | None,
    universe_closes_above_200d: float | None,
    *,
    benchmark: str = "SPY",
) -> dict[str, Any]:
    """The benchmark against its 200-day SMA plus breadth; informational, it blocks nothing.

    ``label`` is ``risk_on`` when the benchmark is above its 200-day and
    breadth (if known) is at least 50%, ``risk_off`` when both point down,
    ``mixed`` when they disagree and ``unknown`` without benchmark data.
    """
    close = sma = None
    above: bool | None = None
    if benchmark_ohlc is not None and len(benchmark_ohlc) >= REGIME_SMA_DAYS:
        close = _last(benchmark_ohlc["close"])
        sma = _last(benchmark_ohlc["close"].rolling(REGIME_SMA_DAYS).mean())
        above = close_above_sma(benchmark_ohlc, REGIME_SMA_DAYS)
    breadth = (
        universe_closes_above_200d if _finite(universe_closes_above_200d) else None
    )
    if above is None:
        label = "unknown"
    elif breadth is None:
        label = "risk_on" if above else "risk_off"
    elif above and breadth >= BREADTH_RISK_ON:
        label = "risk_on"
    elif not above and breadth < BREADTH_RISK_ON:
        label = "risk_off"
    else:
        label = "mixed"
    return {
        "benchmark": benchmark,
        "benchmark_close": close if _finite(close) else None,
        "benchmark_sma200": sma if _finite(sma) else None,
        "benchmark_above_200d": above,
        "breadth_above_200d": breadth,
        "label": label,
    }


def _ordinal(value: int) -> str:
    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def _fmt_date(when: Any) -> str:
    return pd.Timestamp(when).strftime("%Y-%m-%d")


_UNIT_COUNT = re.compile(r"\d+(?:\.\d+)?(?=(?: short)? units)|(?<=units )\d+(?:\.\d+)?")


def _breach_kind(message: str) -> str:
    """A breach message without its unit count, so the same breach matches before and after a unit."""
    return _UNIT_COUNT.sub("#", message)


@dataclass
class _Budget:
    """Running heat, signed units and cash as positions and entries are admitted."""

    total_equity: float
    heat_limit: float
    heat_used: float
    units: dict[str, float]
    cash: dict[str, float]
    correlations: pd.DataFrame
    config: TurtleConfig

    def heat_breach(self, risk: float) -> str | None:
        after = self.heat_used + risk
        if after <= self.heat_limit + 1e-9:
            return None
        equity = self.total_equity or math.nan
        return (
            f"{BLOCKED_HEAT}: {after / equity:.1%} of equity > "
            f"{self.heat_limit / equity:.1%} (${after:,.0f} > ${self.heat_limit:,.0f})"
        )

    def unit_breaches(self, ticker: str, sign: int) -> list[str]:
        """Limit breaches the prospective unit would cause; existing unrelated ones are ignored."""
        before = {
            _breach_kind(b)
            for b in unit_limit_breaches(self.units, self.correlations, self.config)
        }
        prospective = dict(self.units)
        prospective[ticker] = prospective.get(ticker, 0.0) + sign
        after = unit_limit_breaches(prospective, self.correlations, self.config)
        return [b for b in after if _breach_kind(b) not in before]

    def admit(
        self, ticker: str, sign: int, risk: float, account: str | None, value: float
    ) -> None:
        self.heat_used += risk
        self.units[ticker] = self.units.get(ticker, 0.0) + sign
        if account is not None and sign > 0 and account in self.cash:
            self.cash[account] -= value


def _route(
    direction: Direction,
    value: float,
    accounts: Sequence[AccountInfo],
    cash: Mapping[str, float],
) -> tuple[str | None, str | None]:
    """The account for a new position and, when none qualifies, the blocking reason."""
    if direction is Direction.SHORT:
        # Short proceeds credit the account; margin and borrow are not modelled.
        for account in accounts:
            if account.allow_short:
                return account.name, None
        return None, BLOCKED_NO_SHORT_ACCOUNT
    ordered = sorted(accounts, key=lambda a: ROTH_MARKER not in a.name.lower())
    for account in ordered:
        if cash.get(account.name, 0.0) >= value:
            return account.name, None
    return None, BLOCKED_NO_CASH


def _position_recommendation(
    held: AccountPosition,
    budget: _Budget,
    score: TrendScore | None,
    asset_type: str | None,
    config: TurtleConfig,
) -> Recommendation:
    status = held.status
    action = POSITION_ACTIONS[status.action]
    direction = status.direction
    sign = direction.value
    side = "low" if direction is Direction.LONG else "high"
    held_shares = abs(status.shares)
    reasons: list[str] = []
    blocked: list[str] = []
    trigger: float | None = None
    stop: float | None = status.stop_price
    shares = int(held_shares)
    risk = status.open_risk_dollars
    order_type: str | None = None

    if action is RecommendationAction.EXIT_STOP:
        trigger = status.first_exit.price if status.first_exit else status.stop_price
        reasons.append(f"{config.stop_n:g}N stop {status.stop_price:,.2f} hit")
        order_type = "sell" if direction is Direction.LONG else "buy to cover"
    elif action is RecommendationAction.EXIT_CHANNEL:
        trigger = status.first_exit.price if status.first_exit else status.exit_level
        reasons.append(f"{config.exit_period}-day {side} exit")
        order_type = "sell" if direction is Direction.LONG else "buy to cover"
    elif action is RecommendationAction.ADD_UNIT:
        trigger = status.next_add_level
        stop = status.next_add_level - sign * config.stop_n * status.n_at_entry
        shares = status.unit_shares_now
        risk = shares * abs(trigger - stop)
        units = status.units_held
        units_text = f"{abs(units):.1f}" if math.isfinite(units) else "?"
        reasons.append(
            f"close {status.last_close:,.2f} beyond add level {trigger:,.2f} "
            f"({units_text} units held)"
        )
        order_type = "buy stop" if direction is Direction.LONG else "sell short stop"
        if shares <= 0:
            blocked.append(BLOCKED_ZERO_SHARES)
        heat = budget.heat_breach(risk)
        if heat:
            blocked.append(heat)
        blocked.extend(budget.unit_breaches(status.ticker, sign))
        value = shares * trigger
        if (
            direction is Direction.LONG
            and held.account in budget.cash
            and budget.cash[held.account] < value
        ):
            blocked.append(f"{BLOCKED_INSUFFICIENT_CASH} {held.account}")
        if not blocked:
            budget.admit(status.ticker, sign, risk, held.account, value)
    else:
        reasons.append(f"{status.distance_to_stop_n:.1f}N from stop")
    reasons.append(f"open {status.open_r_multiple:+.1f}R")
    if status.is_late and status.first_exit is not None:
        reasons.append(
            f"rule fired {_fmt_date(status.first_exit.exit_date)}, still open"
        )

    return Recommendation(
        ticker=status.ticker,
        action=action,
        account=held.account,
        direction=direction,
        signal_key=position_signal_key(held.account, status.ticker, direction, action),
        trigger_price=trigger,
        stop_price=stop,
        shares=shares,
        risk_dollars=risk,
        risk_pct_equity=risk / budget.total_equity if budget.total_equity else math.nan,
        distance_n=status.distance_to_stop_n,
        trend_score=score.score if score else None,
        rs_rank=score.rs_rank if score else None,
        asset_type=asset_type,
        is_late=status.is_late,
        reasons=tuple(reasons),
        blocked_by=tuple(blocked),
        extra={
            "order_type": order_type,
            "shares_exact": status.shares,
            "last_close": status.last_close,
            "last_date": _fmt_date(status.last_date),
            "entry_price": status.entry_price,
            "entry_date": _fmt_date(status.entry_date),
            "n": status.n_now,
            "n_at_entry": status.n_at_entry,
            "units_held": status.units_held,
            "unit_shares": status.unit_shares_now,
            "exit_level": status.exit_level,
            "next_add_level": status.next_add_level,
            "open_r_multiple": status.open_r_multiple,
            "open_risk_dollars": status.open_risk_dollars,
            "rule_fired_on": _fmt_date(status.first_exit.exit_date)
            if status.first_exit
            else None,
        },
    )


def _entry_reasons(
    signal: BreakoutSignal,
    action: RecommendationAction,
    features: TrendFeatures | None,
    score: TrendScore | None,
    config: TurtleConfig,
) -> list[str]:
    side = "high" if signal.direction is Direction.LONG else "low"
    if action is RecommendationAction.ENTER:
        reasons = [f"{config.entry_period}-day {side} breakout"]
    else:
        reasons = [f"{signal.distance_n:.2f}N from {config.entry_period}-day {side}"]
    if features is not None and _finite(features.adx):
        reasons.append(f"ADX {features.adx:.0f}")
    if score is not None and score.rs_rank is not None:
        reasons.append(f"RS {_ordinal(round(score.rs_rank * 100))} pct")
    if features is not None and _finite(features.distance_from_extreme):
        reasons.append(f"{features.distance_from_extreme:.1%} from 52-week {side}")
    aligned = features.ema_aligned if features is not None else signal.trend_ok
    reasons.append("trend filter aligned" if aligned else "trend filter not aligned")
    if signal.direction is Direction.SHORT:
        reasons.append("check borrow")
    return reasons


def _entry_recommendation(
    signal: BreakoutSignal,
    action: RecommendationAction,
    budget: _Budget,
    accounts: Sequence[AccountInfo],
    features: TrendFeatures | None,
    score: TrendScore | None,
    asset_type: str | None,
    config: TurtleConfig,
) -> Recommendation:
    direction = signal.direction
    sign = direction.value
    entry = signal.entry_level
    stop = signal.stop_if_filled
    shares = signal.unit_shares
    risk = shares * abs(entry - stop)
    value = shares * entry

    blocked: list[str] = []
    if shares <= 0:
        blocked.append(BLOCKED_ZERO_SHARES)
    if config.use_trend_filter and not signal.trend_ok:
        blocked.append(BLOCKED_TREND_FILTER)
    heat = budget.heat_breach(risk)
    if heat:
        blocked.append(heat)
    blocked.extend(budget.unit_breaches(signal.ticker, sign))
    account, no_account = _route(direction, value, accounts, budget.cash)
    if no_account:
        blocked.append(no_account)
    # WATCH orders haven't filled, so they are checked against the budget but don't use it.
    if action is RecommendationAction.ENTER and not blocked:
        budget.admit(signal.ticker, sign, risk, account, value)

    r_to_extreme = None
    if features is not None and _finite(features.extreme_52w) and entry != stop:
        r_to_extreme = sign * (features.extreme_52w - entry) / abs(entry - stop)
    return Recommendation(
        ticker=signal.ticker,
        action=action,
        account=account,
        direction=direction,
        signal_key=entry_signal_key(signal.ticker, direction),
        trigger_price=entry,
        stop_price=stop,
        shares=shares,
        risk_dollars=risk,
        risk_pct_equity=risk / budget.total_equity if budget.total_equity else math.nan,
        distance_n=signal.distance_n,
        trend_score=score.score if score else None,
        rs_rank=score.rs_rank if score else None,
        asset_type=asset_type,
        is_late=False,
        reasons=tuple(_entry_reasons(signal, action, features, score, config)),
        blocked_by=tuple(blocked),
        extra={
            "order_type": "buy stop"
            if direction is Direction.LONG
            else "sell short stop",
            "entry_level": entry,
            "limit_hint": entry + sign * LIMIT_HINT_N * signal.n,
            "n": signal.n,
            "unit_shares": shares,
            "value": value,
            "r_to_extreme": r_to_extreme,
            "check_borrow": direction is Direction.SHORT,
            "last_close": signal.last_close,
            "last_date": _fmt_date(signal.last_date),
            "trend_ok": signal.trend_ok,
        },
    )


def _score_order(score: TrendScore | None, ticker: str) -> tuple[bool, float, str]:
    """Highest score first; unscored last, then by ticker."""
    return (score is None, -(score.score if score else 0.0), ticker)


def _sort_key(rec: Recommendation) -> tuple[int, bool, float, str, str]:
    return (
        ACTION_PRIORITY[rec.action],
        rec.trend_score is None,
        -(rec.trend_score or 0.0),
        rec.ticker,
        rec.account or "",
    )


def entry_action(signal: BreakoutSignal) -> RecommendationAction | None:
    """ENTER when triggered, WATCH when within ``WATCH_MAX_DISTANCE_N``, else None."""
    if signal.triggered:
        return RecommendationAction.ENTER
    if 0 < signal.distance_n <= WATCH_MAX_DISTANCE_N:
        return RecommendationAction.WATCH
    return None


def _held_directions(
    positions: Iterable[AccountPosition],
) -> dict[str, frozenset[Direction]]:
    held: dict[str, set[Direction]] = {}
    for p in positions:
        held.setdefault(p.status.ticker, set()).add(p.status.direction)
    return {t: frozenset(d) for t, d in held.items()}


def build_recommendations(
    *,
    position_statuses: Sequence[AccountPosition],
    breakout_signals: Sequence[BreakoutSignal],
    accounts: Sequence[AccountInfo],
    total_equity: float,
    config: TurtleConfig,
    max_heat_pct: float,
    scores: Mapping[tuple[str, Direction], TrendScore] | None = None,
    features: Mapping[tuple[str, Direction], TrendFeatures] | None = None,
    open_positions_by_ticker: Mapping[str, Collection[Direction]] | None = None,
    correlations: pd.DataFrame | None = None,
    asset_types: Mapping[str, str] | None = None,
) -> list[Recommendation]:
    """Rank position actions and new entries into one list, applying risk and routing rules.

    ``open_positions_by_ticker`` maps each held ticker to the directions it is
    held in; it defaults to what ``position_statuses`` shows, and should also
    cover positions that could not be evaluated so they still block entries.
    Budget is admitted in priority order: adds (by score), then ENTER (by
    score). WATCH rows are checked against what remains but don't use it.
    The result is sorted by action priority, then score.
    """
    scores = scores or {}
    features = features or {}
    asset_types = asset_types or {}
    held = (
        {t: frozenset(d) for t, d in open_positions_by_ticker.items()}
        if open_positions_by_ticker is not None
        else _held_directions(position_statuses)
    )

    units: dict[str, float] = {}
    for p in position_statuses:
        if math.isfinite(p.status.units_held):
            units[p.status.ticker] = (
                units.get(p.status.ticker, 0.0) + p.status.units_held
            )
    budget = _Budget(
        total_equity=total_equity,
        heat_limit=max_heat_pct * total_equity,
        heat_used=sum(p.status.open_risk_dollars for p in position_statuses),
        units=units,
        cash={a.name: a.cash for a in accounts},
        correlations=correlations if correlations is not None else pd.DataFrame(),
        config=config,
    )

    def position_order(p: AccountPosition) -> tuple[int, bool, float, str]:
        action = POSITION_ACTIONS[p.status.action]
        key = (p.status.ticker, p.status.direction)
        return (
            ACTION_PRIORITY[action],
            *_score_order(scores.get(key), p.status.ticker),
        )

    recommendations: list[Recommendation] = []
    for p in sorted(position_statuses, key=position_order):
        key = (p.status.ticker, p.status.direction)
        recommendations.append(
            _position_recommendation(
                p, budget, scores.get(key), asset_types.get(p.status.ticker), config
            )
        )

    candidates: list[tuple[RecommendationAction, BreakoutSignal]] = []
    for signal in breakout_signals:
        action = entry_action(signal)
        if action is None or signal.skipped_after_winner or signal.ticker in held:
            continue
        candidates.append((action, signal))
    candidates.sort(
        key=lambda c: (
            ACTION_PRIORITY[c[0]],
            *_score_order(scores.get((c[1].ticker, c[1].direction)), c[1].ticker),
        )
    )
    for action, signal in candidates:
        key = (signal.ticker, signal.direction)
        recommendations.append(
            _entry_recommendation(
                signal,
                action,
                budget,
                accounts,
                features.get(key),
                scores.get(key),
                asset_types.get(signal.ticker),
                config,
            )
        )
    return sorted(recommendations, key=_sort_key)
