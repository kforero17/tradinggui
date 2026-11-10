"""Review of closed trades against the Turtle rules: what happened, and what the rules would have done."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from collections.abc import Callable
from datetime import date
from enum import Enum

import numpy as np
import pandas as pd

from ..data.broker_import import (
    Direction,
    Fill,
    OpenPosition,
    RoundTrip,
    apply_splits,
    match_round_trips,
)
from .indicators import donchian_high, turtle_n
from .turtle import ExitEvent, TurtleConfig, first_exit

__all__ = [
    "EntryType",
    "TradeReview",
    "RuleReplay",
    "ReviewReport",
    "review_trade",
    "replay_rules",
    "review_trades",
    "summarize",
    "run_review",
]

BREAKOUT_TOLERANCE = 0.98
LONG_TREND_PERIOD = 200
BREAKOUT_LOOKBACK = 20
REENTRY_WINDOW_DAYS = 90
# Smaller unmatched remainders are fractional residue from dividend reinvestment.
MIN_UNMATCHED_SHARES = 1.0


class EntryType(str, Enum):
    BREAKOUT = "Breakout (near 20-day high)"
    PULLBACK = "Pullback in uptrend"
    DOWNTREND = "Downtrend (below 200-day avg)"
    UNKNOWN = "Unknown"


@dataclass(frozen=True)
class TradeReview:
    account: str
    symbol: str
    direction: str
    entry_date: date
    exit_date: date
    quantity: float
    entry_price: float
    exit_price: float
    pnl: float
    return_pct: float
    holding_days: int
    position_value: float
    n_at_entry: float | None
    risk_at_2n: float | None
    r_multiple: float | None
    mae_pct: float | None
    mfe_pct: float | None
    mfe_capture: float | None
    entry_type: EntryType
    turtle_exit_date: date | None
    turtle_exit_price: float | None
    turtle_exit_reason: str | None
    turtle_still_open: bool
    turtle_pnl: float | None
    turtle_reentries: int = 0

    @property
    def is_win(self) -> bool:
        return self.pnl > 0

    @property
    def turtle_minus_actual(self) -> float | None:
        return None if self.turtle_pnl is None else self.turtle_pnl - self.pnl

    @property
    def turtle_exited_earlier(self) -> bool:
        return (
            self.turtle_exit_date is not None and self.turtle_exit_date < self.exit_date
        )

    def to_dict(self) -> dict:
        record = asdict(self)
        record["entry_type"] = self.entry_type.value
        record["is_win"] = self.is_win
        record["turtle_minus_actual"] = self.turtle_minus_actual
        record["turtle_exited_earlier"] = self.turtle_exited_earlier
        for key in ("entry_date", "exit_date", "turtle_exit_date"):
            record[key] = record[key].isoformat() if record[key] else None
        return record


def _classify_entry(
    ohlc: pd.DataFrame, entry_ts: pd.Timestamp, entry_price: float
) -> EntryType:
    prior = ohlc.loc[:entry_ts]
    if len(prior) < LONG_TREND_PERIOD:
        return EntryType.UNKNOWN
    long_average = float(prior["close"].tail(LONG_TREND_PERIOD).mean())
    recent_high = float(donchian_high(prior, BREAKOUT_LOOKBACK).iloc[-1])
    if entry_price >= BREAKOUT_TOLERANCE * recent_high:
        return EntryType.BREAKOUT
    if entry_price < long_average:
        return EntryType.DOWNTREND
    return EntryType.PULLBACK


def review_trade(
    trip: RoundTrip, ohlc: pd.DataFrame | None, config: TurtleConfig
) -> TradeReview:
    base = {
        "account": trip.account,
        "symbol": trip.symbol,
        "direction": trip.direction.value,
        "entry_date": trip.entry_date,
        "exit_date": trip.exit_date,
        "quantity": trip.quantity,
        "entry_price": trip.entry_price,
        "exit_price": trip.exit_price,
        "pnl": trip.pnl,
        "return_pct": trip.return_pct,
        "holding_days": trip.holding_days,
        "position_value": trip.entry_price * trip.quantity,
    }
    empty = {
        "n_at_entry": None,
        "risk_at_2n": None,
        "r_multiple": None,
        "mae_pct": None,
        "mfe_pct": None,
        "mfe_capture": None,
        "entry_type": EntryType.UNKNOWN,
        "turtle_exit_date": None,
        "turtle_exit_price": None,
        "turtle_exit_reason": None,
        "turtle_still_open": False,
        "turtle_pnl": None,
    }
    entry_ts, exit_ts = pd.Timestamp(trip.entry_date), pd.Timestamp(trip.exit_date)
    if ohlc is None or ohlc.loc[:entry_ts].empty:
        return TradeReview(**base, **empty)

    n_series = turtle_n(ohlc, config.n_period).loc[:entry_ts].dropna()
    n_at_entry = float(n_series.iloc[-1]) if not n_series.empty else None
    held = ohlc.loc[entry_ts:exit_ts]
    sign = 1.0 if trip.direction is Direction.LONG else -1.0
    best = held["high"].max() if sign > 0 else held["low"].min()
    worst = held["low"].min() if sign > 0 else held["high"].max()
    mfe_pct = sign * (float(best) / trip.entry_price - 1)
    mae_pct = sign * (float(worst) / trip.entry_price - 1)
    captured = sign * (trip.exit_price / trip.entry_price - 1)
    risk_at_2n = config.stop_n * n_at_entry * trip.quantity if n_at_entry else None

    review = {
        "n_at_entry": n_at_entry,
        "risk_at_2n": risk_at_2n,
        "r_multiple": trip.pnl / risk_at_2n if risk_at_2n else None,
        "mae_pct": mae_pct,
        "mfe_pct": mfe_pct,
        "mfe_capture": captured / mfe_pct if mfe_pct > 0 else None,
        "entry_type": _classify_entry(ohlc, entry_ts, trip.entry_price)
        if sign > 0
        else EntryType.UNKNOWN,
        "turtle_exit_date": None,
        "turtle_exit_price": None,
        "turtle_exit_reason": None,
        "turtle_still_open": False,
        "turtle_pnl": None,
    }
    if sign > 0 and n_at_entry:
        replay = replay_rules(
            ohlc, entry_ts, trip.entry_price, trip.quantity, n_at_entry, exit_ts, config
        )
        first = replay.first_exit
        review.update(
            turtle_exit_date=first.exit_date.date() if first else None,
            turtle_exit_price=first.price if first else None,
            turtle_exit_reason=first.reason.value if first else None,
            turtle_still_open=replay.still_open,
            turtle_reentries=replay.reentries,
            turtle_pnl=replay.pnl - trip.fees,
        )
    return TradeReview(**base, **review)


@dataclass(frozen=True)
class RuleReplay:
    first_exit: ExitEvent | None
    pnl: float
    reentries: int
    still_open: bool


def replay_rules(
    ohlc: pd.DataFrame,
    entry_ts: pd.Timestamp,
    entry_price: float,
    quantity: float,
    n_at_entry: float,
    window_end: pd.Timestamp,
    config: TurtleConfig,
) -> RuleReplay:
    """The same share quantity managed by the Turtle exits, with re-entry on breakouts.

    Starts from the actual entry. After each rule exit it re-enters on the
    next ``entry_period`` breakout that comes before ``window_end`` (the actual
    exit date), the way the Turtles took every breakout. The last re-entry is
    held until its own rule exit, or marked to the latest close if still open.
    """
    entry_high = donchian_high(ohlc, config.entry_period)
    n_before = turtle_n(ohlc, config.n_period).shift(1)
    pnl, reentries, first = 0.0, 0, None
    current_ts, current_price, current_n = entry_ts, entry_price, n_at_entry
    while True:
        event = first_exit(ohlc, current_ts, current_price, current_n, config)
        if event is None:
            pnl += (float(ohlc["close"].iloc[-1]) - current_price) * quantity
            return RuleReplay(first, pnl, reentries, still_open=True)
        first = first or event
        pnl += (event.price - current_price) * quantity
        window = ohlc.loc[(ohlc.index > event.exit_date) & (ohlc.index <= window_end)]
        breakouts = window.index[window["high"] > entry_high.loc[window.index]]
        if breakouts.empty or np.isnan(n_before.loc[breakouts[0]]):
            return RuleReplay(first, pnl, reentries, still_open=False)
        current_ts = breakouts[0]
        current_price = max(
            float(ohlc.at[current_ts, "open"]), float(entry_high.loc[current_ts])
        )
        current_n = float(n_before.loc[current_ts])
        reentries += 1


def review_trades(
    trips: list[RoundTrip], histories: dict[str, pd.DataFrame], config: TurtleConfig
) -> list[TradeReview]:
    """Reviews of the matched shares of round trips where most shares have a cost basis.

    A trip that is mostly unmatched (say a few reinvested-dividend shares
    against a large sale of older lots) would be judged on the wrong entry.
    """
    return [
        review_trade(t, histories.get(t.symbol), config)
        for t in trips
        if t.quantity > 0 and t.unmatched_quantity < t.quantity
    ]


def _median(values: list[float]) -> float | None:
    return float(np.median(values)) if values else None


def _group_pnl(reviews: list[TradeReview], key) -> list[dict]:
    groups: dict[str, list[TradeReview]] = defaultdict(list)
    for r in reviews:
        groups[key(r)].append(r)
    return [
        {
            "group": name,
            "trades": len(items),
            "win_rate": sum(r.is_win for r in items) / len(items),
            "pnl": sum(r.pnl for r in items),
        }
        for name, items in sorted(groups.items())
    ]


def _reentries(reviews: list[TradeReview]) -> list[dict]:
    """Re-buys of the same symbol soon after selling it, at a higher price than the sale."""
    by_symbol: dict[str, list[TradeReview]] = defaultdict(list)
    for r in sorted(reviews, key=lambda r: r.entry_date):
        by_symbol[r.symbol].append(r)
    found = []
    for symbol, items in by_symbol.items():
        for earlier, later in zip(items, items[1:]):
            gap = (later.entry_date - earlier.exit_date).days
            if (
                0 <= gap <= REENTRY_WINDOW_DAYS
                and later.entry_price > earlier.exit_price
            ):
                found.append(
                    {
                        "symbol": symbol,
                        "sold": earlier.exit_date.isoformat(),
                        "sold_at": earlier.exit_price,
                        "rebought": later.entry_date.isoformat(),
                        "rebought_at": later.entry_price,
                        "days_apart": gap,
                    }
                )
    return found


def summarize(reviews: list[TradeReview], unmatched_trips: list[RoundTrip]) -> dict:
    wins = [r for r in reviews if r.is_win]
    losses = [r for r in reviews if not r.is_win]
    gross_win = sum(r.pnl for r in wins)
    gross_loss = -sum(r.pnl for r in losses)
    total = sum(r.pnl for r in reviews)
    top_three = sum(sorted((r.pnl for r in wins), reverse=True)[:3])
    with_r = [r for r in reviews if r.r_multiple is not None]
    past_stop = [r for r in losses if r.r_multiple is not None and r.r_multiple < -1]
    excess_beyond_stop = sum(r.pnl + r.risk_at_2n for r in past_stop if r.risk_at_2n)
    replayed = [r for r in reviews if r.turtle_pnl is not None]
    sizes = [r.position_value for r in reviews]

    return {
        "trades": len(reviews),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(reviews) if reviews else 0.0,
        "total_pnl": total,
        "gross_win": gross_win,
        "gross_loss": gross_loss,
        "avg_win": gross_win / len(wins) if wins else 0.0,
        "avg_loss": -gross_loss / len(losses) if losses else 0.0,
        "payoff_ratio": (gross_win / len(wins)) / (gross_loss / len(losses))
        if wins and losses and gross_loss
        else None,
        "profit_factor": gross_win / gross_loss if gross_loss else None,
        "expectancy": total / len(reviews) if reviews else 0.0,
        "avg_r": float(np.mean([r.r_multiple for r in with_r])) if with_r else None,
        "top3_share_of_gross_win": top_three / gross_win if gross_win else None,
        "median_hold_days_winners": _median([r.holding_days for r in wins]),
        "median_hold_days_losers": _median([r.holding_days for r in losses]),
        "median_mfe_capture_winners": _median(
            [r.mfe_capture for r in wins if r.mfe_capture is not None]
        ),
        "median_mae_losers": _median(
            [r.mae_pct for r in losses if r.mae_pct is not None]
        ),
        "losers_past_2n_stop": len(past_stop),
        "loss_beyond_2n_stop": excess_beyond_stop,
        "turtle_replay_trades": len(replayed),
        "turtle_replay_actual_pnl": sum(r.pnl for r in replayed),
        "turtle_replay_rule_pnl": sum(
            r.turtle_pnl for r in replayed if r.turtle_pnl is not None
        ),
        "turtle_saved_on_losers": sum(
            r.turtle_minus_actual
            for r in replayed
            if not r.is_win and r.turtle_minus_actual and r.turtle_minus_actual > 0
        ),
        "turtle_exited_winners_early": sum(
            1 for r in replayed if r.is_win and r.turtle_exited_earlier
        ),
        "median_position_value": _median(sizes),
        "position_value_cv": float(np.std(sizes) / np.mean(sizes)) if sizes else None,
        "by_account": _group_pnl(reviews, lambda r: r.account),
        "by_year": _group_pnl(reviews, lambda r: str(r.exit_date.year)),
        "by_entry_type": _group_pnl(reviews, lambda r: r.entry_type.value),
        "reentries": _reentries(reviews),
        "unmatched_sells": [
            {
                "account": t.account,
                "symbol": t.symbol,
                "date": t.exit_date.isoformat(),
                "quantity": t.unmatched_quantity,
            }
            for t in unmatched_trips
            if t.unmatched_quantity >= MIN_UNMATCHED_SHARES
        ],
        "first_trade": min(
            (r.entry_date for r in reviews), default=date.today()
        ).isoformat(),
        "last_trade": max(
            (r.exit_date for r in reviews), default=date.today()
        ).isoformat(),
    }


@dataclass(frozen=True)
class ReviewReport:
    reviews: list[TradeReview]
    summary: dict
    open_positions: list[OpenPosition]


def run_review(
    fills: list[Fill],
    load_history: Callable[[str], pd.DataFrame | None],
    config: TurtleConfig,
) -> ReviewReport:
    """Split-adjust fills, FIFO-match them, and review each round trip against ``load_history`` prices."""
    symbols = sorted({f.symbol for f in fills})
    histories = {s: h for s in symbols if (h := load_history(s)) is not None}
    splits = {
        s: [
            (when.date(), float(ratio))
            for when, ratio in h.loc[h["stock_splits"] > 0, "stock_splits"].items()
        ]
        for s, h in histories.items()
        if "stock_splits" in h
    }
    trips, open_positions = match_round_trips(apply_splits(fills, splits))
    reviews = review_trades(trips, histories, config)
    unmatched = [t for t in trips if not t.basis_known]
    return ReviewReport(reviews, summarize(reviews, unmatched), open_positions)
