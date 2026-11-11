"""Turtle trend-following rules from Curtis Faith's *Way of the Turtle*.

Adaptation for stocks and ETFs, long and short. Every rule is written for a
long and mirrored for a short (``Direction``); the caller decides which
accounts may short (a Roth IRA cannot), so the backtest only shorts when
``TurtleConfig.allow_short`` is set and ``scan_breakouts`` only scans the
directions it is asked for. Rules taken from the book's "Original Turtle
Trading Rules" appendix, stated for a long (a short swaps highs for lows,
above for below):

* N is the 20-day exponential average of true range.
* A unit risks ``risk_per_unit`` (1%) of notional equity per 1N of movement.
* System 1 enters on a 20-day breakout, skipping it when the previous
  breakout was a winner (then a 55-day failsafe breakout is used instead), and
  exits on a 10-day low. System 2 enters on a 55-day breakout and exits on a
  20-day low. A short enters on a break of the N-day low and exits on a break
  of the exit-period high.
* Units are added every 1/2 N above the previous fill, up to 4. The stop is 2N
  below each unit's fill, and earlier units' stops rise by 1/2 N when a unit is
  added. For a short, adds are every 1/2 N below, the stop is 2N above, and
  stops fall with each add.
* At most 4 units per ticker, 6 across closely correlated tickers, 10 across
  loosely correlated tickers, and 12 in one direction. Longs and shorts are
  counted separately, so the group and direction limits apply to each side.
* The optional trend filter allows longs only while the fast EMA is above the
  slow one, and shorts only while it is below.
* Notional equity is cut 20% for every 10% drawdown from the yearly starting
  equity, and restored once that starting equity is regained.

Two stock-specific additions that are not in the book: stocks are unlevered
and their N is small relative to price, so a 1%-risk unit can be a very large
share of the account. ``max_unit_fraction`` caps a unit's notional value and
``max_gross_exposure`` caps the total invested value, both as a fraction of
notional equity.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from datetime import date
from enum import Enum

import numpy as np
import pandas as pd

from .indicators import donchian_high, donchian_low, ema, turtle_n

__all__ = [
    "Direction",
    "direction_of",
    "TurtleSystem",
    "TurtleConfig",
    "PositionAction",
    "ExitReason",
    "ExitEvent",
    "PositionStatus",
    "BreakoutSignal",
    "BacktestTrade",
    "BacktestResult",
    "NotionalAccount",
    "unit_shares",
    "first_exit",
    "evaluate_position",
    "scan_breakouts",
    "correlation_matrix",
    "unit_limit_breaches",
    "backtest",
    "performance_stats",
]

TRADING_DAYS_PER_YEAR = 252


class Direction(Enum):
    """Position side; the value is the sign of the share count (the ``Portfolio`` convention)."""

    LONG = 1
    SHORT = -1


def direction_of(shares: float) -> Direction:
    """Negative shares are a short; zero and positive are treated as long."""
    return Direction.SHORT if shares < 0 else Direction.LONG


def _at_or_beyond(price: float, level: float, sign: int) -> bool:
    """True when ``price`` is at ``level`` or past it in direction ``sign`` (+1 up, -1 down)."""
    return price >= level if sign > 0 else price <= level


def _touches(high: float, low: float, level: float, sign: int) -> bool:
    """The bar reached ``level`` moving in direction ``sign``: its high for up, its low for down."""
    return _at_or_beyond(high if sign > 0 else low, level, sign)


def _breaks(high: float, low: float, level: float, sign: int) -> bool:
    """The bar went strictly past ``level`` in direction ``sign``."""
    return high > level if sign > 0 else low < level


def _fill_through(bar_open: float, level: float, sign: int) -> float:
    """Fill of a stop order at ``level`` triggered by price moving in direction ``sign``.

    A buy stop (``sign`` +1) fills at the open when the bar gaps above the
    level, a sell stop (-1) when it gaps below.
    """
    return max(bar_open, level) if sign > 0 else min(bar_open, level)


def _entry_channel(ohlc: pd.DataFrame, period: int, direction: Direction) -> pd.Series:
    """The breakout level an entry must break: the prior N-day high for a long, low for a short."""
    if direction is Direction.LONG:
        return donchian_high(ohlc, period)
    return donchian_low(ohlc, period)


def _exit_channel(ohlc: pd.DataFrame, period: int, direction: Direction) -> pd.Series:
    """The channel exit level: the prior N-day low for a long, high for a short."""
    if direction is Direction.LONG:
        return donchian_low(ohlc, period)
    return donchian_high(ohlc, period)


class TurtleSystem(str, Enum):
    SYSTEM_1 = "S1"
    SYSTEM_2 = "S2"


@dataclass(frozen=True)
class TurtleConfig:
    system: TurtleSystem = TurtleSystem.SYSTEM_2
    entry_period: int = 55
    exit_period: int = 20
    failsafe_period: int = 55
    n_period: int = 20
    risk_per_unit: float = 0.01
    stop_n: float = 2.0
    add_step_n: float = 0.5
    max_units_per_ticker: int = 4
    max_units_closely_correlated: int = 6
    max_units_loosely_correlated: int = 10
    max_units_one_direction: int = 12
    close_correlation: float = 0.7
    loose_correlation: float = 0.4
    use_trend_filter: bool = False
    trend_fast_period: int = 25
    trend_slow_period: int = 350
    drawdown_step: float = 0.10
    drawdown_cut: float = 0.20
    max_unit_fraction: float = 0.25
    max_gross_exposure: float = 1.0
    # Backtest only: also take short entries. Live scans pick directions per call.
    allow_short: bool = False

    @classmethod
    def system_1(cls, **overrides: object) -> TurtleConfig:
        return replace(
            cls(system=TurtleSystem.SYSTEM_1, entry_period=20, exit_period=10),
            **overrides,
        )

    @classmethod
    def system_2(cls, **overrides: object) -> TurtleConfig:
        return replace(
            cls(system=TurtleSystem.SYSTEM_2, entry_period=55, exit_period=20),
            **overrides,
        )

    @property
    def skips_after_winner(self) -> bool:
        return self.system is TurtleSystem.SYSTEM_1


class PositionAction(str, Enum):
    HOLD = "HOLD"
    EXIT_STOP = "EXIT_STOP"
    EXIT_CHANNEL = "EXIT_CHANNEL"
    ADD_UNIT = "ADD_UNIT"


class ExitReason(str, Enum):
    STOP = "stop"
    CHANNEL = "channel"


@dataclass(frozen=True)
class ExitEvent:
    exit_date: pd.Timestamp
    price: float
    reason: ExitReason


def unit_shares(equity: float, n: float, price: float, config: TurtleConfig) -> int:
    """Shares in one unit: 1N of movement risks ``risk_per_unit`` of equity, capped by ``max_unit_fraction``."""
    if not (n > 0 and price > 0 and equity > 0):
        return 0
    by_risk = config.risk_per_unit * equity / n
    by_notional_cap = config.max_unit_fraction * equity / price
    return int(math.floor(min(by_risk, by_notional_cap)))


def _value_on_or_before(series: pd.Series, when: pd.Timestamp) -> float:
    history = series.loc[:when].dropna()
    return float(history.iloc[-1]) if not history.empty else float("nan")


def first_exit(
    ohlc: pd.DataFrame,
    entry_date: pd.Timestamp,
    entry_price: float,
    n_at_entry: float,
    config: TurtleConfig,
    direction: Direction = Direction.LONG,
) -> ExitEvent | None:
    """The first bar after ``entry_date`` on which the 2N stop or the exit channel would have fired.

    Fills are at the trigger price, or at the open when the bar gaps through it.
    A short's stop is 2N above the entry and its channel is the exit-period high.
    """
    sign = direction.value
    stop = entry_price - sign * config.stop_n * n_at_entry
    exit_channel = _exit_channel(ohlc, config.exit_period, direction)
    after_entry = ohlc.loc[ohlc.index > entry_date]
    for when, bar in after_entry.iterrows():
        high, low, bar_open = float(bar["high"]), float(bar["low"]), float(bar["open"])
        # Exits move against the position: down for a long, up for a short.
        if _touches(high, low, stop, -sign):
            return ExitEvent(
                when, _fill_through(bar_open, stop, -sign), ExitReason.STOP
            )
        level = exit_channel.loc[when]
        if not np.isnan(level) and _breaks(high, low, float(level), -sign):
            return ExitEvent(
                when, _fill_through(bar_open, float(level), -sign), ExitReason.CHANNEL
            )
    return None


@dataclass(frozen=True)
class PositionStatus:
    ticker: str
    last_date: pd.Timestamp
    last_close: float
    shares: float
    entry_price: float
    entry_date: pd.Timestamp
    n_at_entry: float
    n_now: float
    stop_price: float
    exit_level: float
    next_add_level: float
    distance_to_stop_n: float
    open_r_multiple: float
    open_risk_dollars: float
    open_risk_pct_equity: float
    units_held: float
    unit_shares_now: int
    action: PositionAction
    first_exit: ExitEvent | None
    # Inferred from the sign of ``shares``; ``shares`` and ``units_held`` are negative for a short.
    direction: Direction = Direction.LONG

    @property
    def is_late(self) -> bool:
        """True when a rule already fired on an earlier bar and the position is still open."""
        return (
            self.first_exit is not None and self.first_exit.exit_date < self.last_date
        )


def evaluate_position(
    ticker: str,
    ohlc: pd.DataFrame,
    shares: float,
    entry_price: float,
    entry_date: date | pd.Timestamp,
    equity: float,
    config: TurtleConfig,
) -> PositionStatus:
    """Apply the Turtle stop, exit channel and add rules to an open position.

    Negative ``shares`` is a short: the stop sits above the entry, the exit
    level is the exit-period high and adds come below the entry. ``entry_price``
    is always the positive per-share fill price.
    """
    direction = direction_of(shares)
    sign = direction.value
    entry_ts = pd.Timestamp(entry_date).normalize()
    n = turtle_n(ohlc, config.n_period)
    n_at_entry = _value_on_or_before(n, entry_ts)
    if np.isnan(n_at_entry):
        n_at_entry = float(n.dropna().iloc[0])
    n_now = float(n.dropna().iloc[-1])
    last_date = ohlc.index[-1]
    last_close = float(ohlc["close"].iloc[-1])

    stop_price = entry_price - sign * config.stop_n * n_at_entry
    # Tomorrow's exit level includes today's bar, hence no shift.
    if direction is Direction.LONG:
        exit_level = float(ohlc["low"].rolling(config.exit_period).min().iloc[-1])
    else:
        exit_level = float(ohlc["high"].rolling(config.exit_period).max().iloc[-1])
    next_add_level = entry_price + sign * config.add_step_n * n_at_entry
    unit_now = unit_shares(equity, n_now, last_close, config)
    unit_at_entry = unit_shares(equity, n_at_entry, entry_price, config)
    # Signed like ``shares``, so a short's units are negative.
    units_held = shares / unit_at_entry if unit_at_entry else float("nan")
    # Negative shares flip the sign, so this is the loss to the stop either way.
    open_risk = max(shares * (last_close - stop_price), 0.0)
    exit_event = first_exit(ohlc, entry_ts, entry_price, n_at_entry, config, direction)

    if exit_event is not None and exit_event.reason is ExitReason.STOP:
        action = PositionAction.EXIT_STOP
    elif exit_event is not None:
        action = PositionAction.EXIT_CHANNEL
    elif (
        _at_or_beyond(last_close, next_add_level, sign)
        and abs(units_held) < config.max_units_per_ticker - 0.5
    ):
        action = PositionAction.ADD_UNIT
    else:
        action = PositionAction.HOLD

    return PositionStatus(
        ticker=ticker,
        last_date=last_date,
        last_close=last_close,
        shares=shares,
        entry_price=entry_price,
        entry_date=entry_ts,
        n_at_entry=n_at_entry,
        n_now=n_now,
        stop_price=stop_price,
        exit_level=exit_level,
        next_add_level=next_add_level,
        # Both positive while the trade is safe / in profit, for either direction.
        distance_to_stop_n=sign * (last_close - stop_price) / n_now,
        open_r_multiple=sign
        * (last_close - entry_price)
        / (config.stop_n * n_at_entry),
        open_risk_dollars=open_risk,
        open_risk_pct_equity=open_risk / equity if equity else float("nan"),
        units_held=units_held,
        unit_shares_now=unit_now,
        action=action,
        first_exit=exit_event,
        direction=direction,
    )


@dataclass(frozen=True)
class _HypotheticalBreakout:
    entry_index: int
    exit_index: int
    winner: bool


def _hypothetical_breakouts(
    ohlc: pd.DataFrame, config: TurtleConfig, direction: Direction = Direction.LONG
) -> list[_HypotheticalBreakout]:
    """Every entry-period breakout taken in sequence, whether or not it was traded.

    System 1 skips a breakout when the previous one was a winner, judged on
    this hypothetical sequence (the book counts skipped breakouts too). Long
    and short breakouts are separate sequences: a short is skipped after a
    winning short breakout.
    """
    sign = direction.value
    highs, lows, opens = (
        ohlc["high"].to_numpy(),
        ohlc["low"].to_numpy(),
        ohlc["open"].to_numpy(),
    )
    entry_level = _entry_channel(ohlc, config.entry_period, direction).to_numpy()
    exit_level = _exit_channel(ohlc, config.exit_period, direction).to_numpy()
    n_prev = turtle_n(ohlc, config.n_period).shift(1).to_numpy()

    events: list[_HypotheticalBreakout] = []
    i = 0
    while i < len(ohlc):
        if (
            np.isnan(entry_level[i])
            or np.isnan(n_prev[i])
            or not _breaks(highs[i], lows[i], entry_level[i], sign)
        ):
            i += 1
            continue
        fill = _fill_through(opens[i], entry_level[i], sign)
        stop = fill - sign * config.stop_n * n_prev[i]
        exit_index, exit_price = len(ohlc) - 1, float(ohlc["close"].iloc[-1])
        for j in range(i + 1, len(ohlc)):
            if _touches(highs[j], lows[j], stop, -sign):
                exit_index, exit_price = j, _fill_through(opens[j], stop, -sign)
                break
            if not np.isnan(exit_level[j]) and _breaks(
                highs[j], lows[j], exit_level[j], -sign
            ):
                exit_index, exit_price = (
                    j,
                    _fill_through(opens[j], exit_level[j], -sign),
                )
                break
        else:
            # Still open at the end of the data: unresolved, so it can't mark a winner.
            return events
        winner = exit_price > fill if sign > 0 else exit_price < fill
        events.append(_HypotheticalBreakout(i, exit_index, winner))
        i = exit_index + 1
    return events


def _last_resolved_was_winner(events: list[_HypotheticalBreakout], index: int) -> bool:
    resolved = [e for e in events if e.exit_index < index]
    return bool(resolved) and resolved[-1].winner


@dataclass(frozen=True)
class BreakoutSignal:
    ticker: str
    last_date: pd.Timestamp
    last_close: float
    entry_level: float
    n: float
    unit_shares: int
    stop_if_filled: float
    triggered: bool
    distance_n: float
    trend_ok: bool
    skipped_after_winner: bool
    direction: Direction = Direction.LONG


def _trend_ok(
    ohlc: pd.DataFrame, config: TurtleConfig, direction: Direction = Direction.LONG
) -> pd.Series:
    fast = ema(ohlc["close"], config.trend_fast_period)
    slow = ema(ohlc["close"], config.trend_slow_period)
    with_trend = fast > slow if direction is Direction.LONG else fast < slow
    # Before the slow average has enough history the filter can't judge, so it allows the trade.
    return with_trend | slow.isna()


def scan_breakouts(
    histories: dict[str, pd.DataFrame],
    equity: float,
    config: TurtleConfig,
    directions: tuple[Direction, ...] = (Direction.LONG,),
) -> list[BreakoutSignal]:
    """Each ticker's entry level for the next session, and whether the last bar already broke it.

    One signal per ticker per requested direction. A short's ``entry_level`` is
    the N-day low, its ``stop_if_filled`` is 2N above that, and ``distance_n``
    is how far price must still fall to reach it (negative once broken), so
    both directions sort together: triggered first, then nearest.
    """
    signals: list[BreakoutSignal] = []
    for ticker, ohlc in histories.items():
        if len(ohlc) < config.entry_period + config.n_period:
            continue
        n_now = float(turtle_n(ohlc, config.n_period).iloc[-1])
        last_close = float(ohlc["close"].iloc[-1])
        last_high = float(ohlc["high"].iloc[-1])
        last_low = float(ohlc["low"].iloc[-1])
        for direction in directions:
            sign = direction.value
            skipped = config.skips_after_winner and _last_resolved_was_winner(
                _hypothetical_breakouts(ohlc, config, direction), len(ohlc)
            )
            period = config.failsafe_period if skipped else config.entry_period
            prior_level = float(_entry_channel(ohlc, period, direction).iloc[-1])
            # Tomorrow's level includes today's bar, hence no shift.
            if direction is Direction.LONG:
                next_level = float(ohlc["high"].rolling(period).max().iloc[-1])
            else:
                next_level = float(ohlc["low"].rolling(period).min().iloc[-1])
            triggered = _breaks(last_high, last_low, prior_level, sign)
            entry_level = prior_level if triggered else next_level
            signals.append(
                BreakoutSignal(
                    ticker=ticker,
                    last_date=ohlc.index[-1],
                    last_close=last_close,
                    entry_level=entry_level,
                    n=n_now,
                    unit_shares=unit_shares(equity, n_now, entry_level, config),
                    stop_if_filled=entry_level - sign * config.stop_n * n_now,
                    triggered=triggered,
                    distance_n=sign * (entry_level - last_close) / n_now,
                    trend_ok=bool(_trend_ok(ohlc, config, direction).iloc[-1]),
                    skipped_after_winner=skipped,
                    direction=direction,
                )
            )
    return sorted(signals, key=lambda s: (not s.triggered, s.distance_n))


def correlation_matrix(
    histories: dict[str, pd.DataFrame], lookback_days: int = TRADING_DAYS_PER_YEAR
) -> pd.DataFrame:
    closes = pd.DataFrame({t: h["close"] for t, h in histories.items()})
    return closes.pct_change().tail(lookback_days).corr()


def _units_in_group(
    units_by_ticker: dict[str, float],
    correlations: pd.DataFrame,
    ticker: str,
    threshold: float,
) -> float:
    """Units on the same side as ``ticker`` across tickers correlated at ``threshold`` or more.

    Returned as a positive magnitude; a short's correlated group only counts shorts.
    """
    side = direction_of(units_by_ticker.get(ticker, 0.0))
    same_side = {
        other: abs(units)
        for other, units in units_by_ticker.items()
        if direction_of(units) is side
    }
    if ticker not in correlations:
        return same_side.get(ticker, 0.0)
    return sum(
        units
        for other, units in same_side.items()
        if other == ticker
        or (other in correlations and correlations.loc[ticker, other] >= threshold)
    )


def unit_limit_breaches(
    units_by_ticker: dict[str, float], correlations: pd.DataFrame, config: TurtleConfig
) -> list[str]:
    """The book's four unit limits, checked against current holdings.

    Units are signed like shares: negative for a short. Longs and shorts are
    counted separately, so the correlated-group and one-direction limits apply
    to each side on its own (the book's "in a single direction"). Breach
    messages for shorts say "short".
    """
    breaches: list[str] = []
    for ticker, units in units_by_ticker.items():
        short = direction_of(units) is Direction.SHORT
        label = " short" if short else ""
        size = abs(units)
        if size > config.max_units_per_ticker:
            breaches.append(
                f"{ticker}: {size:.1f}{label} units > {config.max_units_per_ticker} per ticker"
            )
        close_units = _units_in_group(
            units_by_ticker, correlations, ticker, config.close_correlation
        )
        if close_units > config.max_units_closely_correlated:
            breaches.append(
                f"{ticker}{label} group (ρ ≥ {config.close_correlation}): {close_units:.1f} units > "
                f"{config.max_units_closely_correlated}"
            )
        loose_units = _units_in_group(
            units_by_ticker, correlations, ticker, config.loose_correlation
        )
        if loose_units > config.max_units_loosely_correlated:
            breaches.append(
                f"{ticker}{label} group (ρ ≥ {config.loose_correlation}): {loose_units:.1f} units > "
                f"{config.max_units_loosely_correlated}"
            )
    total_long = sum(u for u in units_by_ticker.values() if u >= 0)
    if total_long > config.max_units_one_direction:
        breaches.append(
            f"Total long units {total_long:.1f} > {config.max_units_one_direction}"
        )
    total_short = -sum(u for u in units_by_ticker.values() if u < 0)
    if total_short > config.max_units_one_direction:
        breaches.append(
            f"Total short units {total_short:.1f} > {config.max_units_one_direction}"
        )
    return breaches


class NotionalAccount:
    """Notional equity with the Turtles' drawdown rule.

    Each time equity falls another ``drawdown_step`` of the current notional
    size, the notional size is cut by ``drawdown_cut``. It returns to full size
    once equity regains the yearly starting equity.
    """

    def __init__(self, starting_equity: float, config: TurtleConfig) -> None:
        self._config = config
        self.start_year(starting_equity)

    def start_year(self, equity: float) -> None:
        self.year_start = equity
        self.notional = equity
        self._next_cut_at = equity * (1 - self._config.drawdown_step)

    def update(self, equity: float) -> float:
        if equity >= self.year_start:
            self.start_year(self.year_start)
        while equity <= self._next_cut_at:
            self.notional *= 1 - self._config.drawdown_cut
            self._next_cut_at -= self._config.drawdown_step * self.notional
        return self.notional


@dataclass
class _Unit:
    shares: int
    fill: float
    stop: float


@dataclass
class _OpenPosition:
    ticker: str
    entry_date: pd.Timestamp
    n_at_entry: float
    shares_per_unit: int
    units: list[_Unit] = field(default_factory=list)
    units_added: int = 0
    shares_bought: int = 0
    accepting_adds: bool = True
    realized: float = 0.0
    cost: float = 0.0
    direction: Direction = Direction.LONG

    @property
    def shares(self) -> int:
        """Share count as a positive magnitude, whichever the direction."""
        return sum(u.shares for u in self.units)

    @property
    def signed_shares(self) -> int:
        return self.direction.value * self.shares


@dataclass(frozen=True)
class BacktestTrade:
    ticker: str
    entry_date: pd.Timestamp
    exit_date: pd.Timestamp
    units: int
    avg_entry: float
    pnl: float
    r_multiple: float
    reason: ExitReason | None
    direction: Direction = Direction.LONG


@dataclass(frozen=True)
class BacktestResult:
    config: TurtleConfig
    equity: pd.Series
    trades: list[BacktestTrade]
    stats: dict[str, float]


def performance_stats(
    equity: pd.Series, trades: list[BacktestTrade]
) -> dict[str, float]:
    """CAGR, max drawdown, MAR, R² of log equity (the book's robustness measures) and trade stats."""
    years = max((equity.index[-1] - equity.index[0]).days / 365.25, 1 / 365.25)
    cagr = (equity.iloc[-1] / equity.iloc[0]) ** (1 / years) - 1
    drawdown = equity / equity.cummax() - 1
    max_drawdown = float(-drawdown.min())
    log_equity = np.log(equity.to_numpy())
    r_squared = (
        float(np.corrcoef(np.arange(len(log_equity)), log_equity)[0, 1] ** 2)
        if len(equity) > 2
        else 0.0
    )
    wins = [t.pnl for t in trades if t.pnl > 0]
    losses = [t.pnl for t in trades if t.pnl <= 0]
    gross_loss = -sum(losses)
    return {
        "cagr": float(cagr),
        "max_drawdown": max_drawdown,
        "mar": float(cagr / max_drawdown) if max_drawdown > 0 else float("nan"),
        "r_squared": 0.0 if np.isnan(r_squared) else r_squared,
        "trades": float(len(trades)),
        "win_rate": len(wins) / len(trades) if trades else 0.0,
        "avg_win": float(np.mean(wins)) if wins else 0.0,
        "avg_loss": float(np.mean(losses)) if losses else 0.0,
        "profit_factor": sum(wins) / gross_loss if gross_loss > 0 else float("nan"),
        "avg_r": float(np.mean([t.r_multiple for t in trades])) if trades else 0.0,
        "final_equity": float(equity.iloc[-1]),
    }


@dataclass
class _SideSeries:
    """One direction's channels, trend filter and hypothetical breakouts for a ticker."""

    entry: pd.Series
    failsafe: pd.Series
    exit: pd.Series
    trend_ok_prev: pd.Series
    breakouts: list[_HypotheticalBreakout]


@dataclass
class _TickerSeries:
    open: pd.Series
    high: pd.Series
    low: pd.Series
    close: pd.Series
    n_prev: pd.Series
    sides: dict[Direction, _SideSeries]
    positions: dict[pd.Timestamp, int]


def _backtest_directions(config: TurtleConfig) -> tuple[Direction, ...]:
    # Long first: on the rare bar that breaks both channels, the long is taken.
    if config.allow_short:
        return (Direction.LONG, Direction.SHORT)
    return (Direction.LONG,)


def _prepare_side(
    ohlc: pd.DataFrame, config: TurtleConfig, direction: Direction
) -> _SideSeries:
    return _SideSeries(
        entry=_entry_channel(ohlc, config.entry_period, direction),
        failsafe=_entry_channel(ohlc, config.failsafe_period, direction),
        exit=_exit_channel(ohlc, config.exit_period, direction),
        trend_ok_prev=(
            _trend_ok(ohlc, config, direction).shift(1, fill_value=True)
            if config.use_trend_filter
            else pd.Series(True, index=ohlc.index)
        ),
        breakouts=_hypothetical_breakouts(ohlc, config, direction)
        if config.skips_after_winner
        else [],
    )


def _prepare(ohlc: pd.DataFrame, config: TurtleConfig) -> _TickerSeries:
    return _TickerSeries(
        open=ohlc["open"],
        high=ohlc["high"],
        low=ohlc["low"],
        close=ohlc["close"],
        n_prev=turtle_n(ohlc, config.n_period).shift(1),
        sides={d: _prepare_side(ohlc, config, d) for d in _backtest_directions(config)},
        positions={when: i for i, when in enumerate(ohlc.index)},
    )


def backtest(
    histories: dict[str, pd.DataFrame],
    config: TurtleConfig,
    starting_equity: float = 100_000.0,
) -> BacktestResult:
    """Daily portfolio simulation of the Turtle rules on shared, unlevered equity.

    With ``config.allow_short`` it also takes the mirrored short entries. A
    ticker holds one position at a time; a short credits its sale proceeds to
    cash and counts its market value toward ``max_gross_exposure``.
    """
    directions = _backtest_directions(config)
    series = {
        t: _prepare(h, config)
        for t, h in histories.items()
        if len(h) > config.entry_period
    }
    calendar = (
        sorted(set().union(*(s.close.index for s in series.values()))) if series else []
    )
    correlations = correlation_matrix(
        {t: histories[t] for t in series}, lookback_days=len(calendar)
    )
    account = NotionalAccount(starting_equity, config)
    cash = starting_equity
    equity = starting_equity
    last_close: dict[str, float] = {}
    open_positions: dict[str, _OpenPosition] = {}
    trades: list[BacktestTrade] = []
    equity_curve: dict[pd.Timestamp, float] = {}
    current_year = calendar[0].year if calendar else 0

    def close_position(
        pos: _OpenPosition, when: pd.Timestamp, reason: ExitReason | None
    ) -> None:
        initial_risk = pos.shares_per_unit * config.stop_n * pos.n_at_entry
        trades.append(
            BacktestTrade(
                ticker=pos.ticker,
                entry_date=pos.entry_date,
                exit_date=when,
                units=pos.units_added,
                avg_entry=pos.cost / pos.shares_bought,
                pnl=pos.realized,
                r_multiple=pos.realized / initial_risk if initial_risk else 0.0,
                reason=reason,
                direction=pos.direction,
            )
        )
        del open_positions[pos.ticker]

    for when in calendar:
        if when.year != current_year:
            current_year = when.year
            account.start_year(equity)
        notional = account.update(equity)

        for ticker, pos in list(open_positions.items()):
            s = series[ticker]
            if when not in s.positions:
                continue
            # +1 for a long, -1 for a short; exits trigger on moves against ``sign``.
            sign = pos.direction.value
            bar_open, bar_high, bar_low = (
                float(s.open[when]),
                float(s.high[when]),
                float(s.low[when]),
            )
            survivors: list[_Unit] = []
            for unit in pos.units:
                if _touches(bar_high, bar_low, unit.stop, -sign):
                    price = _fill_through(bar_open, unit.stop, -sign)
                    cash += sign * unit.shares * price
                    pos.realized += sign * unit.shares * (price - unit.fill)
                else:
                    survivors.append(unit)
            if len(survivors) < len(pos.units):
                pos.accepting_adds = False
            pos.units = survivors
            if not pos.units:
                close_position(pos, when, ExitReason.STOP)
                continue
            level = s.sides[pos.direction].exit[when]
            if not np.isnan(level) and _breaks(bar_high, bar_low, float(level), -sign):
                price = _fill_through(bar_open, float(level), -sign)
                for unit in pos.units:
                    cash += sign * unit.shares * price
                    pos.realized += sign * unit.shares * (price - unit.fill)
                pos.units = []
                close_position(pos, when, ExitReason.CHANNEL)

        for ticker in sorted(series):
            s = series[ticker]
            if when not in s.positions:
                continue
            n = s.n_prev[when]
            if np.isnan(n) or n <= 0:
                continue
            bar_open, bar_high, bar_low = (
                float(s.open[when]),
                float(s.high[when]),
                float(s.low[when]),
            )
            pos = open_positions.get(ticker)

            if pos is None:
                for direction in directions:
                    side = s.sides[direction]
                    trigger = side.entry[when]
                    if config.skips_after_winner and _last_resolved_was_winner(
                        side.breakouts, s.positions[when]
                    ):
                        trigger = side.failsafe[when]
                    if (
                        np.isnan(trigger)
                        or not _breaks(
                            bar_high, bar_low, float(trigger), direction.value
                        )
                        or not bool(side.trend_ok_prev[when])
                    ):
                        continue
                    fill = _fill_through(bar_open, float(trigger), direction.value)
                    shares = unit_shares(notional, float(n), fill, config)
                    if shares < 1:
                        continue
                    pos = _OpenPosition(
                        ticker, when, float(n), shares, direction=direction
                    )
                    next_trigger = fill
                    break
                if pos is None:
                    continue
                sign = pos.direction.value
            else:
                sign = pos.direction.value
                next_trigger = (
                    pos.units[-1].fill + sign * config.add_step_n * pos.n_at_entry
                )

            while (
                pos.accepting_adds
                and _touches(bar_high, bar_low, next_trigger, sign)
                and pos.units_added < config.max_units_per_ticker
            ):
                # Signed units, so longs and shorts count against separate limits.
                units_by_ticker = {
                    t: float(p.direction.value * len(p.units))
                    for t, p in open_positions.items()
                }
                units_by_ticker[ticker] = float(sign * (len(pos.units) + 1))
                if unit_limit_breaches(units_by_ticker, correlations, config):
                    break
                fill = _fill_through(bar_open, next_trigger, sign)
                invested = sum(
                    p.shares * last_close.get(t, p.units[0].fill)
                    for t, p in open_positions.items()
                )
                if (
                    invested + pos.shares_per_unit * fill
                    > config.max_gross_exposure * notional
                ):
                    break
                for unit in pos.units:
                    unit.stop += sign * config.add_step_n * pos.n_at_entry
                pos.units.append(
                    _Unit(
                        pos.shares_per_unit,
                        fill,
                        fill - sign * config.stop_n * pos.n_at_entry,
                    )
                )
                pos.units_added += 1
                pos.shares_bought += pos.shares_per_unit
                pos.cost += pos.shares_per_unit * fill
                # A buy spends cash; a short sale receives it.
                cash -= sign * pos.shares_per_unit * fill
                open_positions[ticker] = pos
                next_trigger = fill + sign * config.add_step_n * pos.n_at_entry

        for ticker, s in series.items():
            if when in s.positions:
                last_close[ticker] = float(s.close[when])
        equity = cash + sum(
            p.signed_shares * last_close[t] for t, p in open_positions.items()
        )
        equity_curve[when] = equity

    for ticker, pos in list(open_positions.items()):
        price = last_close[ticker]
        sign = pos.direction.value
        for unit in pos.units:
            pos.realized += sign * unit.shares * (price - unit.fill)
        close_position(pos, calendar[-1], None)

    curve = pd.Series(equity_curve, dtype=float)
    stats = performance_stats(curve, trades) if len(curve) > 1 else {}
    return BacktestResult(config=config, equity=curve, trades=trades, stats=stats)
