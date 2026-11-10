import numpy as np
import pandas as pd
import pytest

from src.analysis.recommendations import (
    BLOCKED_HEAT,
    BLOCKED_INSUFFICIENT_CASH,
    BLOCKED_NO_CASH,
    BLOCKED_NO_SHORT_ACCOUNT,
    WEIGHT_ADX,
    WEIGHT_PROXIMITY,
    WEIGHT_RELATIVE_STRENGTH,
    WEIGHT_TREND_FILTER,
    AccountInfo,
    AccountPosition,
    RecommendationAction,
    TrendScore,
    build_recommendations,
    close_above_sma,
    entry_signal_key,
    market_regime,
    position_signal_key,
    trend_features,
    trend_scores,
)
from src.analysis.turtle import (
    BreakoutSignal,
    Direction,
    ExitEvent,
    ExitReason,
    PositionAction,
    PositionStatus,
    TurtleConfig,
    evaluate_position,
)

A = RecommendationAction
EQUITY = 100_000.0
CONFIG = TurtleConfig.system_2()
LAST_DATE = pd.Timestamp("2026-11-06")


def _bars(closes, half_range: float = 1.0, start: str = "2023-01-02") -> pd.DataFrame:
    closes = np.asarray(closes, dtype=float)
    return pd.DataFrame(
        {
            "open": closes,
            "high": closes + half_range,
            "low": closes - half_range,
            "close": closes,
        },
        index=pd.bdate_range(start, periods=len(closes)),
    )


def _status(
    ticker: str,
    action: PositionAction = PositionAction.HOLD,
    *,
    shares: float = 100.0,
    open_risk: float = 0.0,
    units: float = 1.0,
    first_exit: ExitEvent | None = None,
) -> PositionStatus:
    sign = 1 if shares >= 0 else -1
    return PositionStatus(
        ticker=ticker,
        last_date=LAST_DATE,
        last_close=110.0,
        shares=shares,
        entry_price=100.0,
        entry_date=pd.Timestamp("2026-09-01"),
        n_at_entry=2.0,
        n_now=2.0,
        stop_price=100.0 - sign * 4.0,
        exit_level=105.0,
        next_add_level=100.0 + sign * 1.0,
        distance_to_stop_n=7.0,
        open_r_multiple=2.5,
        open_risk_dollars=open_risk,
        open_risk_pct_equity=open_risk / EQUITY,
        units_held=units,
        unit_shares_now=100,
        action=action,
        first_exit=first_exit,
        direction=Direction.LONG if shares >= 0 else Direction.SHORT,
    )


def _signal(
    ticker: str,
    *,
    triggered: bool = True,
    distance_n: float = -0.2,
    direction: Direction = Direction.LONG,
    entry: float = 100.0,
    n: float = 2.0,
    unit_shares: int = 100,
    skipped: bool = False,
) -> BreakoutSignal:
    sign = direction.value
    return BreakoutSignal(
        ticker=ticker,
        last_date=LAST_DATE,
        last_close=entry + sign * 0.4,
        entry_level=entry,
        n=n,
        unit_shares=unit_shares,
        stop_if_filled=entry - sign * 2 * n,
        triggered=triggered,
        distance_n=distance_n,
        trend_ok=True,
        skipped_after_winner=skipped,
        direction=direction,
    )


def _accounts(*accounts: AccountInfo) -> list[AccountInfo]:
    return list(accounts) or [AccountInfo("taxable", cash=1e9, allow_short=True)]


def _build(positions=(), signals=(), accounts=None, **kwargs):
    defaults = dict(
        accounts=_accounts() if accounts is None else accounts,
        total_equity=EQUITY,
        config=CONFIG,
        max_heat_pct=0.10,
    )
    defaults.update(kwargs)
    return build_recommendations(
        position_statuses=list(positions), breakout_signals=list(signals), **defaults
    )


def _by_ticker(recs):
    return {r.ticker: r for r in recs}


def test_exits_rank_above_adds_above_entries_above_watch_above_hold():
    recs = _build(
        positions=[
            AccountPosition("taxable", _status("HLD")),
            AccountPosition("taxable", _status("ADD", PositionAction.ADD_UNIT)),
            AccountPosition("taxable", _status("CHN", PositionAction.EXIT_CHANNEL)),
            AccountPosition("taxable", _status("STP", PositionAction.EXIT_STOP)),
        ],
        signals=[_signal("WCH", triggered=False, distance_n=0.5), _signal("ENT")],
    )
    assert [r.action for r in recs] == [
        A.EXIT_STOP,
        A.EXIT_CHANNEL,
        A.ADD_UNIT,
        A.ENTER,
        A.WATCH,
        A.HOLD,
    ]
    assert all(r.account == "taxable" for r in recs)


def test_entries_rank_by_trend_score_within_their_group():
    scores = {
        ("LOW", Direction.LONG): TrendScore(40.0, 0.3),
        ("HIGH", Direction.LONG): TrendScore(90.0, 0.95),
    }
    recs = _build(
        signals=[_signal("LOW"), _signal("NONE"), _signal("HIGH")], scores=scores
    )
    assert [r.ticker for r in recs] == ["HIGH", "LOW", "NONE"]
    assert recs[0].trend_score == 90.0 and recs[0].rs_rank == 0.95


def test_held_tickers_are_excluded_from_entries_in_either_direction():
    recs = _build(
        positions=[AccountPosition("taxable", _status("AAPL"))],
        signals=[
            _signal("AAPL"),
            _signal("AAPL", direction=Direction.SHORT),
            _signal("TSLA"),  # held but couldn't be evaluated
            _signal("MSFT"),
        ],
        open_positions_by_ticker={
            "AAPL": {Direction.LONG},
            "TSLA": {Direction.SHORT},
        },
    )
    entries = [r for r in recs if r.is_entry]
    assert [r.ticker for r in entries] == ["MSFT"]


def test_heat_budget_blocks_lower_ranked_entries():
    # Budget 10% of 100k = 10k; 6k open; each entry risks 500 x 4 = 2k.
    scores = {
        (t, Direction.LONG): TrendScore(s, None)
        for t, s in [("A1", 90), ("A2", 80), ("A3", 70)]
    }
    recs = _build(
        positions=[AccountPosition("taxable", _status("OLD", open_risk=6_000.0))],
        signals=[_signal(t, unit_shares=500) for t in ("A3", "A1", "A2")],
        scores=scores,
    )
    by = _by_ticker(recs)
    assert by["A1"].blocked_by == () and by["A2"].blocked_by == ()
    assert by["A1"].risk_dollars == pytest.approx(2_000.0)
    assert by["A3"].blocked_by[0].startswith(BLOCKED_HEAT)
    assert by["A3"].account == "taxable"


def test_watch_is_checked_against_the_budget_but_does_not_use_it():
    recs = _build(
        positions=[AccountPosition("taxable", _status("OLD", open_risk=7_000.0))],
        signals=[
            _signal("W1", triggered=False, distance_n=0.3, unit_shares=500),
            _signal("W2", triggered=False, distance_n=0.4, unit_shares=500),
            _signal("W3", triggered=False, distance_n=0.5, unit_shares=1_000),
        ],
    )
    by = _by_ticker(recs)
    assert by["W1"].blocked_by == () and by["W2"].blocked_by == ()
    assert by["W3"].blocked_by[0].startswith(BLOCKED_HEAT)


def test_correlated_group_breach_is_blocked_but_unrelated_breaches_are_not():
    tickers = ["AAA", "BBB", "CCC", "ZZZ", "XXX"]
    correlations = pd.DataFrame(0.0, index=tickers, columns=tickers)
    for a in ("AAA", "BBB", "CCC"):
        for b in ("AAA", "BBB", "CCC"):
            correlations.loc[a, b] = 0.9
    for t in tickers:
        correlations.loc[t, t] = 1.0
    recs = _build(
        positions=[
            AccountPosition("taxable", _status("AAA", units=3.0)),
            AccountPosition("taxable", _status("BBB", units=3.0)),
            # Already over the 4-per-ticker limit, which has nothing to do with XXX.
            AccountPosition("taxable", _status("ZZZ", units=4.5)),
        ],
        signals=[_signal("CCC"), _signal("XXX")],
        correlations=correlations,
    )
    by = _by_ticker(recs)
    assert any("group (ρ ≥ 0.7)" in b for b in by["CCC"].blocked_by)
    assert by["XXX"].blocked_by == ()


def test_unit_limits_aggregate_a_ticker_across_accounts():
    recs = _build(
        positions=[
            AccountPosition("roth", _status("AAA", PositionAction.ADD_UNIT, units=2.0)),
            AccountPosition("taxable", _status("AAA", units=2.0)),
        ],
        accounts=[
            AccountInfo("roth", cash=1e9, allow_short=False),
            AccountInfo("taxable", cash=1e9, allow_short=True),
        ],
    )
    add = next(r for r in recs if r.action is A.ADD_UNIT)
    assert add.account == "roth"
    assert any("per ticker" in b for b in add.blocked_by)


def test_add_unit_needs_cash_in_its_own_account():
    recs = _build(
        positions=[AccountPosition("roth", _status("AAA", PositionAction.ADD_UNIT))],
        accounts=[AccountInfo("roth", cash=50.0, allow_short=False)],
    )
    assert recs[0].blocked_by == (f"{BLOCKED_INSUFFICIENT_CASH} roth",)
    # The add's ticket: 100 shares at the add level, stop 2N below it.
    assert recs[0].trigger_price == pytest.approx(101.0)
    assert recs[0].stop_price == pytest.approx(97.0)
    assert recs[0].risk_dollars == pytest.approx(400.0)


def test_shorts_route_only_to_short_enabled_accounts():
    roth = AccountInfo("Roth IRA", cash=1e9, allow_short=False)
    taxable = AccountInfo("taxable", cash=0.0, allow_short=True)
    short = _signal("XOM", direction=Direction.SHORT, distance_n=-0.2)
    rec = _build(signals=[short], accounts=[roth, taxable])[0]
    assert rec.account == "taxable" and rec.blocked_by == ()

    rec = _build(signals=[short], accounts=[roth])[0]
    assert rec.account is None
    assert rec.blocked_by == (BLOCKED_NO_SHORT_ACCOUNT,)


def test_short_order_ticket():
    rec = _build(
        signals=[_signal("XOM", direction=Direction.SHORT, entry=50.0, n=1.0)]
    )[0]
    assert rec.direction is Direction.SHORT
    assert rec.trigger_price == pytest.approx(50.0)
    assert rec.stop_price == pytest.approx(52.0)
    assert rec.risk_dollars == pytest.approx(200.0)
    assert rec.extra["order_type"] == "sell short stop"
    assert rec.extra["check_borrow"] is True
    assert rec.extra["limit_hint"] == pytest.approx(49.75)
    assert rec.extra["value"] == pytest.approx(5_000.0)
    assert rec.extra["entry_level"] == pytest.approx(50.0)
    assert "check borrow" in rec.reasons
    assert rec.reasons[0] == "55-day low breakout"


def test_long_order_ticket_and_reasons():
    features = {
        ("AAPL", Direction.LONG): trend_features(
            _bars(np.linspace(50, 100, 400)), Direction.LONG, "AAPL"
        )
    }
    scores = {("AAPL", Direction.LONG): TrendScore(88.0, 0.92)}
    rec = _build(
        signals=[_signal("AAPL", entry=100.0, n=2.0)], features=features, scores=scores
    )[0]
    assert rec.extra["order_type"] == "buy stop"
    assert rec.extra["check_borrow"] is False
    assert rec.extra["limit_hint"] == pytest.approx(100.5)
    # 52-week high is 101 (close 100 + 1); R = (101 - 100) / 4.
    assert rec.extra["r_to_extreme"] == pytest.approx(0.25)
    assert rec.reasons[0] == "55-day high breakout"
    assert "RS 92nd pct" in rec.reasons
    assert "trend filter aligned" in rec.reasons
    assert any(r.startswith("ADX ") for r in rec.reasons)


def test_longs_prefer_a_roth_with_enough_cash_then_fall_back_to_taxable():
    taxable = AccountInfo("taxable", cash=1e6, allow_short=True)
    roth = AccountInfo("Kevin Roth IRA", cash=15_000.0, allow_short=False)
    scores = {
        ("AAA", Direction.LONG): TrendScore(90.0, None),
        ("BBB", Direction.LONG): TrendScore(80.0, None),
    }
    # Each entry is 100 shares x $100 = $10k; the Roth can fund only the first.
    recs = _build(
        signals=[_signal("AAA"), _signal("BBB")],
        accounts=[taxable, roth],
        scores=scores,
    )
    by = _by_ticker(recs)
    assert by["AAA"].account == "Kevin Roth IRA"
    assert by["BBB"].account == "taxable"


def test_entry_without_a_fundable_account_is_blocked():
    rec = _build(
        signals=[_signal("AAA")],
        accounts=[AccountInfo("roth", cash=100.0, allow_short=False)],
    )[0]
    assert rec.account is None
    assert rec.blocked_by == (BLOCKED_NO_CASH,)


def test_watch_versus_enter():
    recs = _build(
        signals=[
            _signal("ENT", triggered=True, distance_n=-0.3),
            _signal("WCH", triggered=False, distance_n=0.8),
            _signal("FAR", triggered=False, distance_n=1.5),
            _signal("SKP", triggered=True, skipped=True),
        ]
    )
    assert {r.ticker: r.action for r in recs} == {"ENT": A.ENTER, "WCH": A.WATCH}
    watch = _by_ticker(recs)["WCH"]
    assert watch.reasons[0] == "0.80N from 55-day high"


def test_signal_keys():
    recs = _build(
        positions=[
            AccountPosition("roth", _status("STP", PositionAction.EXIT_STOP)),
            AccountPosition("roth", _status("CHN", PositionAction.EXIT_CHANNEL)),
            AccountPosition("roth", _status("ADD", PositionAction.ADD_UNIT)),
            AccountPosition("taxable", _status("HLD", shares=-50.0)),
        ],
        signals=[_signal("ENT"), _signal("WCH", triggered=False, distance_n=0.5)],
    )
    assert {r.ticker: r.signal_key for r in recs} == {
        "STP": "roth|STP|LONG|EXIT",
        "CHN": "roth|CHN|LONG|EXIT",
        "ADD": "roth|ADD|LONG|ADD",
        "HLD": "taxable|HLD|SHORT|HOLD",
        "ENT": "ENT|LONG|ENTRY",
        "WCH": "WCH|LONG|ENTRY",
    }
    assert entry_signal_key("WCH", Direction.LONG) == "WCH|LONG|ENTRY"
    assert (
        position_signal_key("roth", "X", Direction.SHORT, A.EXIT_CHANNEL)
        == "roth|X|SHORT|EXIT"
    )


def test_late_exit_passes_is_late_through():
    # Flat at 100, then a gap to 92 hits the 96 stop; two more bars later it is still held.
    ohlc = _bars([100.0] * 60 + [100.0, 92.0, 92.5, 93.0])
    status = evaluate_position("LATE", ohlc, 100, 100.0, ohlc.index[59], EQUITY, CONFIG)
    assert status.is_late
    rec = _build(positions=[AccountPosition("taxable", status)])[0]
    assert rec.action is A.EXIT_STOP
    assert rec.is_late
    assert rec.trigger_price == pytest.approx(92.0)
    assert any("still open" in r for r in rec.reasons)
    assert rec.extra["order_type"] == "sell"

    fresh = _status(
        "NOW",
        PositionAction.EXIT_STOP,
        first_exit=ExitEvent(LAST_DATE, 95.0, ExitReason.STOP),
    )
    rec = _build(positions=[AccountPosition("taxable", fresh)])[0]
    assert not rec.is_late


def test_trend_features_and_scores_rank_strength_by_direction():
    riser = _bars(np.linspace(50, 150, 400))
    laggard = _bars(np.linspace(100, 110, 400))
    faller = _bars(np.linspace(150, 50, 400))
    features = {
        (t, d): trend_features(ohlc, d, t)
        for t, ohlc in {"UP": riser, "MID": laggard, "DOWN": faller}.items()
        for d in Direction
    }
    up = features[("UP", Direction.LONG)]
    assert up.ema_aligned and up.roc_6m > 0 and up.distance_from_extreme < 0.01
    down_short = features[("DOWN", Direction.SHORT)]
    assert down_short.ema_aligned and down_short.extreme_52w == pytest.approx(49.0)

    scores = trend_scores(features)
    assert scores[("UP", Direction.LONG)].rs_rank == pytest.approx(1.0)
    assert scores[("DOWN", Direction.SHORT)].rs_rank == pytest.approx(1.0)
    assert scores[("UP", Direction.LONG)].score > scores[("MID", Direction.LONG)].score
    assert (
        scores[("DOWN", Direction.SHORT)].score > scores[("UP", Direction.SHORT)].score
    )
    assert all(0 <= s.score <= 100 for s in scores.values())
    assert (
        WEIGHT_RELATIVE_STRENGTH + WEIGHT_ADX + WEIGHT_PROXIMITY + WEIGHT_TREND_FILTER
        == 100
    )


def test_short_history_scores_without_failing():
    features = {
        ("NEW", Direction.LONG): trend_features(
            _bars([10.0] * 30), Direction.LONG, "NEW"
        )
    }
    score = trend_scores(features)[("NEW", Direction.LONG)]
    assert score.rs_rank is None and score.score == 0.0


def test_market_regime():
    rising = _bars(np.linspace(100, 200, 300))
    regime = market_regime(rising, 0.62)
    assert regime["benchmark_above_200d"] is True
    assert regime["label"] == "risk_on"
    assert regime["breadth_above_200d"] == pytest.approx(0.62)
    assert market_regime(rising, 0.3)["label"] == "mixed"
    falling = _bars(np.linspace(200, 100, 300))
    assert market_regime(falling, 0.3)["label"] == "risk_off"
    unknown = market_regime(None, None)
    assert unknown["label"] == "unknown" and unknown["benchmark_close"] is None
    assert close_above_sma(_bars([1.0] * 10)) is None
