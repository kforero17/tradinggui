import math
from datetime import datetime

from src.analysis.recommendations import (
    Recommendation,
    RecommendationAction,
    entry_signal_key,
    position_signal_key,
)
from src.analysis.signal_history import (
    SignalStatus,
    diff_runs,
    expired_signals,
    to_rows,
)
from src.analysis.turtle import Direction

A = RecommendationAction
FIRST = datetime(2026, 11, 1, 6, 0)
NOW = datetime(2026, 11, 8, 6, 0)


def _rec(
    ticker: str, action: A, account: str = "taxable", **overrides
) -> Recommendation:
    direction = overrides.pop("direction", Direction.LONG)
    key = (
        entry_signal_key(ticker, direction)
        if action in (A.ENTER, A.WATCH)
        else position_signal_key(account, ticker, direction, action)
    )
    fields = dict(
        ticker=ticker,
        action=action,
        account=None if action in (A.ENTER, A.WATCH) else account,
        direction=direction,
        signal_key=key,
        trigger_price=100.0,
        stop_price=96.0,
        shares=100,
        risk_dollars=400.0,
        risk_pct_equity=0.004,
        distance_n=0.5,
        trend_score=80.0,
        rs_rank=0.9,
    )
    fields.update(overrides)
    return Recommendation(**fields)


def _stored(rec: Recommendation, first_seen_at: datetime = FIRST) -> dict:
    """A recommendation as ``db.get_run_recommendations`` returns it."""
    return {
        "action": rec.action.name,
        "signal_key": rec.signal_key,
        "first_seen_at": first_seen_at,
        "ticker": rec.ticker,
    }


def _statuses(diffed) -> dict[str, tuple[SignalStatus, datetime]]:
    return {rec.signal_key: (status, seen) for rec, status, seen in diffed}


def test_first_run_is_all_new():
    current = [_rec("AAA", A.ENTER), _rec("BBB", A.HOLD)]
    diffed, expired = diff_runs(None, current, now=NOW)
    assert all(status is SignalStatus.NEW and seen == NOW for _, status, seen in diffed)
    assert expired == []


def test_new_still_valid_and_expired_with_first_seen_carried_forward():
    previous = [
        _stored(_rec("KEEP", A.ENTER)),
        _stored(_rec("GONE", A.WATCH)),
        _stored(_rec("OUT", A.EXIT_STOP)),
        _stored(_rec("OLDHOLD", A.HOLD)),
    ]
    current = [_rec("KEEP", A.ENTER), _rec("FRESH", A.ENTER)]
    diffed, expired = diff_runs(previous, current, now=NOW)
    statuses = _statuses(diffed)
    assert statuses["KEEP|LONG|ENTRY"] == (SignalStatus.STILL_VALID, FIRST)
    assert statuses["FRESH|LONG|ENTRY"] == (SignalStatus.NEW, NOW)
    # HOLD rows never expire.
    assert {r["signal_key"] for r in expired} == {
        "GONE|LONG|ENTRY",
        "taxable|OUT|LONG|EXIT",
    }


def test_watch_that_triggers_keeps_its_key_and_first_seen():
    previous = [_stored(_rec("AAA", A.WATCH))]
    diffed, expired = diff_runs(previous, [_rec("AAA", A.ENTER)], now=NOW)
    rec, status, seen = diffed[0]
    assert rec.action is A.ENTER
    assert (status, seen) == (SignalStatus.STILL_VALID, FIRST)
    assert expired == []


def test_stop_exit_that_becomes_a_channel_exit_is_the_same_signal():
    previous = [_stored(_rec("AAA", A.EXIT_CHANNEL))]
    diffed, _ = diff_runs(previous, [_rec("AAA", A.EXIT_STOP)], now=NOW)
    assert diffed[0][1:] == (SignalStatus.STILL_VALID, FIRST)


def test_hold_is_still_valid_after_the_first_run_even_when_unseen():
    previous = [_stored(_rec("AAA", A.ADD_UNIT))]
    diffed, expired = diff_runs(
        previous, [_rec("AAA", A.HOLD), _rec("BBB", A.HOLD)], now=NOW
    )
    assert _statuses(diffed) == {
        "taxable|AAA|LONG|HOLD": (SignalStatus.STILL_VALID, NOW),
        "taxable|BBB|LONG|HOLD": (SignalStatus.STILL_VALID, NOW),
    }
    # The add was done (or lapsed), so it expires.
    assert [r["signal_key"] for r in expired] == ["taxable|AAA|LONG|ADD"]


def test_a_run_without_an_entry_scan_does_not_expire_entries():
    previous = [
        _stored(_rec("ENT", A.ENTER)),
        _stored(_rec("WCH", A.WATCH)),
        _stored(_rec("OUT", A.EXIT_CHANNEL)),
        _stored(_rec("HLD", A.HOLD)),
    ]
    current = [_rec("HLD", A.HOLD)]
    _, expired = diff_runs(previous, current, now=NOW, include_entries=False)
    assert [r["signal_key"] for r in expired] == ["taxable|OUT|LONG|EXIT"]
    assert expired_signals(None, []) == []


def test_to_rows_matches_the_db_row_shape_and_drops_nan():
    rec = _rec(
        "XOM",
        A.ENTER,
        direction=Direction.SHORT,
        rs_rank=math.nan,
        reasons=("55-day low breakout",),
        blocked_by=("heat budget: 11.0% of equity > 10.0%",),
        extra={"r_to_extreme": math.nan, "n": 1.5, "nested": [math.inf]},
    )
    (row,) = to_rows([(rec, SignalStatus.NEW, NOW)])
    assert row["signal_key"] == "XOM|SHORT|ENTRY"
    assert (
        row["direction"] == "SHORT"
        and row["action"] == "ENTER"
        and row["status"] == "NEW"
    )
    assert row["first_seen_at"] == NOW
    assert row["rs_rank"] is None
    assert row["reasons"] == ["55-day low breakout"]
    assert row["blocked_by"] == ["heat budget: 11.0% of equity > 10.0%"]
    assert row["extra"] == {"r_to_extreme": None, "n": 1.5, "nested": [None]}
