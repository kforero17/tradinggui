from datetime import date

import numpy as np
import pandas as pd
import pytest

from src.analysis.trade_review import EntryType, review_trade, run_review, summarize
from src.analysis.turtle import TurtleConfig
from src.data.broker_import import Fill, Side, match_round_trips


def _bars(closes: list[float], half_range: float = 1.0) -> pd.DataFrame:
    closes_array = np.asarray(closes, dtype=float)
    index = pd.bdate_range("2024-01-01", periods=len(closes_array))
    return pd.DataFrame(
        {
            "open": closes_array,
            "high": closes_array + half_range,
            "low": closes_array - half_range,
            "close": closes_array,
            "stock_splits": 0.0,
        },
        index=index,
    )


def _trip(
    ohlc: pd.DataFrame,
    entry_index: int,
    exit_index: int,
    entry: float,
    exit_: float,
    shares: float = 10,
):
    fills = [
        Fill("Acct", ohlc.index[entry_index].date(), "XYZ", Side.BUY, shares, entry),
        Fill("Acct", ohlc.index[exit_index].date(), "XYZ", Side.SELL, shares, exit_),
    ]
    return match_round_trips(fills)[0][0]


def test_losing_trade_held_past_stop_shows_r_beyond_minus_one_and_rule_savings():
    # Flat at 100 for 220 days (N = 2), bought at 100, then slid to 80 before selling.
    slide = [100.0 - i for i in range(1, 21)]
    ohlc = _bars([100.0] * 220 + slide)
    trip = _trip(ohlc, 219, 239, 100.0, 80.0)
    review = review_trade(trip, ohlc, TurtleConfig.system_2())
    assert review.n_at_entry == pytest.approx(2.0)
    assert review.r_multiple == pytest.approx(-200 / 40)
    assert review.mae_pct == pytest.approx(-0.21)
    # The 20-day low (99) breaks first, on the next bar at 99.
    assert review.turtle_exit_reason == "channel"
    assert review.turtle_exit_date == ohlc.index[220].date()
    assert review.turtle_pnl == pytest.approx(-10.0)
    assert review.turtle_minus_actual == pytest.approx(190.0)


def test_entry_type_distinguishes_breakouts_from_downtrends():
    uptrend = [50.0 + 0.25 * i for i in range(240)]
    ohlc = _bars(uptrend)
    trip = _trip(ohlc, 230, 239, uptrend[230], uptrend[239])
    assert (
        review_trade(trip, ohlc, TurtleConfig.system_2()).entry_type
        is EntryType.BREAKOUT
    )

    downtrend = [200.0 - 0.25 * i for i in range(240)]
    ohlc = _bars(downtrend)
    trip = _trip(ohlc, 230, 239, downtrend[230], downtrend[239])
    assert (
        review_trade(trip, ohlc, TurtleConfig.system_2()).entry_type
        is EntryType.DOWNTREND
    )


def test_rule_replay_re_enters_on_a_later_breakout():
    # Stopped out by a dip, then a new 55-day high before the actual exit brings the rules back in.
    path = [100.0] * 220 + [94.0] * 5 + [100.0 + i for i in range(1, 30)]
    ohlc = _bars(path)
    trip = _trip(ohlc, 219, len(path) - 1, 100.0, path[-1])
    review = review_trade(trip, ohlc, TurtleConfig.system_2())
    assert review.turtle_exit_reason == "stop"
    assert review.turtle_reentries == 1
    assert review.turtle_still_open


def test_summary_counts_losers_past_stop_and_disposition_holds():
    ohlc = _bars(
        [100.0] * 220
        + [100.0 - i for i in range(1, 21)]
        + [80.0 + 2 * i for i in range(1, 41)]
    )
    loser = review_trade(
        _trip(ohlc, 219, 239, 100.0, 80.0), ohlc, TurtleConfig.system_2()
    )
    winner = review_trade(
        _trip(ohlc, 240, 279, 80.0, 160.0), ohlc, TurtleConfig.system_2()
    )
    summary = summarize([loser, winner], [])
    assert summary["trades"] == 2 and summary["win_rate"] == 0.5
    assert summary["losers_past_2n_stop"] == 1
    assert summary["median_hold_days_losers"] == loser.holding_days
    assert summary["total_pnl"] == pytest.approx(-200 + 800)


def test_run_review_split_adjusts_fills_before_matching():
    ohlc = _bars([10.0] * 260)
    ohlc.iloc[200, ohlc.columns.get_loc("stock_splits")] = 10.0
    fills = [
        Fill("Acct", ohlc.index[100].date(), "XYZ", Side.BUY, 1, 100.0),
        Fill("Acct", ohlc.index[250].date(), "XYZ", Side.SELL, 10, 11.0),
    ]
    report = run_review(fills, lambda symbol: ohlc, TurtleConfig.system_2())
    assert len(report.reviews) == 1 and not report.open_positions
    assert report.reviews[0].entry_price == pytest.approx(10.0)
    assert report.reviews[0].pnl == pytest.approx(10.0)
    assert report.summary["unmatched_sells"] == []
    assert report.summary["first_trade"] == date.isoformat(ohlc.index[100].date())


def test_mostly_unmatched_round_trip_is_not_reviewed():
    ohlc = _bars([10.0] * 260)
    fills = [
        Fill("Acct", ohlc.index[100].date(), "XYZ", Side.BUY, 0.1, 10.0),
        Fill("Acct", ohlc.index[250].date(), "XYZ", Side.SELL, 50, 11.0),
    ]
    report = run_review(fills, lambda symbol: ohlc, TurtleConfig.system_2())
    assert report.reviews == []
    assert report.summary["unmatched_sells"][0]["quantity"] == pytest.approx(49.9)
