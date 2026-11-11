from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from src.analysis.turtle import (
    Direction,
    ExitReason,
    NotionalAccount,
    PositionAction,
    TurtleConfig,
    backtest,
    direction_of,
    evaluate_position,
    first_exit,
    performance_stats,
    scan_breakouts,
    unit_limit_breaches,
    unit_shares,
)


def _bars(
    closes: list[float], half_range: float = 1.0, start: str = "2023-01-02"
) -> pd.DataFrame:
    """Bars whose open equals the close and whose range is close ± half_range, so N settles at 2 * half_range."""
    closes_array = np.asarray(closes, dtype=float)
    index = pd.bdate_range(start, periods=len(closes_array))
    return pd.DataFrame(
        {
            "open": closes_array,
            "high": closes_array + half_range,
            "low": closes_array - half_range,
            "close": closes_array,
        },
        index=index,
    )


def _flat_then(
    path: list[float], flat_days: int = 60, level: float = 100.0
) -> pd.DataFrame:
    return _bars([level] * flat_days + path)


def test_unit_size_is_risk_over_n_capped_by_notional():
    config = TurtleConfig(risk_per_unit=0.01, max_unit_fraction=0.25)
    assert unit_shares(100_000, n=2.0, price=50.0, config=config) == 500
    capped = TurtleConfig(risk_per_unit=0.01, max_unit_fraction=0.10)
    assert unit_shares(100_000, n=2.0, price=50.0, config=capped) == 200


def test_first_exit_hits_2n_stop_and_fills_at_gap_open():
    ohlc = _flat_then([100.0, 92.0])
    entry_date = ohlc.index[59]
    event = first_exit(
        ohlc,
        entry_date,
        entry_price=100.0,
        n_at_entry=2.0,
        config=TurtleConfig.system_2(),
    )
    assert event.reason is ExitReason.STOP
    assert event.exit_date == ohlc.index[61]
    # Stop is 96; the bar opened at 92, below it.
    assert event.price == pytest.approx(92.0)


def test_first_exit_uses_channel_low_when_it_comes_before_the_stop():
    # A tight 1.5N slide stays above a 2N stop of 96, but breaks the prior 20-day low of 99.
    ohlc = _flat_then([98.5])
    event = first_exit(
        ohlc,
        ohlc.index[59],
        entry_price=100.0,
        n_at_entry=2.0,
        config=TurtleConfig.system_2(),
    )
    assert event.reason is ExitReason.CHANNEL
    assert event.price == pytest.approx(98.5)


def test_evaluate_position_flags_stop_breach_as_late_exit():
    ohlc = _flat_then([100.0, 95.0, 94.0, 95.0])
    status = evaluate_position(
        "XYZ", ohlc, 100, 100.0, ohlc.index[59], 100_000, TurtleConfig.system_2()
    )
    assert status.action is PositionAction.EXIT_STOP
    assert status.is_late
    assert status.stop_price == pytest.approx(96.0)


def test_evaluate_position_suggests_adding_a_unit_after_half_n_gain():
    ohlc = _flat_then([101.0, 102.0])
    status = evaluate_position(
        "XYZ", ohlc, 10, 100.0, ohlc.index[59], 100_000, TurtleConfig.system_2()
    )
    assert status.action is PositionAction.ADD_UNIT
    assert status.next_add_level == pytest.approx(101.0)


def test_notional_account_cuts_20_percent_per_10_percent_drawdown_and_restores():
    account = NotionalAccount(1_000_000, TurtleConfig())
    assert account.update(950_000) == pytest.approx(1_000_000)
    assert account.update(900_000) == pytest.approx(800_000)
    # Book example: another 10% of the $800k notional puts the next cut at $820k.
    assert account.update(830_000) == pytest.approx(800_000)
    assert account.update(820_000) == pytest.approx(640_000)
    assert account.update(1_000_000) == pytest.approx(1_000_000)


def test_backtest_pyramids_to_four_units_then_exits_on_channel():
    rally = [100.0 + i for i in range(1, 31)]
    slide = [130.0 - 0.5 * i for i in range(1, 30)]
    ohlc = _flat_then(rally + slide)
    config = TurtleConfig.system_2(max_unit_fraction=1.0, max_gross_exposure=10.0)
    result = backtest({"XYZ": ohlc}, config, 1_000_000)
    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.units == 4
    assert trade.reason is ExitReason.CHANNEL
    assert trade.pnl > 0


def test_backtest_stops_raise_by_half_n_with_each_add():
    # Rally adds 4 units at ~101, 102, 103, 104 (N = 2); stops all sit near 104 - 4 = 100.
    rally = [100.0 + i for i in range(1, 5)]
    crash = [95.0]
    ohlc = _flat_then(rally + crash)
    config = TurtleConfig.system_2(max_unit_fraction=1.0, max_gross_exposure=10.0)
    trade = backtest({"XYZ": ohlc}, config, 1_000_000).trades[0]
    assert trade.units == 4
    assert trade.reason is ExitReason.STOP


def test_trend_filter_blocks_entries_against_the_long_trend():
    downtrend = [200.0 - 0.25 * i for i in range(400)]
    breakout = [downtrend[-1] + 2 * i for i in range(1, 15)]
    ohlc = _bars(downtrend + breakout)
    without_filter = backtest({"XYZ": ohlc}, TurtleConfig.system_2(), 100_000)
    with_filter = backtest(
        {"XYZ": ohlc}, TurtleConfig.system_2(use_trend_filter=True), 100_000
    )
    assert without_filter.trades
    assert not with_filter.trades


def test_system_1_skips_the_breakout_after_a_winner():
    rally = [100.0 + i for i in range(1, 21)]
    drift_down = [120.0 - 0.6 * i for i in range(1, 16)]
    base = drift_down[-1]
    ohlc = _flat_then(rally + drift_down + [base] * 25)
    s1 = scan_breakouts({"XYZ": ohlc}, 100_000, TurtleConfig.system_1())[0]
    s2 = scan_breakouts({"XYZ": ohlc}, 100_000, TurtleConfig.system_2())[0]
    assert s1.skipped_after_winner
    assert not s2.skipped_after_winner


def test_scan_reports_a_breakout_on_the_last_bar():
    ohlc = _flat_then([103.0], flat_days=80)
    signal = scan_breakouts({"XYZ": ohlc}, 100_000, TurtleConfig.system_2())[0]
    assert signal.triggered
    assert signal.entry_level == pytest.approx(101.0)


def test_unit_limits_flag_correlated_groups_and_direction():
    correlations = pd.DataFrame(
        [[1.0, 0.9, 0.1], [0.9, 1.0, 0.1], [0.1, 0.1, 1.0]],
        index=list("ABC"),
        columns=list("ABC"),
    )
    config = TurtleConfig()
    assert unit_limit_breaches({"A": 3, "B": 3, "C": 4}, correlations, config) == []
    breaches = unit_limit_breaches({"A": 4, "B": 3, "C": 4}, correlations, config)
    assert any("ρ ≥ 0.7" in b for b in breaches)
    assert unit_limit_breaches({"A": 5}, correlations, config)[0].startswith(
        "A: 5.0 units"
    )
    assert any(
        "Total long units" in b
        for b in unit_limit_breaches({"A": 4, "C": 4, "D": 5}, correlations, config)
    )


def test_performance_stats_on_steady_growth():
    index = pd.bdate_range("2020-01-01", periods=505)
    equity = pd.Series(100_000 * 1.001 ** np.arange(505), index=index)
    stats = performance_stats(equity, [])
    assert stats["max_drawdown"] == 0
    assert stats["r_squared"] == pytest.approx(1.0)
    assert stats["cagr"] > 0.2


# --- Short side: the same rules mirrored, on falling bars. ---


def test_direction_of_reads_the_share_sign():
    assert direction_of(-5) is Direction.SHORT
    assert direction_of(5) is Direction.LONG
    assert direction_of(0) is Direction.LONG
    assert Direction.SHORT.value == -1


def test_first_exit_short_hits_2n_stop_above_entry_and_fills_at_gap_open():
    ohlc = _flat_then([100.0, 108.0])
    event = first_exit(
        ohlc,
        ohlc.index[59],
        entry_price=100.0,
        n_at_entry=2.0,
        config=TurtleConfig.system_2(),
        direction=Direction.SHORT,
    )
    assert event.reason is ExitReason.STOP
    assert event.exit_date == ohlc.index[61]
    # Stop is 104; the bar opened at 108, above it.
    assert event.price == pytest.approx(108.0)


def test_first_exit_short_uses_channel_high_when_it_comes_before_the_stop():
    # A 1.5N bounce stays below a 2N stop of 104, but breaks the prior 20-day high of 101.
    ohlc = _flat_then([101.5])
    event = first_exit(
        ohlc,
        ohlc.index[59],
        entry_price=100.0,
        n_at_entry=2.0,
        config=TurtleConfig.system_2(),
        direction=Direction.SHORT,
    )
    assert event.reason is ExitReason.CHANNEL
    assert event.price == pytest.approx(101.5)


def test_long_first_exit_ignores_a_bounce_that_stops_a_short():
    ohlc = _flat_then([100.0, 108.0])
    assert first_exit(ohlc, ohlc.index[59], 100.0, 2.0, TurtleConfig.system_2()) is None


def test_evaluate_position_with_negative_shares_exits_when_price_rises_through_stop():
    ohlc = _flat_then([100.0, 105.0, 106.0, 105.0])
    status = evaluate_position(
        "XYZ", ohlc, -100, 100.0, ohlc.index[59], 100_000, TurtleConfig.system_2()
    )
    assert status.direction is Direction.SHORT
    assert status.action is PositionAction.EXIT_STOP
    assert status.is_late
    assert status.stop_price == pytest.approx(104.0)
    # Price is through the stop, so the trade is past -1R.
    assert status.open_r_multiple < -1
    assert status.distance_to_stop_n < 0


def test_evaluate_position_short_adds_a_unit_after_half_n_drop():
    ohlc = _flat_then([99.0, 98.0])
    status = evaluate_position(
        "XYZ", ohlc, -10, 100.0, ohlc.index[59], 100_000, TurtleConfig.system_2()
    )
    assert status.direction is Direction.SHORT
    assert status.action is PositionAction.ADD_UNIT
    assert status.next_add_level == pytest.approx(99.0)
    assert status.exit_level == pytest.approx(101.0)
    assert status.units_held < 0
    # In profit and 6 points (~3N) below the stop at 104: all positive for a short.
    assert status.open_r_multiple > 0
    assert status.distance_to_stop_n > 0
    assert status.open_risk_dollars == pytest.approx(10 * (104.0 - 98.0))


def test_evaluate_position_long_reports_long_direction():
    ohlc = _flat_then([101.0, 102.0])
    status = evaluate_position(
        "XYZ", ohlc, 10, 100.0, ohlc.index[59], 100_000, TurtleConfig.system_2()
    )
    assert status.direction is Direction.LONG


def test_scan_reports_a_short_breakout_with_stop_above_entry():
    ohlc = _flat_then([97.0], flat_days=80)
    config = TurtleConfig.system_2()
    assert [s.direction for s in scan_breakouts({"XYZ": ohlc}, 100_000, config)] == [
        Direction.LONG
    ]
    signal = scan_breakouts(
        {"XYZ": ohlc}, 100_000, config, directions=(Direction.SHORT,)
    )[0]
    assert signal.direction is Direction.SHORT
    assert signal.triggered
    assert signal.entry_level == pytest.approx(99.0)
    assert signal.stop_if_filled == pytest.approx(99.0 + 2 * signal.n)
    # Already below the broken low, so the downward distance is negative.
    assert signal.distance_n == pytest.approx((97.0 - 99.0) / signal.n)


def test_scan_both_directions_sorts_triggered_first_then_by_distance():
    falling = _flat_then([97.0], flat_days=80)
    flat = _flat_then([100.0], flat_days=80)
    signals = scan_breakouts(
        {"DOWN": falling, "FLAT": flat},
        100_000,
        TurtleConfig.system_2(),
        directions=(Direction.LONG, Direction.SHORT),
    )
    assert len(signals) == 4
    assert (signals[0].ticker, signals[0].direction) == ("DOWN", Direction.SHORT)
    assert all(not s.triggered for s in signals[1:])
    distances = [s.distance_n for s in signals[1:]]
    assert distances == sorted(distances)


def test_system_1_skips_the_short_breakout_after_a_winning_short():
    drop = [100.0 - i for i in range(1, 21)]
    drift_up = [80.0 + 0.6 * i for i in range(1, 16)]
    base = drift_up[-1]
    ohlc = _flat_then(drop + drift_up + [base] * 25)
    short = (Direction.SHORT,)
    s1 = scan_breakouts({"XYZ": ohlc}, 100_000, TurtleConfig.system_1(), short)[0]
    s2 = scan_breakouts({"XYZ": ohlc}, 100_000, TurtleConfig.system_2(), short)[0]
    assert s1.skipped_after_winner
    assert not s2.skipped_after_winner


def test_backtest_short_pyramids_to_four_units_then_exits_on_channel_high():
    drop = [100.0 - i for i in range(1, 31)]
    rebound = [70.0 + 0.5 * i for i in range(1, 30)]
    ohlc = _flat_then(drop + rebound)
    config = TurtleConfig.system_2(
        max_unit_fraction=1.0, max_gross_exposure=10.0, allow_short=True
    )
    result = backtest({"XYZ": ohlc}, config, 1_000_000)
    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.direction is Direction.SHORT
    assert trade.units == 4
    assert trade.reason is ExitReason.CHANNEL
    assert trade.pnl > 0
    assert result.equity.iloc[-1] > 1_000_000
    # Long-only, the same falling series never breaks out.
    assert not backtest(
        {"XYZ": ohlc}, replace(config, allow_short=False), 1_000_000
    ).trades


def test_backtest_short_stops_fall_by_half_n_with_each_add():
    # Slide adds 4 short units near 99, 98, 97, 96 (N = 2); stops all sit near 96 + 4 = 100.
    slide = [100.0 - i for i in range(1, 5)]
    spike = [105.0]
    ohlc = _flat_then(slide + spike)
    config = TurtleConfig.system_2(
        max_unit_fraction=1.0, max_gross_exposure=10.0, allow_short=True
    )
    trade = backtest({"XYZ": ohlc}, config, 1_000_000).trades[0]
    assert trade.direction is Direction.SHORT
    assert trade.units == 4
    assert trade.reason is ExitReason.STOP
    assert trade.pnl < 0


def test_trend_filter_blocks_short_entries_against_the_uptrend():
    uptrend = [100.0 + 0.25 * i for i in range(400)]
    breakdown = [uptrend[-1] - 2 * i for i in range(1, 15)]
    ohlc = _bars(uptrend + breakdown)
    config = TurtleConfig.system_2(allow_short=True)
    without_filter = backtest({"XYZ": ohlc}, config, 100_000)
    with_filter = backtest(
        {"XYZ": ohlc}, replace(config, use_trend_filter=True), 100_000
    )

    def shorts(trades: list) -> list:
        return [t for t in trades if t.direction is Direction.SHORT]

    assert shorts(without_filter.trades)
    assert not shorts(with_filter.trades)
    signal = scan_breakouts({"XYZ": ohlc}, 100_000, config, (Direction.SHORT,))[0]
    assert not signal.trend_ok


def test_unit_limits_count_longs_and_shorts_separately():
    correlations = pd.DataFrame(
        [[1.0, 0.9, 0.1], [0.9, 1.0, 0.1], [0.1, 0.1, 1.0]],
        index=list("ABC"),
        columns=list("ABC"),
    )
    config = TurtleConfig()
    # 4 long A and 3 short B are 7 correlated units, but on opposite sides.
    assert unit_limit_breaches({"A": 4, "B": -3}, correlations, config) == []
    breaches = unit_limit_breaches({"A": -4, "B": -3}, correlations, config)
    assert any(b.startswith("A short group (ρ ≥ 0.7)") for b in breaches)
    assert unit_limit_breaches({"A": -5}, correlations, config)[0].startswith(
        "A: 5.0 short units"
    )
    twelve_each_way = {"L1": 4, "L2": 4, "L3": 4, "S1": -4, "S2": -4, "S3": -4}
    assert unit_limit_breaches(twelve_each_way, correlations, config) == []
    assert any(
        "Total short units 13.0" in b
        for b in unit_limit_breaches(
            {**twelve_each_way, "S4": -1}, correlations, config
        )
    )


def _waves(phase: float, drift: float, days: int = 600) -> pd.DataFrame:
    """Deterministic trending waves with real gaps, so the backtest trades, adds and exits."""
    i = np.arange(days, dtype=float)
    close = (
        100 + 25 * np.sin(i / 45 + phase) + drift * i + 4 * np.sin(i / 9 + 2 * phase)
    )
    open_ = close - 0.6 * np.sin(i / 3 + phase)
    return pd.DataFrame(
        {
            "open": open_,
            "high": np.maximum(open_, close) + 1.0,
            "low": np.minimum(open_, close) - 1.0,
            "close": close,
        },
        index=pd.bdate_range("2021-01-04", periods=days),
    )


# Snapshot of the long-only backtest taken before the short side was added.
_LONG_ONLY_BASELINE = {
    "S1": (
        267038.47848262853,
        [
            ("AAA", "2021-02-01", "2021-04-21", 3, 9087.2507, ExitReason.CHANNEL),
            ("BBB", "2021-07-29", "2021-08-25", 3, -624.2271, ExitReason.CHANNEL),
            ("BBB", "2021-10-01", "2022-02-02", 3, 38684.4395, ExitReason.CHANNEL),
            ("AAA", "2022-02-02", "2022-05-20", 3, 27731.8823, ExitReason.CHANNEL),
            ("BBB", "2022-08-30", "2022-09-26", 3, -769.376, ExitReason.CHANNEL),
            ("BBB", "2022-11-02", "2023-03-06", 3, 61045.4242, ExitReason.CHANNEL),
            ("AAA", "2023-01-02", "2023-04-21", 3, 31883.0848, None),
        ],
    ),
    "S2": (
        233367.25982893893,
        [
            ("AAA", "2021-03-22", "2021-04-28", 3, 471.2444, ExitReason.CHANNEL),
            ("BBB", "2021-10-01", "2022-02-11", 3, 36943.2731, ExitReason.CHANNEL),
            ("AAA", "2022-02-11", "2022-05-30", 3, 17082.8777, ExitReason.CHANNEL),
            ("BBB", "2022-11-02", "2023-03-14", 3, 54909.4329, ExitReason.CHANNEL),
            ("AAA", "2023-01-02", "2023-04-21", 3, 23960.4316, None),
        ],
    ),
}


@pytest.mark.parametrize("system", ["S1", "S2"])
def test_backtest_without_shorts_matches_the_long_only_snapshot(system: str):
    histories = {"AAA": _waves(0.0, 0.05), "BBB": _waves(1.3, -0.03)}
    factory = TurtleConfig.system_1 if system == "S1" else TurtleConfig.system_2
    config = factory()
    assert config.allow_short is False
    result = backtest(histories, config, 100_000)
    final_equity, trades = _LONG_ONLY_BASELINE[system]
    assert result.stats["final_equity"] == pytest.approx(final_equity, rel=1e-12)
    assert [
        (
            t.ticker,
            str(t.entry_date.date()),
            str(t.exit_date.date()),
            t.units,
            round(t.pnl, 4),
            t.reason,
        )
        for t in result.trades
    ] == trades
    assert all(t.direction is Direction.LONG for t in result.trades)


def test_backtest_with_shorts_trades_both_sides_of_the_waves():
    histories = {"AAA": _waves(0.0, 0.05), "BBB": _waves(1.3, -0.03)}
    result = backtest(histories, TurtleConfig.system_2(allow_short=True), 100_000)
    shorts = [t for t in result.trades if t.direction is Direction.SHORT]
    assert shorts
    assert sum(t.pnl for t in shorts) > 0
