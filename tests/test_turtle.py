import numpy as np
import pandas as pd
import pytest

from src.analysis.turtle import (
    ExitReason,
    NotionalAccount,
    PositionAction,
    TurtleConfig,
    backtest,
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
