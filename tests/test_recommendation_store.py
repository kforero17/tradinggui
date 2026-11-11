import enum
from datetime import datetime

import pytest

from src.config.settings import settings
from src.data.database import Database


class _Direction(enum.Enum):
    LONG = 1
    SHORT = -1


@pytest.fixture
def database(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", tmp_path / "recs.db")
    return Database()


def _row(ticker: str, **overrides) -> dict:
    row = {
        "signal_key": f"{ticker}:LONG:ENTER",
        "ticker": ticker,
        "account": "taxable",
        "direction": "LONG",
        "action": "ENTER",
        "status": "NEW",
        "trigger_price": 101.5,
        "stop_price": 95.0,
        "shares": 40,
        "risk_dollars": 260.0,
        "risk_pct_equity": 0.0026,
        "distance_n": 0.0,
        "trend_score": 82.0,
        "rs_rank": 0.91,
        "asset_type": "stock",
        "is_late": False,
        "reasons": ["55-day breakout", "EMA25 > EMA350"],
        "blocked_by": [],
        "extra": {"order_type": "stop", "limit_offset": 0.1},
    }
    row.update(overrides)
    return row


def test_create_and_read_back_a_run(database):
    first_seen = datetime(2026, 11, 3, 6, 0)
    run_id = database.create_recommendation_run(
        "weekly",
        equity=100_000.0,
        config={"entry_period": 55, "exit_period": 20},
        regime={"spy_above_200d": True, "breadth": 0.62},
        rows=[
            _row("AAPL"),
            _row(
                "TSLA",
                signal_key="TSLA:SHORT:ENTER",
                direction=_Direction.SHORT,
                status="STILL_VALID",
                first_seen_at=first_seen,
                blocked_by=("no short-enabled account",),
                is_late=True,
            ),
        ],
    )

    run = database.get_latest_run()
    assert run["id"] == run_id
    assert run["job"] == "weekly"
    assert run["equity"] == 100_000.0
    assert run["config"] == {"entry_period": 55, "exit_period": 20}
    assert run["regime"] == {"spy_above_200d": True, "breadth": 0.62}

    aapl, tsla = database.get_run_recommendations(run_id)
    assert aapl["reasons"] == ["55-day breakout", "EMA25 > EMA350"]
    assert aapl["blocked_by"] == []
    assert aapl["extra"] == {"order_type": "stop", "limit_offset": 0.1}
    assert aapl["first_seen_at"] == run["run_at"]
    assert aapl["shares"] == 40
    assert aapl["is_late"] is False
    assert tsla["direction"] == "SHORT"
    assert tsla["status"] == "STILL_VALID"
    assert tsla["first_seen_at"] == first_seen
    assert tsla["blocked_by"] == ["no short-enabled account"]
    assert tsla["is_late"] is True


def test_latest_and_previous_run_by_job(database):
    weekly_1 = database.create_recommendation_run("weekly", 1.0, {}, {}, [])
    daily_1 = database.create_recommendation_run("daily", 1.0, {}, {}, [_row("AAPL")])
    weekly_2 = database.create_recommendation_run("weekly", 1.0, {}, {}, [])

    assert database.get_latest_run()["id"] == weekly_2
    assert database.get_latest_run("daily")["id"] == daily_1
    assert database.get_latest_run("manual") is None
    assert database.get_previous_run(weekly_2)["id"] == daily_1
    assert database.get_previous_run(weekly_2, job="weekly")["id"] == weekly_1
    assert database.get_previous_run(weekly_1) is None
    assert database.get_run_recommendations(weekly_2) == []


@pytest.mark.parametrize(
    "job,row",
    [
        ("hourly", _row("AAPL")),
        ("weekly", _row("AAPL", direction="SIDEWAYS")),
        ("weekly", _row("AAPL", status="EXPIRED")),
        ("weekly", _row("AAPL", unknown_field=1)),
    ],
)
def test_invalid_run_stores_nothing(database, job, row):
    with pytest.raises(ValueError):
        database.create_recommendation_run(job, 1.0, {}, {}, [row])
    assert database.get_latest_run() is None


def test_signal_decision_upsert(database):
    first_seen = datetime(2026, 11, 3, 6, 0)
    database.upsert_signal_decision(
        "AAPL:LONG:ENTER", first_seen, "pending", "watch earnings"
    )
    database.upsert_signal_decision("MSFT:LONG:ENTER", first_seen, "skipped")

    [pending] = database.get_signal_decisions(["AAPL:LONG:ENTER"])
    assert pending["status"] == "pending"
    assert pending["note"] == "watch earnings"
    assert pending["decided_at"] is None

    database.upsert_signal_decision("AAPL:LONG:ENTER", first_seen, "taken")

    [taken] = database.get_signal_decisions(["AAPL:LONG:ENTER"])
    assert taken["id"] == pending["id"]
    assert taken["status"] == "taken"
    assert taken["note"] == "watch earnings"
    assert taken["decided_at"] is not None
    assert taken["first_seen_at"] == first_seen

    database.upsert_signal_decision("AAPL:LONG:ENTER", datetime(2026, 12, 1), "pending")
    assert len(database.get_signal_decisions(["AAPL:LONG:ENTER"])) == 2
    assert len(database.get_signal_decisions()) == 3
    assert database.get_signal_decisions([]) == []


def test_invalid_decision_status_raises(database):
    with pytest.raises(ValueError):
        database.upsert_signal_decision(
            "AAPL:LONG:ENTER", datetime(2026, 11, 3), "maybe"
        )
