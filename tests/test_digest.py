import subprocess
from datetime import datetime

import pytest
import requests

from src.config.settings import settings
from src.notify import digest as digest_mod
from src.notify.digest import Digest, build_digest, write_report


def _row(ticker, action, direction="LONG", status="NEW", **overrides):
    row = {
        "id": 1,
        "run_id": 3,
        "signal_key": f"{ticker}|{direction}|ENTRY",
        "ticker": ticker,
        "account": "Roth IRA",
        "direction": direction,
        "action": action,
        "status": status,
        "first_seen_at": "2026-11-07T11:00:00",
        "trigger_price": 101.0,
        "stop_price": 97.0,
        "shares": 120,
        "risk_dollars": 480.0,
        "risk_pct_equity": 0.0048,
        "distance_n": 0.4,
        "trend_score": 72.5,
        "rs_rank": 0.8,
        "asset_type": "stock",
        "is_late": False,
        "reasons": ["55-day high breakout"],
        "blocked_by": [],
        "extra": {
            "order_type": "buy stop",
            "entry_level": 101.0,
            "n": 2.0,
            "value": 12_120.0,
            "check_borrow": False,
            "last_close": 102.5,
        },
        "decision": None,
    }
    row.update(overrides)
    return row


DASHBOARD = {
    "run": {
        "id": 3,
        "run_at": "2026-11-07T11:00:00",
        "job": "weekly",
        "equity": 100_000.0,
        "config": {},
        "regime": {
            "benchmark": "SPY",
            "benchmark_close": 580.2,
            "benchmark_sma200": 540.0,
            "benchmark_above_200d": True,
            "breadth_above_200d": 0.62,
            "label": "risk_on",
        },
    },
    "entries_run": {"id": 3, "job": "weekly", "run_at": "2026-11-07T11:00:00"},
    "recommendations": [
        _row(
            "XOM",
            "EXIT_STOP",
            status="STILL_VALID",
            account="taxable",
            reasons=["2N stop 95.00 hit"],
            extra={"order_type": "sell", "last_close": 94.0},
        ),
        _row(
            "AAPL",
            "ADD_UNIT",
            extra={"order_type": "buy stop", "units_held": 2.0},
        ),
        _row(
            "A&B<script>alert(1)</script>",
            "ENTER",
            decision={
                "status": "pending",
                "note": "<img src=x onerror=alert(1)> wait for earnings",
                "decided_at": "2026-11-08T10:00:00",
            },
        ),
        _row(
            "DWN",
            "ENTER",
            direction="SHORT",
            status="STILL_VALID",
            account="taxable",
            blocked_by=["heat budget"],
            extra={
                "order_type": "sell short stop",
                "entry_level": 50.0,
                "check_borrow": True,
                "last_close": 51.0,
            },
        ),
        _row("WCH", "WATCH", trend_score=40.0),
        _row("HLD", "HOLD", status="STILL_VALID"),
    ],
    "expired": [_row("OLD", "WATCH", status="STILL_VALID")],
    "summary": {"total": 6, "new": 2, "watch_hidden": 4},
    "heat": {
        "equity": 100_000.0,
        "open_risk_dollars": 2_000.0,
        "open_risk_pct": 0.02,
        "planned_risk_dollars": 960.0,
        "heat_after_planned_pct": 0.0296,
        "budget_pct": 0.10,
        "budget_dollars": 10_000.0,
        "remaining_dollars": 7_040.0,
    },
}
REPORT = {
    "job": "weekly",
    "run_id": 3,
    "stage_seconds": {"universe": 2.0, "metrics": 61.5, "recommendations": 12.25},
    "metrics": {"tickers": 150, "computed": 148, "total_seconds": 61.5},
    "universe": {
        "listed_rows": 6000,
        "listed_etfs": 2000,
        "sp500_rows": 503,
        "listed_error": None,
        "sp500_error": None,
    },
    "alerts_triggered": 3,
}


def test_build_digest_renders_every_section():
    digest = build_digest(DASHBOARD, REPORT)
    assert digest.title == "TradingGui weekly digest"
    assert digest.summary_line == (
        "1 exit, 1 add, 2 entries (1 new), 1 watch, 1 expired"
    )
    html = digest.html
    for heading in (
        "Exits (1)",
        "Adds (1)",
        "New entries (2)",
        "Top watch (1 of 5)",
        "Expired (1)",
        "Heat vs budget",
        "Market regime",
        "Run stats",
        "Metrics refresh",
        "Universe files",
    ):
        assert heading in html, heading
    # Order ticket details.
    assert "buy stop" in html and "sell short stop" in html
    assert "101.00" in html and "$480" in html and "120" in html
    assert "CHECK BORROW" in html and "NEW" in html and "heat budget" in html
    assert "risk on" in html and "62.0%" in html
    assert "61.5s" in html
    # HOLD rows are not digest material.
    assert "HLD" not in html
    assert html.count('class="exit"') == 1


def test_build_digest_escapes_tickers_and_notes():
    html = build_digest(DASHBOARD).html
    assert "<script>alert(1)</script>" not in html
    assert "A&amp;B&lt;script&gt;alert(1)&lt;/script&gt;" in html
    assert "<img src=x" not in html
    assert "&lt;img src=x onerror=alert(1)&gt; wait for earnings" in html


def test_build_digest_without_report_or_run():
    empty = {
        "run": None,
        "entries_run": None,
        "recommendations": [],
        "expired": [],
        "summary": {"total": 0, "watch_hidden": 0},
        "heat": None,
    }
    digest = build_digest(empty)
    assert digest.summary_line == "0 exits, 0 adds, 0 entries (0 new), 0 watch"
    assert "Run stats" not in digest.html
    assert "No exits." in digest.html and "No run stored yet." in digest.html


def test_digest_leads_with_stale_holdings_and_lists_skipped_positions():
    dashboard = {
        **DASHBOARD,
        "stale_holdings": [{"ticker": "AAPL", "last_bar": "2026-11-05"}],
        "skipped_positions": [
            {"account": "Roth IRA", "ticker": "NVDA", "reason": "missing cost basis"}
        ],
    }
    digest = build_digest(dashboard, REPORT)
    assert digest.summary_line == (
        "price data stale for 1 holding – stop checks may be late; "
        "1 exit, 1 add, 2 entries (1 new), 1 watch, 1 expired"
    )
    assert "Stale prices" in digest.html and "last bar 2026-11-05" in digest.html
    assert "Not evaluated (1)" in digest.html
    assert "NVDA</span> (Roth IRA) – missing cost basis" in digest.html
    assert "Portfolio page" in digest.html
    assert "Stale bars: AAPL (last 2026-11-05)" in digest.text
    assert "Not evaluated: NVDA (Roth IRA) – missing cost basis" in digest.text

    plain = build_digest(DASHBOARD, REPORT)
    assert "Stale prices" not in plain.html and "Not evaluated" not in plain.html


def test_text_digest_lists_order_tickets():
    text = build_digest(DASHBOARD, REPORT).text
    assert text.splitlines()[0] == "*TradingGui weekly digest*"
    assert "Exits:" in text and "New entries:" in text
    assert (
        "DWN SHORT · sell short stop · 120 @ 50.00 · stop 97.00 · risk $480 · taxable"
        " · [check borrow, blocked: heat budget]"
    ) in text
    assert "WCH" not in text  # watch rows stay in the HTML report


def test_write_report_creates_the_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "REPORTS_DIR", tmp_path / "nested" / "reports")
    digest = Digest("t", "s", "<html>ok</html>", "text")
    path = write_report(digest, "daily", datetime(2026, 11, 10, 16, 30))
    assert path == tmp_path / "nested" / "reports" / "digest_daily_2026-11-10.html"
    assert path.read_text() == "<html>ok</html>"


def test_notify_macos_escapes_the_applescript(monkeypatch):
    calls = []

    def run(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(digest_mod.subprocess, "run", run)
    assert digest_mod.notify_macos('He said "hi"', 'back\\slash\nnext "line"')
    args, kwargs = calls[0]
    assert args[:2] == ["osascript", "-e"]
    assert args[2] == (
        'display notification "back\\\\slash next \\"line\\"" '
        'with title "He said \\"hi\\""'
    )
    assert kwargs["timeout"] and kwargs["check"] is False


def test_notify_macos_failures_are_not_raised(monkeypatch):
    monkeypatch.setattr(
        digest_mod.subprocess,
        "run",
        lambda args, **kw: subprocess.CompletedProcess(args, 1, "", "-600 denied"),
    )
    assert digest_mod.notify_macos("t", "m") is False

    def missing(args, **kw):
        raise FileNotFoundError("osascript")

    monkeypatch.setattr(digest_mod.subprocess, "run", missing)
    assert digest_mod.notify_macos("t", "m") is False

    def slow(args, **kw):
        raise subprocess.TimeoutExpired(args, 10)

    monkeypatch.setattr(digest_mod.subprocess, "run", slow)
    assert digest_mod.notify_macos("t", "m") is False


class _Response:
    def __init__(self, status=200):
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(
                "404 Client Error for url: https://hooks.slack.com/secret",
                response=self,
            )


def test_notify_slack_only_when_the_url_is_set(monkeypatch):
    posts = []
    monkeypatch.setattr(
        digest_mod.requests,
        "post",
        lambda url, **kw: posts.append((url, kw)) or _Response(),
    )
    monkeypatch.setattr(settings, "SLACK_WEBHOOK_URL", None)
    assert digest_mod.notify_slack("hello") is False
    assert posts == []

    monkeypatch.setattr(settings, "SLACK_WEBHOOK_URL", "https://hooks.slack.com/x")
    assert digest_mod.notify_slack("hello") is True
    assert posts[0][0] == "https://hooks.slack.com/x"
    assert posts[0][1]["json"] == {"text": "hello"} and posts[0][1]["timeout"]


@pytest.mark.parametrize("disable, verify", [("true", False), ("false", True)])
def test_notify_slack_follows_disable_ssl_verify(monkeypatch, disable, verify):
    posts = []
    monkeypatch.setenv("DISABLE_SSL_VERIFY", disable)
    monkeypatch.setattr(settings, "SLACK_WEBHOOK_URL", "https://hooks.slack.com/x")
    monkeypatch.setattr(
        digest_mod.requests, "post", lambda url, **kw: posts.append(kw) or _Response()
    )
    assert digest_mod.notify_slack("hello") is True
    assert posts[0]["verify"] is verify


@pytest.mark.parametrize(
    "post",
    [
        lambda url, **kw: _Response(404),
        lambda url, **kw: (_ for _ in ()).throw(requests.ConnectionError("down")),
    ],
)
def test_notify_slack_failures_are_not_raised(monkeypatch, post):
    monkeypatch.setattr(settings, "SLACK_WEBHOOK_URL", "https://hooks.slack.com/x")
    monkeypatch.setattr(digest_mod.requests, "post", post)
    assert digest_mod.notify_slack("hello") is False


def test_send_digest_uses_both_channels(monkeypatch):
    sent = []
    monkeypatch.setattr(
        digest_mod, "notify_macos", lambda t, m: sent.append(("mac", t, m)) or False
    )
    monkeypatch.setattr(
        digest_mod, "notify_slack", lambda text: sent.append(("slack", text)) or True
    )
    digest = Digest("Title", "1 exit", "<html/>", "full text")
    assert digest_mod.send_digest(digest) is True
    assert sent == [("mac", "Title", "1 exit"), ("slack", "full text")]
