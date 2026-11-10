"""Render a recommendation run as an HTML/text digest and send it.

``build_digest`` turns ``recommendation_service.latest_dashboard()`` (plus,
optionally, a job report as a dict) into a self-contained HTML page and a
plain-text summary. Holdings on stale bars lead the summary line; holdings
that could not be evaluated are listed too. ``write_report`` saves the page under ``REPORTS_DIR``.
Notifications (macOS ``osascript``, Slack webhook) are best-effort: a failure
is logged and reported as False, never raised.
"""

from __future__ import annotations

import math
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from jinja2 import Environment, FileSystemLoader, Undefined, select_autoescape
from loguru import logger

from ..config.settings import settings

__all__ = [
    "Digest",
    "DIGEST_WATCH_ROWS",
    "build_digest",
    "stale_line",
    "write_report",
    "notify_macos",
    "notify_slack",
    "send_digest",
]

TEMPLATES_DIR = Path(__file__).parent / "templates"
# The digest lists the best-scoring watch rows only; the dashboard has the rest.
DIGEST_WATCH_ROWS = 15
# Plain-text (Slack) lines per section before "and N more".
TEXT_ROWS_PER_SECTION = 10
NOTIFY_TIMEOUT_S = 10

EXIT_ACTIONS = ("EXIT_STOP", "EXIT_CHANNEL")

_env = Environment(
    loader=FileSystemLoader(TEMPLATES_DIR),
    autoescape=select_autoescape(["html"]),
    trim_blocks=True,
    lstrip_blocks=True,
)


@dataclass(frozen=True)
class Digest:
    title: str
    summary_line: str
    html: str
    text: str


def _finite(value: Any) -> float | None:
    if isinstance(value, (bool, Undefined)) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _money(value: Any) -> str:
    number = _finite(value)
    return "–" if number is None else f"${number:,.0f}"


def _price(value: Any) -> str:
    number = _finite(value)
    return "–" if number is None else f"{number:,.2f}"


def _pct(value: Any, digits: int = 1) -> str:
    number = _finite(value)
    return "–" if number is None else f"{number * 100:.{digits}f}%"


def _num(value: Any, digits: int = 0) -> str:
    number = _finite(value)
    return "–" if number is None else f"{number:,.{digits}f}"


def _seconds(value: Any) -> str:
    number = _finite(value)
    return "–" if number is None else f"{number:,.1f}s"


_env.filters.update(money=_money, price=_price, pct=_pct, num=_num, seconds=_seconds)


def _plural(count: int, word: str, plural: str | None = None) -> str:
    return f"{count} {word if count == 1 else plural or word + 's'}"


def _sections(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    def by_score(row: Mapping[str, Any]) -> float:
        return -(_finite(row.get("trend_score")) or 0.0)

    watch = sorted((r for r in rows if r["action"] == "WATCH"), key=by_score)
    return {
        "exits": [r for r in rows if r["action"] in EXIT_ACTIONS],
        "adds": [r for r in rows if r["action"] == "ADD_UNIT"],
        "entries": [r for r in rows if r["action"] == "ENTER"],
        "watch": watch[:DIGEST_WATCH_ROWS],
        "watch_total": watch,
    }


def stale_line(stale_holdings: Sequence[Mapping[str, Any]]) -> str:
    """The warning that leads a digest whose holdings' bars are behind the last session."""
    return (
        f"price data stale for {_plural(len(stale_holdings), 'holding')} – "
        "stop checks may be late"
    )


def _summary_line(sections: Mapping[str, list], expired: int) -> str:
    entries = sections["entries"]
    new_entries = sum(1 for r in entries if r["status"] == "NEW")
    parts = [
        _plural(len(sections["exits"]), "exit"),
        _plural(len(sections["adds"]), "add"),
        f"{_plural(len(entries), 'entry', 'entries')} ({new_entries} new)",
        f"{len(sections['watch_total'])} watch",
    ]
    if expired:
        parts.append(f"{expired} expired")
    return ", ".join(parts)


def _ticket(row: Mapping[str, Any]) -> str:
    extra = row.get("extra") or {}
    entry = extra.get("entry_level", row.get("trigger_price"))
    parts = [
        f"{row['ticker']} {row['direction']}",
        str(extra.get("order_type") or row["action"].lower()),
        f"{_num(row.get('shares'))} @ {_price(entry)}",
        f"stop {_price(row.get('stop_price'))}",
        f"risk {_money(row.get('risk_dollars'))}",
    ]
    if row.get("account"):
        parts.append(str(row["account"]))
    flags = []
    if row.get("status") == "NEW":
        flags.append("NEW")
    if extra.get("check_borrow"):
        flags.append("check borrow")
    if row.get("blocked_by"):
        flags.append("blocked: " + "; ".join(map(str, row["blocked_by"])))
    if flags:
        parts.append(f"[{', '.join(flags)}]")
    return " · ".join(parts)


def _text(
    title: str,
    summary_line: str,
    sections: Mapping[str, list],
    regime: Mapping[str, Any],
    heat: Mapping[str, Any] | None,
    stale_holdings: Sequence[Mapping[str, Any]] = (),
    skipped_positions: Sequence[Mapping[str, Any]] = (),
) -> str:
    lines = [f"*{title}*", summary_line]
    if stale_holdings:
        lines.append(
            "Stale bars: "
            + ", ".join(f"{s['ticker']} (last {s['last_bar']})" for s in stale_holdings)
        )
    if skipped_positions:
        lines.append(
            "Not evaluated: "
            + ", ".join(
                f"{s['ticker']} ({s['account']}) – {s['reason']}"
                for s in skipped_positions
            )
        )
    if regime:
        lines.append(f"Regime: {regime.get('label', 'unknown')}")
    if heat:
        lines.append(
            f"Heat: {_pct(heat.get('heat_after_planned_pct'))} after planned "
            f"of {_pct(heat.get('budget_pct'))} budget"
        )
    for name, label in (
        ("exits", "Exits"),
        ("adds", "Adds"),
        ("entries", "New entries"),
    ):
        rows = sections[name]
        if not rows:
            continue
        lines.append(f"{label}:")
        lines.extend(f"• {_ticket(r)}" for r in rows[:TEXT_ROWS_PER_SECTION])
        if len(rows) > TEXT_ROWS_PER_SECTION:
            lines.append(f"…and {len(rows) - TEXT_ROWS_PER_SECTION} more")
    return "\n".join(lines)


def build_digest(
    dashboard: Mapping[str, Any], job_report_like: Mapping[str, Any] | None = None
) -> Digest:
    """A digest of ``latest_dashboard()``; ``job_report_like`` adds a run-stats section.

    ``job_report_like`` may carry ``job``, ``run_id``, ``stage_seconds``,
    ``metrics`` (``MetricsRefreshStats`` as a dict), ``universe`` and
    ``alerts_triggered``.
    """
    report = dict(job_report_like or {})
    run = dashboard.get("run") or {}
    job = report.get("job") or run.get("job") or "manual"
    rows = list(dashboard.get("recommendations") or [])
    expired = list(dashboard.get("expired") or [])
    sections = _sections(rows)
    stale_holdings = list(dashboard.get("stale_holdings") or [])
    skipped_positions = list(dashboard.get("skipped_positions") or [])
    summary_line = _summary_line(sections, len(expired))
    if stale_holdings:
        summary_line = f"{stale_line(stale_holdings)}; {summary_line}"
    title = f"TradingGui {job} digest"
    regime = dict(run.get("regime") or {})
    heat = dashboard.get("heat")

    html = _env.get_template("digest.html").render(
        title=title,
        summary_line=summary_line,
        run=run,
        entries_run=dashboard.get("entries_run") or {},
        summary=dashboard.get("summary") or {},
        exits=sections["exits"],
        adds=sections["adds"],
        entries=sections["entries"],
        watch=sections["watch"],
        watch_total=len(sections["watch_total"]),
        watch_hidden=(dashboard.get("summary") or {}).get("watch_hidden", 0),
        expired=expired,
        heat=heat,
        regime=regime,
        stale_holdings=stale_holdings,
        skipped_positions=skipped_positions,
        stale_line=stale_line(stale_holdings),
        report=report,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )
    text = _text(
        title, summary_line, sections, regime, heat, stale_holdings, skipped_positions
    )
    return Digest(title=title, summary_line=summary_line, html=html, text=text)


def write_report(digest: Digest, job: str, now: datetime | None = None) -> Path:
    """Write the HTML to ``REPORTS_DIR/digest_<job>_<YYYY-MM-DD>.html``; returns the path."""
    now = now or datetime.now()
    settings.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = settings.REPORTS_DIR / f"digest_{job}_{now:%Y-%m-%d}.html"
    path.write_text(digest.html, encoding="utf-8")
    logger.info(f"Wrote digest report {path}")
    return path


def _applescript_string(value: str) -> str:
    """A double-quoted AppleScript string literal; newlines become spaces."""
    flat = " ".join(value.splitlines())
    return '"' + flat.replace("\\", "\\\\").replace('"', '\\"') + '"'


def notify_macos(title: str, message: str) -> bool:
    """Show a macOS notification via ``osascript``; False (logged) on any failure."""
    script = (
        f"display notification {_applescript_string(message)} "
        f"with title {_applescript_string(title)}"
    )
    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=NOTIFY_TIMEOUT_S,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as e:
        logger.warning(f"macOS notification failed: {e}")
        return False
    if result.returncode != 0:
        logger.warning(
            f"macOS notification failed (exit {result.returncode}): "
            f"{(result.stderr or '').strip()}"
        )
        return False
    return True


def notify_slack(text: str) -> bool:
    """Post ``text`` to ``SLACK_WEBHOOK_URL`` if set; False when unset or on failure (logged)."""
    url = settings.SLACK_WEBHOOK_URL
    if not url:
        return False
    try:
        response = requests.post(url, json={"text": text}, timeout=NOTIFY_TIMEOUT_S)
        response.raise_for_status()
    except requests.RequestException as e:
        # The message of an HTTPError contains the URL, which is the webhook secret.
        status = getattr(e.response, "status_code", None)
        logger.warning(
            f"Slack notification failed: {type(e).__name__}"
            + (f" (HTTP {status})" if status else "")
        )
        return False
    return True


def send_digest(digest: Digest) -> bool:
    """Send to macOS and Slack; True if at least one channel delivered."""
    mac = notify_macos(digest.title, digest.summary_line)
    slack = notify_slack(digest.text)
    return mac or slack
