"""CLI for the scheduled jobs.

    python -m src.jobs weekly [--tickers A,B,C] [--limit N] [--no-notify]
    python -m src.jobs daily [--no-notify]

Exit codes: 0 success, 1 error, 2 another job holds the lock (argparse also
uses 2 for bad arguments), 3 completed with stale data (some holdings' bars end
before the last completed session, so their stop checks may be late).
"""

from __future__ import annotations

import argparse
import sys

from loguru import logger

from ..main import setup_logging
from .pipeline import JobAlreadyRunning, JobReport, run_daily, run_weekly

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_LOCKED = 2
EXIT_STALE = 3

EXIT_CODES_HELP = """exit codes:
  0  success
  1  error
  2  another job holds the lock (also argparse's code for bad arguments)
  3  completed with stale data: some holdings' bars end before the last
     completed session, so their stop checks may be late
"""


def _ticker_list(value: str) -> list[str]:
    tickers = [t.strip().upper() for t in value.split(",") if t.strip()]
    if not tickers:
        raise argparse.ArgumentTypeError("expected a comma-separated ticker list")
    return tickers


def _positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.jobs",
        description="TradingGui scheduled jobs",
        epilog=EXIT_CODES_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    commands = parser.add_subparsers(dest="command", required=True)

    weekly = commands.add_parser(
        "weekly", help="universe, metrics, alerts, recommendations, digest"
    )
    subset = weekly.add_mutually_exclusive_group()
    subset.add_argument(
        "--tickers",
        type=_ticker_list,
        help="comma-separated tickers to run instead of the universe (smoke run)",
    )
    subset.add_argument(
        "--limit",
        type=_positive_int,
        help="only the first N universe tickers, plus holdings (smoke run)",
    )
    weekly.add_argument("--no-notify", action="store_true", help="skip notifications")

    daily = commands.add_parser(
        "daily", help="positions and the ENTER/WATCH list on fresh bars"
    )
    daily.add_argument("--no-notify", action="store_true", help="skip notifications")
    return parser


def run(args: argparse.Namespace) -> JobReport:
    if args.command == "weekly":
        return run_weekly(
            tickers=args.tickers, limit=args.limit, notify=not args.no_notify
        )
    return run_daily(notify=not args.no_notify)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging()
    try:
        report = run(args)
    except JobAlreadyRunning as e:
        logger.error(f"{e}; not starting the {args.command} job")
        return EXIT_LOCKED
    except Exception:
        logger.exception(f"The {args.command} job failed")
        return EXIT_ERROR
    print(report.summary_line())
    if report.stale_holdings:
        logger.warning(
            f"The {args.command} job completed with stale data for "
            f"{len(report.stale_holdings)} holdings"
        )
        return EXIT_STALE
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
