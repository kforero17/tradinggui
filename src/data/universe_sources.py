"""Refresh the universe CSVs from the NASDAQ Trader symbol directory and Wikipedia.

The parse functions are pure (text in, DataFrame out) so they can be tested
against fixtures; ``refresh_universe_files`` does the I/O. Each file is written
to a temp file and atomically renamed, and a failed download or a row count
below the sanity threshold keeps the previous file, so a bad refresh never
empties the universe.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from loguru import logger

from ..config.settings import settings

__all__ = [
    "LISTED_COLUMNS",
    "NASDAQ_LISTED_URL",
    "OTHER_LISTED_URL",
    "SP500_URL",
    "UniverseRefreshResult",
    "parse_nasdaq_listed",
    "parse_other_listed",
    "parse_sp500_html",
    "refresh_universe_files",
    "to_yfinance_symbol",
]

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
# Wikipedia rejects requests without a descriptive User-Agent.
REQUEST_HEADERS = {"User-Agent": "TradingGui/1.0 (personal research tool)"}
REQUEST_TIMEOUT_SECONDS = 30

LISTED_COLUMNS = ["symbol", "name", "exchange", "is_etf"]
FOOTER_PREFIX = "File Creation Time"

# Warrants and rights by name. SPAC units are matched by their "- Units" suffix
# or "Units, each consisting of ..." wording, so MLP common units (ET, EPD, AB)
# stay in the universe.
EXCLUDED_NAME_PATTERN = re.compile(
    r"\b(?:warrants?|rights?)\b|-\s*units?\.?\s*$|\bunits?,?\s+each\b",
    re.IGNORECASE,
)
# otherlisted uses ACT symbols, where these suffixes mark units, warrants and rights.
EXCLUDED_SYMBOL_PATTERN = re.compile(r"\.(?:U|UN|W|WS|R|RT)$")

# otherlisted "Exchange" codes; anything else is kept as the raw code.
OTHER_EXCHANGES = {
    "A": "NYSE American",
    "N": "NYSE",
    "P": "NYSE Arca",
    "Z": "Cboe BZX",
    "V": "IEX",
}


@dataclass(frozen=True)
class UniverseRefreshResult:
    """Outcome of one ``refresh_universe_files`` call; row counts are 0 when a file was kept."""

    listed_rows: int
    listed_etfs: int
    sp500_rows: int
    listed_ok: bool
    sp500_ok: bool
    listed_error: str | None = None
    sp500_error: str | None = None

    @property
    def ok(self) -> bool:
        return self.listed_ok and self.sp500_ok


def to_yfinance_symbol(symbol: str) -> str:
    """Class-share dots become dashes, the form yfinance expects (``BRK.B`` → ``BRK-B``)."""
    return symbol.strip().upper().replace(".", "-")


def _read_pipe_table(text: str) -> pd.DataFrame:
    # keep_default_na=False so a real symbol like "NA" isn't read as missing.
    frame = pd.read_csv(StringIO(text), sep="|", dtype=str, keep_default_na=False)
    return frame.apply(lambda column: column.str.strip())


def _finish_listing(
    symbols: pd.Series, names: pd.Series, exchanges: pd.Series, etf_flags: pd.Series
) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "symbol": symbols.map(to_yfinance_symbol),
            "name": names,
            "exchange": exchanges,
            "is_etf": etf_flags.eq("Y"),
        }
    )
    frame = frame[frame["symbol"].ne("")]
    return (
        frame.drop_duplicates(subset="symbol")
        .sort_values("symbol")
        .reset_index(drop=True)[LISTED_COLUMNS]
    )


def parse_nasdaq_listed(text: str) -> pd.DataFrame:
    """Parse ``nasdaqlisted.txt`` into ``symbol, name, exchange, is_etf`` rows.

    Test issues, the footer row, warrants, rights and units are dropped.
    """
    raw = _read_pipe_table(text)
    keep = (
        ~raw["Symbol"].str.startswith(FOOTER_PREFIX)
        & raw["Test Issue"].ne("Y")
        & ~raw["Security Name"].str.contains(EXCLUDED_NAME_PATTERN)
    )
    raw = raw[keep]
    return _finish_listing(
        raw["Symbol"],
        raw["Security Name"],
        pd.Series("NASDAQ", index=raw.index),
        raw["ETF"],
    )


def parse_other_listed(text: str) -> pd.DataFrame:
    """Parse ``otherlisted.txt`` (NYSE, NYSE American, Arca, Cboe, ...) like ``parse_nasdaq_listed``.

    Preferred shares (``$`` in the ACT symbol) are dropped too; yfinance uses a
    different symbol scheme for them.
    """
    raw = _read_pipe_table(text)
    symbols = raw["ACT Symbol"]
    keep = (
        ~symbols.str.startswith(FOOTER_PREFIX)
        & raw["Test Issue"].ne("Y")
        & ~raw["Security Name"].str.contains(EXCLUDED_NAME_PATTERN)
        & ~symbols.str.contains(EXCLUDED_SYMBOL_PATTERN)
        & ~symbols.str.contains("$", regex=False)
    )
    raw = raw[keep]
    return _finish_listing(
        raw["ACT Symbol"],
        raw["Security Name"],
        raw["Exchange"].map(lambda code: OTHER_EXCHANGES.get(code, code)),
        raw["ETF"],
    )


def parse_sp500_html(html: str) -> pd.DataFrame:
    """The constituents table of the Wikipedia page as ``Symbol, Security`` rows (dots mapped to dashes)."""
    tables = pd.read_html(StringIO(html), match="Symbol")
    table = next((t for t in tables if "Symbol" in t.columns), None)
    if table is None:
        raise ValueError("No table with a 'Symbol' column found in the S&P 500 page")
    frame = pd.DataFrame(
        {
            "Symbol": table["Symbol"].astype(str).map(to_yfinance_symbol),
            "Security": table["Security"].astype(str).str.strip()
            if "Security" in table.columns
            else "",
        }
    )
    frame = frame[frame["Symbol"].ne("")]
    return frame.drop_duplicates(subset="Symbol").reset_index(drop=True)


def _fetch_text(url: str) -> str:
    response = requests.get(
        url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS
    )
    response.raise_for_status()
    return response.text


def _write_atomic(frame: pd.DataFrame, path: Path, min_rows: int) -> None:
    """Replace ``path`` with ``frame`` only if it passes the row-count sanity check."""
    if len(frame) < min_rows:
        raise ValueError(
            f"{path.name}: only {len(frame)} rows, expected at least {min_rows}; keeping the old file"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    try:
        frame.to_csv(tmp, index=False)
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def _refresh_listed() -> pd.DataFrame:
    listed = pd.concat(
        [
            parse_nasdaq_listed(_fetch_text(NASDAQ_LISTED_URL)),
            parse_other_listed(_fetch_text(OTHER_LISTED_URL)),
        ],
        ignore_index=True,
    )
    listed = (
        listed.drop_duplicates(subset="symbol")
        .sort_values("symbol")
        .reset_index(drop=True)
    )
    _write_atomic(listed, settings.LISTED_CSV_PATH, settings.UNIVERSE_MIN_LISTED_ROWS)
    return listed


def _refresh_sp500() -> pd.DataFrame:
    sp500 = parse_sp500_html(_fetch_text(SP500_URL))
    _write_atomic(sp500, settings.SP500_CSV_PATH, settings.UNIVERSE_MIN_SP500_ROWS)
    return sp500


def refresh_universe_files() -> UniverseRefreshResult:
    """Download the US listings and the S&P 500 list; each file succeeds or fails on its own."""
    listed_rows = listed_etfs = sp500_rows = 0
    listed_error: str | None = None
    sp500_error: str | None = None

    try:
        listed = _refresh_listed()
        listed_rows, listed_etfs = len(listed), int(listed["is_etf"].sum())
        logger.info(
            f"Wrote {listed_rows} US listings ({listed_etfs} ETFs) to {settings.LISTED_CSV_PATH}"
        )
    except Exception as e:
        listed_error = f"{type(e).__name__}: {e}"
        logger.error(
            f"US listings refresh failed, keeping the old file: {listed_error}"
        )

    try:
        sp500_rows = len(_refresh_sp500())
        logger.info(f"Wrote {sp500_rows} S&P 500 symbols to {settings.SP500_CSV_PATH}")
    except Exception as e:
        sp500_error = f"{type(e).__name__}: {e}"
        logger.error(f"S&P 500 refresh failed, keeping the old file: {sp500_error}")

    return UniverseRefreshResult(
        listed_rows=listed_rows,
        listed_etfs=listed_etfs,
        sp500_rows=sp500_rows,
        listed_ok=listed_error is None,
        sp500_ok=sp500_error is None,
        listed_error=listed_error,
        sp500_error=sp500_error,
    )
