from pathlib import Path

import pandas as pd
import pytest
import requests

from src.config.settings import settings
from src.data import universe_sources
from src.data.ticker_loader import (
    TickerInfo,
    is_valid_ticker,
    load_universe,
    ticker_loader,
)
from src.data.universe_sources import (
    NASDAQ_LISTED_URL,
    OTHER_LISTED_URL,
    SP500_URL,
    parse_nasdaq_listed,
    parse_other_listed,
    parse_sp500_html,
    refresh_universe_files,
    to_yfinance_symbol,
)

FIXTURES = Path(__file__).parent / "fixtures"
NASDAQ_TEXT = (FIXTURES / "nasdaqlisted.txt").read_text()
OTHER_TEXT = (FIXTURES / "otherlisted.txt").read_text()
SP500_HTML = (FIXTURES / "sp500.html").read_text()


class _Response:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        pass


def _fake_get(pages: dict[str, str | Exception]):
    def get(url: str, **kwargs: object) -> _Response:
        assert "User-Agent" in kwargs["headers"]
        page = pages[url]
        if isinstance(page, Exception):
            raise page
        return _Response(page)

    return get


@pytest.fixture
def universe_paths(tmp_path, monkeypatch):
    paths = {
        "LISTED_CSV_PATH": tmp_path / "us_listed.csv",
        "SP500_CSV_PATH": tmp_path / "sp500.csv",
        "NASDAQ_CSV_PATH": tmp_path / "nasdaq.csv",
    }
    for key, path in paths.items():
        monkeypatch.setattr(settings, key, path)
    monkeypatch.setattr(settings, "UNIVERSE_MIN_LISTED_ROWS", 5)
    monkeypatch.setattr(settings, "UNIVERSE_MIN_SP500_ROWS", 3)
    monkeypatch.setattr(settings, "UNIVERSE_INCLUDE_ETFS", True)
    return paths


@pytest.fixture
def live_pages(monkeypatch):
    pages: dict[str, str | Exception] = {
        NASDAQ_LISTED_URL: NASDAQ_TEXT,
        OTHER_LISTED_URL: OTHER_TEXT,
        SP500_URL: SP500_HTML,
    }
    monkeypatch.setattr(universe_sources.requests, "get", _fake_get(pages))
    return pages


@pytest.mark.parametrize("disable, verify", [("true", False), ("false", True)])
def test_fetch_text_follows_disable_ssl_verify(monkeypatch, disable, verify):
    # requests ignores the global ssl override, so behind the corporate proxy
    # the S&P download failed with CERTIFICATE_VERIFY_FAILED until verify was passed.
    calls = []
    monkeypatch.setenv("DISABLE_SSL_VERIFY", disable)
    monkeypatch.setattr(
        universe_sources.requests,
        "get",
        lambda url, **kw: calls.append(kw) or _Response("ok"),
    )
    assert universe_sources._fetch_text(SP500_URL) == "ok"
    assert calls[0]["verify"] is verify


def test_to_yfinance_symbol_maps_class_dots_to_dashes():
    assert to_yfinance_symbol(" brk.b ") == "BRK-B"
    assert to_yfinance_symbol("AAPL") == "AAPL"


def test_parse_nasdaq_listed_drops_test_issues_footer_and_spac_securities():
    listed = parse_nasdaq_listed(NASDAQ_TEXT)

    assert list(listed.columns) == ["symbol", "name", "exchange", "is_etf"]
    # Units, warrant, rights, the test issue and the footer are gone; LP common units and "NA" stay.
    assert listed["symbol"].tolist() == ["AAPL", "ARLP", "MSFT", "NA", "QQQ"]
    assert set(listed["exchange"]) == {"NASDAQ"}
    etfs = listed.set_index("symbol")["is_etf"]
    assert bool(etfs["QQQ"]) and not bool(etfs["AAPL"])


def test_parse_other_listed_maps_dots_and_keeps_etf_and_exchange():
    listed = parse_other_listed(OTHER_TEXT).set_index("symbol")

    assert listed.index.tolist() == ["A", "BF-B", "BRK-B", "ET", "JPM", "SPY"]
    assert listed.loc["SPY", "exchange"] == "NYSE Arca"
    assert bool(listed.loc["SPY", "is_etf"])
    assert listed.loc["BRK-B", "exchange"] == "NYSE"
    assert not bool(listed.loc["BRK-B", "is_etf"])
    # Unknown exchange codes are kept as-is.
    assert listed.loc["ET", "exchange"] == "F"
    assert listed.loc["A", "name"] == "Agilent Technologies, Inc. Common Stock"


def test_parse_sp500_html_picks_constituents_table():
    sp500 = parse_sp500_html(SP500_HTML)

    assert sp500["Symbol"].tolist() == ["MMM", "AAPL", "BRK-B", "BF-B", "JPM"]
    assert (
        sp500.loc[sp500["Symbol"] == "BRK-B", "Security"].item() == "Berkshire Hathaway"
    )


def test_refresh_writes_both_files(universe_paths, live_pages):
    result = refresh_universe_files()

    assert result.ok
    assert (result.listed_rows, result.listed_etfs, result.sp500_rows) == (11, 2, 5)
    written = pd.read_csv(universe_paths["LISTED_CSV_PATH"], keep_default_na=False)
    assert list(written.columns) == ["symbol", "name", "exchange", "is_etf"]
    assert "BRK-B" in written["symbol"].tolist()
    assert "NA" in written["symbol"].tolist()
    sp500 = pd.read_csv(universe_paths["SP500_CSV_PATH"])
    assert sp500["Symbol"].tolist() == ["MMM", "AAPL", "BRK-B", "BF-B", "JPM"]


def test_failed_sanity_check_keeps_old_file(universe_paths, live_pages, monkeypatch):
    listed_path = universe_paths["LISTED_CSV_PATH"]
    listed_path.write_text("symbol,name,exchange,is_etf\nOLD,Old Co,NYSE,False\n")
    monkeypatch.setattr(settings, "UNIVERSE_MIN_LISTED_ROWS", 5000)

    result = refresh_universe_files()

    assert not result.listed_ok and result.listed_rows == 0
    assert "expected at least 5000" in result.listed_error
    assert (
        listed_path.read_text()
        == "symbol,name,exchange,is_etf\nOLD,Old Co,NYSE,False\n"
    )
    assert not list(listed_path.parent.glob("*.tmp"))
    # The S&P file is independent and still refreshed.
    assert result.sp500_ok and universe_paths["SP500_CSV_PATH"].exists()


def test_failed_download_keeps_old_file(universe_paths, live_pages):
    sp500_path = universe_paths["SP500_CSV_PATH"]
    sp500_path.write_text("Symbol\nOLD\n")
    live_pages[SP500_URL] = requests.ConnectionError("offline")

    result = refresh_universe_files()

    assert result.listed_ok
    assert not result.sp500_ok and "offline" in result.sp500_error
    assert not result.ok
    assert sp500_path.read_text() == "Symbol\nOLD\n"


def test_load_universe_flags_etfs_and_sp500_members(universe_paths, live_pages):
    refresh_universe_files()

    universe = load_universe()

    assert universe["BRK-B"] == TickerInfo(
        "BRK-B", "Berkshire Hathaway Inc. New Common Stock", "NYSE", False, True
    )
    assert universe["SPY"].is_etf and not universe["SPY"].in_sp500
    assert universe["QQQ"].is_etf and universe["QQQ"].exchange == "NASDAQ"
    # In the S&P file but not in the listings fixture.
    assert universe["MMM"] == TickerInfo("MMM", "3M", "", False, True)
    assert not universe["MSFT"].in_sp500
    # S&P members come first, the way load_unique_tickers always ordered them.
    tickers = ticker_loader.load_unique_tickers()
    assert tickers[:5] == ["MMM", "AAPL", "BRK-B", "BF-B", "JPM"]
    assert len(tickers) == len(set(tickers)) == 12


def test_load_universe_can_exclude_etfs(universe_paths, live_pages, monkeypatch):
    refresh_universe_files()
    monkeypatch.setattr(settings, "UNIVERSE_INCLUDE_ETFS", False)

    universe = load_universe()

    assert "SPY" not in universe and "QQQ" not in universe
    assert "AAPL" in universe


def test_load_unique_tickers_falls_back_to_legacy_nasdaq_csv(universe_paths):
    universe_paths["SP500_CSV_PATH"].write_text("﻿Name\nMSFT\nBRK.B\n")
    universe_paths["NASDAQ_CSV_PATH"].write_text("Symbol\nAAPL\nmsft\nBRK/B\nBFS^D\n")

    assert ticker_loader.validate_ticker_files()
    tickers = ticker_loader.load_unique_tickers()

    # Legacy symbols with "/" or "^" still fail validation, as before.
    assert tickers == ["MSFT", "BRK-B", "AAPL"]


def test_validate_ticker_files_reports_missing(universe_paths):
    assert not ticker_loader.validate_ticker_files()
    universe_paths["SP500_CSV_PATH"].write_text("Symbol\nAAPL\n")
    assert not ticker_loader.validate_ticker_files()
    universe_paths["LISTED_CSV_PATH"].write_text("symbol,name,exchange,is_etf\n")
    assert ticker_loader.validate_ticker_files()


@pytest.mark.parametrize(
    ("ticker", "valid"),
    [("BRK-B", True), ("BRK.B", True), ("AAPL", True), ("BFS^D", False), ("", False)],
)
def test_ticker_validation(ticker, valid):
    assert is_valid_ticker(ticker) is valid
