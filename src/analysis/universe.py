"""The tradable trend-following universe: liquid listings plus holdings and the watchlist.

Liquidity comes from the latest ``valuation_momentum`` metrics (last price and
50-day average dollar volume), so no price history has to be loaded to pick
the universe. Leveraged and inverse ETFs are dropped by name: their daily
reset makes long-run trend rules unreliable on them.
"""

from __future__ import annotations

import math
import re
from collections.abc import Collection, Iterable, Mapping

import pandas as pd
from loguru import logger

from ..data.ticker_loader import TickerInfo

__all__ = [
    "LEVERAGED_NAME_PATTERN",
    "is_leveraged_or_inverse",
    "liquid_universe",
    "with_required",
]

PRICE_COLUMN = "last_price"
DOLLAR_VOLUME_COLUMN = "avg_dollar_volume_50d"

# Matched case-insensitively against ETF/ETN names; checked against the real
# us_listed.csv ETF names. Bare "Ultra", "Short", "Bull" and "Bear" are not enough on
# their own: "Ultra Short Duration", "Ultrashort Income", "Ultra-Small Company",
# "Ultra Buffer", "Short Treasury", "Bull Hedge" and "BulletShares" funds are unlevered.
LEVERAGED_NAME_PATTERN = re.compile(
    r"""
    (?<![\w.])[-+]?\d+(?:\.\d+)?\s?[x×]\b       # 2x, 3X, -1x, 1.25X (Direxion "Bull 3X", "Bear 1X")
    | \bUltraPro\b                              # ProShares UltraPro QQQ, UltraPro Short
    | \bUltraShort\b(?!\s+(?:Income|Bond|Fixed|Treasury|Tax|Muni|Municipal|Duration)\b)
    | \bProShares\s+(?:Ultra|Short)\b           # ProShares Ultra QQQ (2x), Short S&P500 (-1x)
    | \bInverse\b
    | \bLeveraged\b(?!\s+Loans?\b)
    | \bLeverage\s+Shares\b                     # issuer of daily leveraged single-stock ETPs
    | \b(?:Daily|Weekly)\s+(?:Long|Short)\b     # "Daily Short", "Weekly Long" resets
    """,
    re.IGNORECASE | re.VERBOSE,
)


def is_leveraged_or_inverse(name: str) -> bool:
    """True when an ETF/ETN name marks it as leveraged (2x, Ultra, ...) or inverse (Bear, -1x, ...).

    Meant for fund names only: some operating companies have names that would
    match (``10x Genomics``), so ``liquid_universe`` applies it to ETFs alone.
    """
    return bool(name) and LEVERAGED_NAME_PATTERN.search(name) is not None


def _finite(value: object) -> float | None:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def liquid_universe(
    metrics: pd.DataFrame,
    universe: Mapping[str, TickerInfo],
    *,
    min_price: float,
    min_dollar_volume: float,
    exclude_leveraged: bool = True,
    exclude: Collection[str] = (),
) -> list[str]:
    """Universe tickers whose latest metrics clear the price and dollar-volume floors.

    A ticker needs a metrics row with a finite ``last_price`` and
    ``avg_dollar_volume_50d``; missing or NaN values exclude it, as does a
    missing column. Tickers absent from ``universe`` (delisted since their
    metrics were stored) are dropped. With ``exclude_leveraged`` set,
    leveraged and inverse ETFs are dropped by name. Tickers in ``exclude``
    (the metrics job's skip list: their metrics rows stopped updating, so the
    stored price and volume are stale) are dropped. The result is ordered by
    dollar volume, most liquid first.
    """
    missing = [
        c for c in ("ticker", PRICE_COLUMN, DOLLAR_VOLUME_COLUMN) if c not in metrics
    ]
    if missing:
        logger.warning(
            f"Metrics have no {missing} column(s); the liquid universe is empty"
        )
        return []

    excluded = {t.strip().upper() for t in exclude}
    liquid: list[tuple[float, str]] = []
    leveraged = skip_listed = 0
    for ticker, price, dollar_volume in zip(
        metrics["ticker"], metrics[PRICE_COLUMN], metrics[DOLLAR_VOLUME_COLUMN]
    ):
        info = universe.get(str(ticker).upper())
        price, dollar_volume = _finite(price), _finite(dollar_volume)
        if info is None or price is None or dollar_volume is None:
            continue
        if price < min_price or dollar_volume < min_dollar_volume:
            continue
        if exclude_leveraged and info.is_etf and is_leveraged_or_inverse(info.name):
            leveraged += 1
            continue
        if info.symbol.upper() in excluded:
            skip_listed += 1
            continue
        liquid.append((dollar_volume, info.symbol))

    liquid.sort(key=lambda item: (-item[0], item[1]))
    tickers = list(dict.fromkeys(symbol for _, symbol in liquid))
    logger.info(
        f"Liquid universe: {len(tickers)} of {len(metrics)} metrics rows "
        f"(price >= {min_price}, 50d $vol >= {min_dollar_volume:,.0f}, "
        f"{leveraged} leveraged/inverse ETFs and {skip_listed} skip-listed tickers dropped)"
    )
    return tickers


def with_required(
    tickers: Iterable[str], holdings: Iterable[str], watchlist: Iterable[str]
) -> list[str]:
    """``tickers`` followed by any holdings and watchlist names not already in it.

    Symbols are stripped and upper-cased; first occurrence wins, so the order is kept.
    """
    combined = (
        t.strip().upper() for group in (tickers, holdings, watchlist) for t in group
    )
    return list(dict.fromkeys(t for t in combined if t))
