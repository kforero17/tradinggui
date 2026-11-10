"""Parse broker trade-history CSV exports and group fills into round trips.

Supported exports:

* E*TRADE / Morgan Stanley "Buys & Sells" activity (``DownloadTxnHistory.csv``).
* E*TRADE trade history (``tradesdownload.csv``), which names no account, so
  the caller supplies one.
* Fidelity account history (``Accounts_History.csv``).

Both have free-text preambles and disclaimer footers around the CSV table,
so each parser locates its header row and stops at the first blank line.
"""

from __future__ import annotations

import csv
import io
import re
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum

__all__ = [
    "Side",
    "Direction",
    "BrokerFormat",
    "Fill",
    "RoundTrip",
    "OpenPosition",
    "UnsupportedFormatError",
    "MissingAccountError",
    "detect_format",
    "parse_etrade",
    "parse_etrade_trades",
    "parse_fidelity",
    "parse_export",
    "apply_splits",
    "match_round_trips",
]

ETRADE_HEADER_PREFIX = "Activity/Trade Date,"
ETRADE_TRADES_HEADER_PREFIX = "Trade Date,Order Type,Security,"
FIDELITY_HEADER_PREFIX = "Run Date,Account,"
QUANTITY_TOLERANCE = 1e-6


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    SHORT = "SHORT"
    COVER = "COVER"

    @property
    def opens(self) -> bool:
        return self in (Side.BUY, Side.SHORT)


class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class BrokerFormat(str, Enum):
    ETRADE = "etrade"
    ETRADE_TRADES = "etrade_trades"
    FIDELITY = "fidelity"


class UnsupportedFormatError(ValueError):
    pass


class MissingAccountError(ValueError):
    """The export doesn't name its account, and none was supplied."""


@dataclass(frozen=True)
class Fill:
    account: str
    trade_date: date
    symbol: str
    side: Side
    quantity: float
    price: float
    fees: float = 0.0
    source: str = ""

    @property
    def direction(self) -> Direction:
        return Direction.LONG if self.side in (Side.BUY, Side.SELL) else Direction.SHORT

    @property
    def dedupe_key(self) -> str:
        return (
            f"{self.account}|{self.trade_date.isoformat()}|{self.symbol}|{self.side.value}|"
            f"{self.quantity:.6f}|{self.price:.6f}"
        )


ETRADE_SIDES = {
    "Bought": Side.BUY,
    "Sold": Side.SELL,
    "Sold Short": Side.SHORT,
    "Bought To Cover": Side.COVER,
}


def _number(text: str) -> float:
    cleaned = text.strip().replace(",", "").replace("$", "")
    if cleaned in ("", "--"):
        return 0.0
    return float(cleaned)


def _us_date(text: str) -> date:
    return datetime.strptime(text.strip(), "%m/%d/%Y").date()


def _table_rows(text: str, header_prefix: str) -> list[dict[str, str]]:
    lines = text.lstrip("﻿").splitlines()
    start = next(
        i for i, line in enumerate(lines) if line.lstrip("﻿").startswith(header_prefix)
    )
    table: list[str] = []
    for line in lines[start:]:
        if not line.strip():
            break
        table.append(line)
    return list(csv.DictReader(io.StringIO("\n".join(table))))


def detect_format(text: str) -> BrokerFormat:
    if ETRADE_HEADER_PREFIX in text:
        return BrokerFormat.ETRADE
    if ETRADE_TRADES_HEADER_PREFIX in text:
        return BrokerFormat.ETRADE_TRADES
    if FIDELITY_HEADER_PREFIX in text:
        return BrokerFormat.FIDELITY
    raise UnsupportedFormatError("Not an E*TRADE or Fidelity trade-history export")


def _etrade_account(text: str) -> str:
    match = re.search(r"Account Activity for (.+?) from ", text)
    return match.group(1).strip() if match else "E*TRADE"


def parse_etrade(text: str, source: str = "") -> list[Fill]:
    account = _etrade_account(text)
    fills: list[Fill] = []
    for row in _table_rows(text, ETRADE_HEADER_PREFIX):
        side = ETRADE_SIDES.get(row["Activity Type"].strip())
        if side is None:
            continue
        fills.append(
            Fill(
                account=account,
                trade_date=_us_date(row["Activity/Trade Date"]),
                symbol=row["Symbol"].strip().upper(),
                side=side,
                quantity=abs(_number(row["Quantity #"])),
                price=_number(row["Price $"]),
                fees=_number(row["Commission"]),
                source=source,
            )
        )
    return fills


def _fidelity_side(action: str) -> Side | None:
    action = action.upper()
    if "SHORT" in action and "SOLD" in action:
        return Side.SHORT
    if "COVER" in action:
        return Side.COVER
    if action.startswith("YOU BOUGHT"):
        return Side.BUY
    if action.startswith("YOU SOLD"):
        return Side.SELL
    return None


def parse_fidelity(text: str, source: str = "") -> list[Fill]:
    fills: list[Fill] = []
    for row in _table_rows(text, FIDELITY_HEADER_PREFIX):
        side = _fidelity_side(row["Action"])
        # Corporate-action rows (e.g. reverse splits) carry a CUSIP but no symbol.
        if side is None or not row["Symbol"].strip():
            continue
        account_number = row["Account Number"].strip()
        fills.append(
            Fill(
                account=f"{row['Account'].strip()} -{account_number[-4:]}",
                trade_date=_us_date(row["Run Date"]),
                symbol=row["Symbol"].strip().upper(),
                side=side,
                quantity=abs(_number(row["Quantity"])),
                price=_number(row["Price ($)"]),
                fees=_number(row["Commission ($)"]) + _number(row["Fees ($)"]),
                source=source,
            )
        )
    return fills


ETRADE_TRADES_SIDES = {
    "BUY": Side.BUY,
    "SELL": Side.SELL,
    "SELL SHORT": Side.SHORT,
    "BUY TO COVER": Side.COVER,
}


def parse_etrade_trades(text: str, account: str, source: str = "") -> list[Fill]:
    """E*TRADE's trade-history download; dividend reinvestments count as buys because they add to the cost basis."""
    fills: list[Fill] = []
    for row in _table_rows(text, ETRADE_TRADES_HEADER_PREFIX):
        side = ETRADE_TRADES_SIDES.get(row["Order Type"].strip().upper())
        if side is None or not row["Security"].strip():
            continue
        fills.append(
            Fill(
                account=account,
                trade_date=_us_date(row["Trade Date"]),
                symbol=row["Security"].strip().upper(),
                side=side,
                quantity=abs(_number(row["Quantity"])),
                price=_number(row["Executed Price"]),
                fees=_number(row["Commission"]),
                source=source,
            )
        )
    return fills


def parse_export(text: str, source: str = "", account: str | None = None) -> list[Fill]:
    """Parse any supported export; ``account`` is required only for exports that don't name one."""
    match detect_format(text):
        case BrokerFormat.ETRADE:
            return parse_etrade(text, source)
        case BrokerFormat.ETRADE_TRADES:
            if not account:
                raise MissingAccountError(
                    "This E*TRADE trade download doesn't say which account it is from. Choose the account and import it again."
                )
            return parse_etrade_trades(text, account, source)
        case BrokerFormat.FIDELITY:
            return parse_fidelity(text, source)


def apply_splits(
    fills: list[Fill], splits_by_symbol: dict[str, list[tuple[date, float]]]
) -> list[Fill]:
    """Restate fills on today's share basis so quantities match across a split and prices match split-adjusted history."""
    adjusted: list[Fill] = []
    for fill in fills:
        factor = 1.0
        for split_date, ratio in splits_by_symbol.get(fill.symbol, []):
            if split_date > fill.trade_date:
                factor *= ratio
        if factor == 1.0:
            adjusted.append(fill)
        else:
            adjusted.append(
                Fill(
                    account=fill.account,
                    trade_date=fill.trade_date,
                    symbol=fill.symbol,
                    side=fill.side,
                    quantity=fill.quantity * factor,
                    price=fill.price / factor,
                    fees=fill.fees,
                    source=fill.source,
                )
            )
    return adjusted


@dataclass(frozen=True)
class RoundTrip:
    """One position episode in one account: from flat, through every fill, back to flat.

    ``unmatched_quantity`` counts shares closed without an opening fill in the
    imported history (lots bought before the export window). Their P&L is
    unknown, so ``pnl`` covers only the matched shares.
    """

    account: str
    symbol: str
    direction: Direction
    entry_date: date
    exit_date: date
    quantity: float
    entry_price: float
    exit_price: float
    fees: float
    unmatched_quantity: float
    fills: tuple[Fill, ...]

    @property
    def basis_known(self) -> bool:
        return self.unmatched_quantity <= QUANTITY_TOLERANCE

    @property
    def pnl(self) -> float:
        sign = 1.0 if self.direction is Direction.LONG else -1.0
        return sign * (self.exit_price - self.entry_price) * self.quantity - self.fees

    @property
    def return_pct(self) -> float:
        cost = self.entry_price * self.quantity
        return self.pnl / cost if cost else 0.0

    @property
    def holding_days(self) -> int:
        return (self.exit_date - self.entry_date).days


@dataclass(frozen=True)
class OpenPosition:
    account: str
    symbol: str
    direction: Direction
    entry_date: date
    quantity: float
    entry_price: float


@dataclass
class _Lot:
    quantity: float
    price: float
    trade_date: date


@dataclass
class _Episode:
    direction: Direction
    fills: list[Fill] = field(default_factory=list)
    matched_quantity: float = 0.0
    matched_cost: float = 0.0
    matched_proceeds: float = 0.0
    unmatched_quantity: float = 0.0
    first_matched_entry: date | None = None


def _close_episode(account: str, symbol: str, episode: _Episode) -> RoundTrip:
    quantity = episode.matched_quantity
    first_fill_date = episode.fills[0].trade_date
    return RoundTrip(
        account=account,
        symbol=symbol,
        direction=episode.direction,
        entry_date=episode.first_matched_entry or first_fill_date,
        exit_date=episode.fills[-1].trade_date,
        quantity=quantity,
        entry_price=episode.matched_cost / quantity if quantity else 0.0,
        exit_price=episode.matched_proceeds / quantity if quantity else 0.0,
        fees=sum(f.fees for f in episode.fills),
        unmatched_quantity=episode.unmatched_quantity,
        fills=tuple(episode.fills),
    )


def _continues_same_day_close(group: list[Fill], position: int) -> bool:
    """Brokers split one sale into a whole-share and a fractional fill; keep them in one episode."""
    if position + 1 >= len(group):
        return False
    current, following = group[position], group[position + 1]
    return following.trade_date == current.trade_date and following.side is current.side


def match_round_trips(fills: list[Fill]) -> tuple[list[RoundTrip], list[OpenPosition]]:
    """FIFO-match fills per (account, symbol) into round trips, plus whatever is still open."""
    by_key: dict[tuple[str, str], list[Fill]] = defaultdict(list)
    for fill in fills:
        by_key[(fill.account, fill.symbol)].append(fill)

    round_trips: list[RoundTrip] = []
    open_positions: list[OpenPosition] = []
    for (account, symbol), group in by_key.items():
        # Same-day opens sort before closes: the exports carry no time of day.
        group.sort(key=lambda f: (f.trade_date, not f.side.opens))
        lots: dict[Direction, deque[_Lot]] = {
            Direction.LONG: deque(),
            Direction.SHORT: deque(),
        }
        episodes: dict[Direction, _Episode | None] = {
            Direction.LONG: None,
            Direction.SHORT: None,
        }

        for position, fill in enumerate(group):
            direction = fill.direction
            episode = episodes[direction] or _Episode(direction)
            episodes[direction] = episode
            episode.fills.append(fill)
            if fill.side.opens:
                lots[direction].append(_Lot(fill.quantity, fill.price, fill.trade_date))
                continue

            remaining = fill.quantity
            queue = lots[direction]
            while remaining > QUANTITY_TOLERANCE and queue:
                lot = queue[0]
                taken = min(lot.quantity, remaining)
                if episode.first_matched_entry is None:
                    episode.first_matched_entry = lot.trade_date
                episode.matched_quantity += taken
                episode.matched_cost += taken * lot.price
                episode.matched_proceeds += taken * fill.price
                lot.quantity -= taken
                remaining -= taken
                if lot.quantity <= QUANTITY_TOLERANCE:
                    queue.popleft()
            if remaining > QUANTITY_TOLERANCE:
                episode.unmatched_quantity += remaining
            if not queue and not _continues_same_day_close(group, position):
                round_trips.append(_close_episode(account, symbol, episode))
                episodes[direction] = None

        for direction, queue in lots.items():
            if not queue:
                continue
            quantity = sum(lot.quantity for lot in queue)
            open_positions.append(
                OpenPosition(
                    account=account,
                    symbol=symbol,
                    direction=direction,
                    entry_date=queue[0].trade_date,
                    quantity=quantity,
                    entry_price=sum(lot.quantity * lot.price for lot in queue)
                    / quantity,
                )
            )

    round_trips.sort(key=lambda t: (t.exit_date, t.symbol))
    return round_trips, open_positions
