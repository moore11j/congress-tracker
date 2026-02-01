from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import DateTime, Index, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class Member(Base):
    __tablename__ = "members"
    id: Mapped[int] = mapped_column(primary_key=True)
    bioguide_id: Mapped[str] = mapped_column(unique=True, index=True)
    first_name: Mapped[str | None]
    last_name: Mapped[str | None]
    chamber: Mapped[str]
    party: Mapped[str | None]
    state: Mapped[str | None]


class Security(Base):
    __tablename__ = "securities"
    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str | None] = mapped_column(unique=True, index=True)
    name: Mapped[str]
    asset_class: Mapped[str]
    sector: Mapped[str | None]


class Filing(Base):
    __tablename__ = "filings"
    id: Mapped[int] = mapped_column(primary_key=True)
    member_id: Mapped[int]
    source: Mapped[str]
    filing_date: Mapped[date | None]
    document_url: Mapped[str | None]
    document_hash: Mapped[str | None]


class Transaction(Base):
    __tablename__ = "transactions"
    id: Mapped[int] = mapped_column(primary_key=True)
    filing_id: Mapped[int]
    member_id: Mapped[int]
    security_id: Mapped[int | None]

    owner_type: Mapped[str]
    transaction_type: Mapped[str]
    trade_date: Mapped[date | None]
    report_date: Mapped[date | None]
    amount_range_min: Mapped[float | None]
    amount_range_max: Mapped[float | None]
    description: Mapped[str | None]


class Watchlist(Base):
    __tablename__ = "watchlists"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True)


class WatchlistItem(Base):
    __tablename__ = "watchlist_items"
    id: Mapped[int] = mapped_column(primary_key=True)
    watchlist_id: Mapped[int]
    security_id: Mapped[int]


class Event(Base):
    __tablename__ = "events"
    __table_args__ = (
        Index("ix_events_ticker_ts", "ticker", "ts"),
        Index("ix_events_event_type_ts", "event_type", "ts"),
        Index("ix_events_symbol", "symbol"),
        Index("ix_events_member_bioguide_id", "member_bioguide_id"),
        Index("ix_events_party", "party"),
        Index("ix_events_chamber", "chamber"),
        Index("ix_events_trade_type", "trade_type"),
        Index("ix_events_event_date", "event_date"),
        Index("ix_events_symbol_event_date", "symbol", "event_date"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    event_type: Mapped[str]
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    event_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ticker: Mapped[str]
    symbol: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str]
    headline: Mapped[str | None]
    summary: Mapped[str | None]
    url: Mapped[str | None]
    impact_score: Mapped[float] = mapped_column(
        default=0.0,
        server_default=text("0.0"),
    )
    payload_json: Mapped[str] = mapped_column(Text)
    member_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    member_bioguide_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    chamber: Mapped[str | None] = mapped_column(Text, nullable=True)
    party: Mapped[str | None] = mapped_column(Text, nullable=True)
    transaction_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    trade_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    amount_min: Mapped[int | None]
    amount_max: Mapped[int | None]
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
