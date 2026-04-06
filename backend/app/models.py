from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, Index, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class Member(Base):
    __tablename__ = "members"
    id: Mapped[int] = mapped_column(primary_key=True)
    bioguide_id: Mapped[str] = mapped_column(unique=True, index=True)
    first_name: Mapped[Optional[str]]
    last_name: Mapped[Optional[str]]
    chamber: Mapped[str]
    party: Mapped[Optional[str]]
    state: Mapped[Optional[str]]


class Security(Base):
    __tablename__ = "securities"
    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[Optional[str]] = mapped_column(unique=True, index=True)
    name: Mapped[str]
    asset_class: Mapped[str]
    sector: Mapped[Optional[str]]


class Filing(Base):
    __tablename__ = "filings"
    id: Mapped[int] = mapped_column(primary_key=True)
    member_id: Mapped[int]
    source: Mapped[str]
    filing_date: Mapped[Optional[date]]
    document_url: Mapped[Optional[str]]
    document_hash: Mapped[Optional[str]]


class Transaction(Base):
    __tablename__ = "transactions"
    id: Mapped[int] = mapped_column(primary_key=True)
    filing_id: Mapped[int]
    member_id: Mapped[int]
    security_id: Mapped[Optional[int]]

    owner_type: Mapped[str]
    transaction_type: Mapped[str]
    trade_date: Mapped[Optional[date]]
    report_date: Mapped[Optional[date]]
    amount_range_min: Mapped[Optional[float]]
    amount_range_max: Mapped[Optional[float]]
    description: Mapped[Optional[str]]


class InsiderTransaction(Base):
    __tablename__ = "insider_transactions"
    __table_args__ = (
        Index("ix_insider_transactions_symbol", "symbol"),
        Index("ix_insider_transactions_filing_date", "filing_date"),
        Index("ix_insider_transactions_external_id", "external_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str]
    external_id: Mapped[str] = mapped_column(unique=True)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reporting_cik: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    insider_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transaction_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    role: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ownership: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transaction_date: Mapped[Optional[date]]
    filing_date: Mapped[Optional[date]]
    shares: Mapped[Optional[float]]
    price: Mapped[Optional[float]]
    payload_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class InstitutionalTransaction(Base):
    __tablename__ = "institutional_transactions"
    __table_args__ = (
        Index("ix_institutional_transactions_symbol", "symbol"),
        Index("ix_institutional_transactions_filing_date", "filing_date"),
        Index("ix_institutional_transactions_external_id", "external_id"),
        Index("ix_institutional_transactions_institution_cik", "institution_cik"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str]
    external_id: Mapped[str] = mapped_column(unique=True)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    institution_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    institution_cik: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    filing_date: Mapped[Optional[date]]
    report_date: Mapped[Optional[date]]
    shares: Mapped[Optional[float]]
    market_value: Mapped[Optional[float]]
    avg_price: Mapped[Optional[float]]
    change_in_shares: Mapped[Optional[float]]
    change_pct: Mapped[Optional[float]]
    payload_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


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
        Index("ix_events_event_type_ts", "event_type", "ts"),
        Index("ix_events_ts", "ts"),
        Index("ix_events_symbol", "symbol"),
        Index("ix_events_member_bioguide_id", "member_bioguide_id"),
        Index("ix_events_party", "party"),
        Index("ix_events_chamber", "chamber"),
        Index("ix_events_trade_type", "trade_type"),
        Index("ix_events_event_date", "event_date"),
        Index("ix_events_symbol_event_date", "symbol", "event_date"),
        Index("ix_events_symbol_ts", "symbol", "ts"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    event_type: Mapped[str]
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    event_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source: Mapped[str]
    impact_score: Mapped[float] = mapped_column(
        default=0.0,
        server_default=text("0.0"),
    )
    payload_json: Mapped[str] = mapped_column(Text)
    member_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    member_bioguide_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    chamber: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    party: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    trade_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transaction_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    amount_min: Mapped[Optional[int]]
    amount_max: Mapped[Optional[int]]
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class QuoteCache(Base):
    __tablename__ = "quotes_cache"

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    price: Mapped[float]
    asof_ts: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class PriceCache(Base):
    __tablename__ = "price_cache"

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    date: Mapped[str] = mapped_column(Text, primary_key=True)
    close: Mapped[float]
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class TickerMeta(Base):
    __tablename__ = "ticker_meta"

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    company_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    exchange: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )


class CikMeta(Base):
    __tablename__ = "cik_meta"

    cik: Mapped[str] = mapped_column(Text, primary_key=True)
    company_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )


class TradeOutcome(Base):
    __tablename__ = "trade_outcomes"
    __table_args__ = (
        Index("ix_trade_outcomes_member_trade_date", "member_id", "trade_date"),
        Index("ix_trade_outcomes_status", "scoring_status"),
        Index("ix_trade_outcomes_symbol", "symbol"),
        Index("ix_trade_outcomes_benchmark_trade_date_member", "benchmark_symbol", "trade_date", "member_id"),
        Index(
            "ix_trade_outcomes_benchmark_status_trade_date_member",
            "benchmark_symbol",
            "scoring_status",
            "trade_date",
            "member_id",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[int] = mapped_column(unique=True, index=True)
    member_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    member_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    trade_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    trade_date: Mapped[Optional[date]]
    entry_price: Mapped[Optional[float]]
    entry_price_date: Mapped[Optional[date]]
    current_price: Mapped[Optional[float]]
    current_price_date: Mapped[Optional[date]]
    benchmark_symbol: Mapped[str] = mapped_column(Text, default="^GSPC", server_default="^GSPC")
    benchmark_entry_price: Mapped[Optional[float]]
    benchmark_current_price: Mapped[Optional[float]]
    return_pct: Mapped[Optional[float]]
    benchmark_return_pct: Mapped[Optional[float]]
    alpha_pct: Mapped[Optional[float]]
    holding_days: Mapped[Optional[int]]
    amount_min: Mapped[Optional[int]]
    amount_max: Mapped[Optional[int]]
    scoring_status: Mapped[str] = mapped_column(Text, default="ok", server_default="ok")
    scoring_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    methodology_version: Mapped[str] = mapped_column(Text, default="congress_v1", server_default="congress_v1")
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class CongressMemberAlias(Base):
    __tablename__ = "congress_member_aliases"
    __table_args__ = (
        Index("ix_congress_member_aliases_group_key", "group_key"),
        Index("ix_congress_member_aliases_chamber_group_key", "chamber", "group_key"),
    )

    alias_member_id: Mapped[str] = mapped_column(Text, primary_key=True)
    group_key: Mapped[str] = mapped_column(Text, nullable=False)
    authoritative_member_id: Mapped[str] = mapped_column(Text, nullable=False)
    member_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    member_slug: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    chamber: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    party: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    state: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )
