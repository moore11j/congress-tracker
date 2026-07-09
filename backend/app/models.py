from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import BigInteger, Boolean, DateTime, Float, Index, Text, UniqueConstraint, func, text
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


class InstitutionalHolder(Base):
    __tablename__ = "institutional_holders"
    __table_args__ = (
        Index("ix_institutional_holders_name", "normalized_holder_name"),
    )

    cik: Mapped[str] = mapped_column(Text, primary_key=True)
    holder_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    normalized_holder_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    holder_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_passive_like: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"), nullable=False)
    quality_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    latest_filing_date: Mapped[Optional[date]]
    latest_report_year: Mapped[Optional[int]]
    latest_report_quarter: Mapped[Optional[int]]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class InstitutionalFiling(Base):
    __tablename__ = "institutional_filings"
    __table_args__ = (
        UniqueConstraint("accession_number", name="uq_institutional_filings_accession"),
        UniqueConstraint(
            "cik",
            "report_year",
            "report_quarter",
            "filing_date",
            "form_type",
            name="uq_institutional_filings_period",
        ),
        Index("ix_institutional_filings_cik", "cik"),
        Index("ix_institutional_filings_period", "report_year", "report_quarter"),
        Index("ix_institutional_filings_filing_date", "filing_date"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    cik: Mapped[str] = mapped_column(Text, nullable=False)
    accession_number: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    filing_date: Mapped[date]
    report_year: Mapped[int]
    report_quarter: Mapped[int]
    report_period_end: Mapped[Optional[date]]
    filing_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    form_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_amendment: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"), nullable=False)
    superseded_by: Mapped[Optional[int]]
    raw_metadata_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class InstitutionalPosition(Base):
    __tablename__ = "institutional_positions"
    __table_args__ = (
        UniqueConstraint("filing_id", "normalized_symbol", "cusip", "put_call", name="uq_institutional_positions_filing_security"),
        Index("ix_institutional_positions_symbol", "normalized_symbol"),
        Index("ix_institutional_positions_cik", "cik"),
        Index("ix_institutional_positions_period", "report_year", "report_quarter"),
        Index("ix_institutional_positions_filing_date", "filing_date"),
        Index("ix_institutional_positions_cusip", "cusip"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    filing_id: Mapped[int]
    cik: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    normalized_symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    cusip: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    issuer_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    shares: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    value_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    put_call: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    investment_discretion: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    voting_authority: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    portfolio_weight: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ownership_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    report_year: Mapped[int]
    report_quarter: Mapped[int]
    filing_date: Mapped[date]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class InstitutionalPositionChange(Base):
    __tablename__ = "institutional_position_changes"
    __table_args__ = (
        UniqueConstraint(
            "cik",
            "normalized_symbol",
            "cusip",
            "report_year",
            "report_quarter",
            "change_type",
            name="uq_institutional_position_changes_period",
        ),
        Index("ix_institutional_position_changes_symbol", "normalized_symbol"),
        Index("ix_institutional_position_changes_cik", "cik"),
        Index("ix_institutional_position_changes_period", "report_year", "report_quarter"),
        Index("ix_institutional_position_changes_filing_date", "filing_date"),
        Index("ix_institutional_position_changes_materiality", "materiality_score"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    cik: Mapped[str] = mapped_column(Text, nullable=False)
    holder_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    normalized_symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    cusip: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    report_year: Mapped[int]
    report_quarter: Mapped[int]
    filing_date: Mapped[date]
    prev_shares: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    curr_shares: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    shares_delta: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    shares_delta_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    prev_value_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    curr_value_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    value_delta_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    value_delta_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    prev_portfolio_weight: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    curr_portfolio_weight: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    portfolio_weight_delta: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    prev_ownership_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    curr_ownership_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ownership_pct_delta: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    change_type: Mapped[str] = mapped_column(Text, nullable=False)
    direction: Mapped[str] = mapped_column(Text, default="neutral", server_default="neutral", nullable=False)
    materiality_score: Mapped[float] = mapped_column(Float, default=0.0, server_default=text("0.0"), nullable=False)
    holder_quality_weight: Mapped[float] = mapped_column(Float, default=1.0, server_default=text("1.0"), nullable=False)
    passive_adjusted_score: Mapped[float] = mapped_column(Float, default=0.0, server_default=text("0.0"), nullable=False)
    is_material: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class InstitutionalSymbolSummary(Base):
    __tablename__ = "institutional_symbol_summary"
    __table_args__ = (
        UniqueConstraint("normalized_symbol", "report_year", "report_quarter", name="uq_institutional_symbol_summary_period"),
        Index("ix_institutional_symbol_summary_symbol", "normalized_symbol"),
        Index("ix_institutional_symbol_summary_period", "report_year", "report_quarter"),
        Index("ix_institutional_symbol_summary_latest_filing", "latest_filing_date"),
        Index("ix_institutional_symbol_summary_materiality", "materiality_score"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    report_year: Mapped[int]
    report_quarter: Mapped[int]
    latest_filing_date: Mapped[Optional[date]]
    total_holders: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    holders_increased: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    holders_reduced: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    new_positions: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    exits: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    unchanged_holders: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    total_value_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    net_value_delta_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    net_shares_delta: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    institutional_ownership_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    put_call_ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    accumulation_score: Mapped[float] = mapped_column(Float, default=0.0, server_default=text("0.0"), nullable=False)
    distribution_score: Mapped[float] = mapped_column(Float, default=0.0, server_default=text("0.0"), nullable=False)
    direction: Mapped[str] = mapped_column(Text, default="neutral", server_default="neutral", nullable=False)
    materiality_score: Mapped[float] = mapped_column(Float, default=0.0, server_default=text("0.0"), nullable=False)
    top_accumulators_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    top_reducers_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class InstitutionalActivityEvent(Base):
    __tablename__ = "institutional_activity_events"
    __table_args__ = (
        UniqueConstraint(
            "normalized_symbol",
            "cik",
            "event_type",
            "report_year",
            "report_quarter",
            name="uq_institutional_activity_events_period",
        ),
        Index("ix_institutional_activity_events_symbol", "normalized_symbol"),
        Index("ix_institutional_activity_events_filing_date", "filing_date"),
        Index("ix_institutional_activity_events_materiality", "materiality_score"),
        Index("ix_institutional_activity_events_type", "event_type"),
        Index("ix_institutional_activity_events_direction", "direction"),
        Index("ix_institutional_activity_events_feed_visible", "feed_visible"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    cik: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    holder_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    direction: Mapped[str] = mapped_column(Text, default="neutral", server_default="neutral", nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    filing_date: Mapped[date]
    report_year: Mapped[int]
    report_quarter: Mapped[int]
    reported_value_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    value_delta_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ownership_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    holder_breadth: Mapped[Optional[int]]
    materiality_score: Mapped[float] = mapped_column(Float, default=0.0, server_default=text("0.0"), nullable=False)
    confirmation_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    freshness_status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_label: Mapped[str] = mapped_column(Text, default="Institutional Activity", server_default="Institutional Activity", nullable=False)
    pro_required: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"), nullable=False)
    feed_visible: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"), nullable=False)
    metadata_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class InstitutionalIndustrySummary(Base):
    __tablename__ = "institutional_industry_summary"
    __table_args__ = (
        UniqueConstraint("industry", "sector", "report_year", "report_quarter", name="uq_institutional_industry_summary_period"),
        Index("ix_institutional_industry_summary_industry", "industry"),
        Index("ix_institutional_industry_summary_period", "report_year", "report_quarter"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    industry: Mapped[str] = mapped_column(Text, nullable=False)
    sector: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    report_year: Mapped[int]
    report_quarter: Mapped[int]
    total_value_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    net_value_delta_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    accumulation_score: Mapped[float] = mapped_column(Float, default=0.0, server_default=text("0.0"), nullable=False)
    distribution_score: Mapped[float] = mapped_column(Float, default=0.0, server_default=text("0.0"), nullable=False)
    direction: Mapped[str] = mapped_column(Text, default="neutral", server_default="neutral", nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class InstitutionalHolderIndustryBreakdown(Base):
    __tablename__ = "institutional_holder_industry_breakdown"
    __table_args__ = (
        UniqueConstraint("cik", "report_year", "report_quarter", "industry", "sector", name="uq_institutional_holder_industry_period"),
        Index("ix_institutional_holder_industry_cik", "cik"),
        Index("ix_institutional_holder_industry_period", "report_year", "report_quarter"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    cik: Mapped[str] = mapped_column(Text, nullable=False)
    report_year: Mapped[int]
    report_quarter: Mapped[int]
    industry: Mapped[str] = mapped_column(Text, nullable=False)
    sector: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    value_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    weight_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Watchlist(Base):
    __tablename__ = "watchlists"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True)
    owner_user_id: Mapped[Optional[int]]


class WatchlistItem(Base):
    __tablename__ = "watchlist_items"
    id: Mapped[int] = mapped_column(primary_key=True)
    watchlist_id: Mapped[int]
    security_id: Mapped[int]


class WatchlistViewState(Base):
    __tablename__ = "watchlist_view_states"
    watchlist_id: Mapped[int] = mapped_column(primary_key=True)
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class InstitutionalIngestJobState(Base):
    __tablename__ = "institutional_ingest_job_state"
    __table_args__ = (Index("ix_institutional_ingest_job_state_status", "last_status"),)

    job_name: Mapped[str] = mapped_column(Text, primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"), nullable=False)
    cursor_page: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    limit: Mapped[int] = mapped_column(default=25, server_default=text("25"), nullable=False)
    pages_per_run: Mapped[int] = mapped_column(default=2, server_default=text("2"), nullable=False)
    max_filings_per_run: Mapped[int] = mapped_column(default=25, server_default=text("25"), nullable=False)
    first_empty_page: Mapped[Optional[int]]
    last_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_status: Mapped[str] = mapped_column(Text, default="idle", server_default="idle", nullable=False)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    total_pages_scanned: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    total_filings_processed: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    total_position_rows: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    total_activity_events: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    total_feed_events: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class InstitutionalIngestJobRun(Base):
    __tablename__ = "institutional_ingest_job_runs"
    __table_args__ = (
        Index("ix_institutional_ingest_job_runs_job_started", "job_name", "started_at"),
        Index("ix_institutional_ingest_job_runs_status", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    job_name: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    start_page: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    pages_requested: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    pages_scanned: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    limit: Mapped[int] = mapped_column(default=25, server_default=text("25"), nullable=False)
    max_filings: Mapped[int] = mapped_column(default=25, server_default=text("25"), nullable=False)
    scanned: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    parsed: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    parse_failed: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    already_processed_skipped: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    processed_filings: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    empty_extract_retryable: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    empty_extract_processed_no_holdings: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    skipped: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    errors: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    position_rows: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    position_changes: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    summaries: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    activity_events: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    feed_events: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    first_empty_page_seen: Mapped[Optional[int]]
    next_cursor_page: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class ConfirmationMonitoringSnapshot(Base):
    __tablename__ = "confirmation_monitoring_snapshots"
    __table_args__ = (
        Index(
            "ix_confirmation_monitoring_snapshot_scope",
            "user_id",
            "watchlist_id",
            "ticker",
            unique=True,
        ),
        Index("ix_confirmation_monitoring_snapshot_observed", "observed_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int]
    watchlist_id: Mapped[int]
    ticker: Mapped[str] = mapped_column(Text)
    score: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    band: Mapped[str] = mapped_column(Text, default="inactive", server_default="inactive")
    direction: Mapped[str] = mapped_column(Text, default="neutral", server_default="neutral")
    source_count: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    status: Mapped[str] = mapped_column(Text, default="Inactive", server_default="Inactive")
    source_states_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class ConfirmationMonitoringEvent(Base):
    __tablename__ = "confirmation_monitoring_events"
    __table_args__ = (
        Index("ix_confirmation_monitoring_events_watchlist_created", "user_id", "watchlist_id", "created_at"),
        Index(
            "ix_confirmation_monitoring_events_dedupe",
            "user_id",
            "watchlist_id",
            "ticker",
            "event_type",
            "created_at",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int]
    watchlist_id: Mapped[int]
    ticker: Mapped[str] = mapped_column(Text)
    event_type: Mapped[str] = mapped_column(Text)
    title: Mapped[str] = mapped_column(Text)
    body: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    score_before: Mapped[Optional[int]]
    score_after: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    band_before: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    band_after: Mapped[str] = mapped_column(Text)
    direction_before: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    direction_after: Mapped[str] = mapped_column(Text)
    source_count_before: Mapped[Optional[int]]
    source_count_after: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    payload_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class UserAccount(Base):
    __tablename__ = "user_accounts"
    __table_args__ = (
        Index("ix_user_accounts_email", "email", unique=True),
        Index("ix_user_accounts_stripe_customer", "stripe_customer_id", unique=True),
        Index("ix_user_accounts_stripe_subscription", "stripe_subscription_id", unique=True),
        Index("ix_user_accounts_created_at", "created_at"),
        Index("ix_user_accounts_last_seen_at", "last_seen_at"),
        Index("ix_user_accounts_subscription_status", "subscription_status"),
        Index("ix_user_accounts_deleted_at", "deleted_at"),
        Index("ix_user_accounts_reactivation_token", "reactivation_token_hash", unique=True),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(Text, nullable=False)
    original_email: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    country: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    state_province: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    postal_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    city: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    address_line1: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    address_line2: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    auth_provider: Mapped[str] = mapped_column(Text, default="email", server_default="email")
    google_sub: Mapped[Optional[str]] = mapped_column(Text, nullable=True, unique=True)
    avatar_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    password_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    password_reset_token_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    password_reset_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    email_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    email_verification_token_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    email_verification_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    reactivation_token_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reactivation_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    role: Mapped[str] = mapped_column(Text, default="user", server_default="user")
    entitlement_tier: Mapped[str] = mapped_column(Text, default="free", server_default="free")
    manual_tier_override: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    monthly_price_override: Mapped[Optional[int]]
    annual_price_override: Mapped[Optional[int]]
    override_currency: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    override_note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_suspended: Mapped[bool] = mapped_column(default=False, server_default=text("false"))
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stripe_price_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    current_plan_amount_cents: Mapped[Optional[int]]
    current_plan_currency: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    subscription_status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    subscription_plan: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    subscription_interval: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    subscription_cancel_at_period_end: Mapped[bool] = mapped_column(default=False, server_default=text("false"))
    access_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    deleted_by_user: Mapped[bool] = mapped_column(default=False, server_default=text("false"))
    deletion_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    deletion_plan: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    alerts_enabled: Mapped[bool] = mapped_column(default=True, server_default=text("true"))
    email_notifications_enabled: Mapped[bool] = mapped_column(default=True, server_default=text("true"))
    watchlist_activity_notifications: Mapped[bool] = mapped_column(default=True, server_default=text("true"))
    signals_notifications: Mapped[bool] = mapped_column(default=True, server_default=text("true"))
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class AppSetting(Base):
    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class FeatureGate(Base):
    __tablename__ = "feature_gates"

    feature_key: Mapped[str] = mapped_column(Text, primary_key=True)
    required_tier: Mapped[str] = mapped_column(Text, default="premium", server_default="premium")
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class PlanLimit(Base):
    __tablename__ = "plan_limits"

    tier: Mapped[str] = mapped_column(Text, primary_key=True)
    feature_key: Mapped[str] = mapped_column(Text, primary_key=True)
    limit_value: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class PlanPrice(Base):
    __tablename__ = "plan_prices"

    tier: Mapped[str] = mapped_column(Text, primary_key=True)
    billing_interval: Mapped[str] = mapped_column(Text, primary_key=True)
    amount_cents: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    currency: Mapped[str] = mapped_column(Text, default="USD", server_default="USD")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class StripeWebhookEvent(Base):
    __tablename__ = "stripe_webhook_events"

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    event_type: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="processed", server_default="processed")
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class AdminBillingOverrideAuditLog(Base):
    __tablename__ = "admin_billing_override_audit_log"
    __table_args__ = (
        Index("ix_admin_billing_override_target_created", "target_user_id", "created_at"),
        Index("ix_admin_billing_override_admin_created", "admin_user_id", "created_at"),
        Index("ix_admin_billing_override_status_created", "stripe_sync_status", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    admin_user_id: Mapped[Optional[int]]
    admin_email: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    target_user_id: Mapped[int]
    target_email: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    override_type: Mapped[str] = mapped_column(Text)
    previous_state_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    requested_state_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stripe_sync_status: Mapped[str] = mapped_column(Text, default="pending", server_default="pending")
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class EmailTemplate(Base):
    __tablename__ = "email_templates"
    __table_args__ = (
        Index("ix_email_templates_template_key", "template_key", unique=True),
        Index("ix_email_templates_category", "category"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    template_key: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    from_name: Mapped[str] = mapped_column(Text, nullable=False)
    from_email: Mapped[str] = mapped_column(Text, nullable=False)
    reply_to: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    subject: Mapped[str] = mapped_column(Text, nullable=False)
    preheader: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    body_text: Mapped[str] = mapped_column(Text, nullable=False)
    body_html: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    variables_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]")
    enabled: Mapped[bool] = mapped_column(default=True, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class EmailDelivery(Base):
    __tablename__ = "email_deliveries"
    __table_args__ = (
        Index("ix_email_deliveries_user_created", "user_id", "created_at"),
        Index("ix_email_deliveries_status", "status"),
        Index("ix_email_deliveries_template_key", "template_key"),
        Index("ix_email_deliveries_idempotency_key", "idempotency_key", unique=True),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[Optional[int]]
    to_email: Mapped[str] = mapped_column(Text, nullable=False)
    from_email: Mapped[str] = mapped_column(Text, nullable=False)
    template_key: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    subject: Mapped[str] = mapped_column(Text, nullable=False)
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    provider_message_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, default="queued", server_default="queued")
    idempotency_key: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class AiMarketingCampaign(Base):
    __tablename__ = "ai_marketing_campaigns"
    __table_args__ = (
        Index("ix_ai_marketing_campaigns_enabled", "enabled"),
        Index("ix_ai_marketing_campaigns_mode", "mode"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(default=True, server_default=text("true"), nullable=False)
    mode: Mapped[str] = mapped_column(Text, nullable=False)
    campaign_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    content_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, default="active", server_default="active", nullable=False)
    schedule_config_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}", nullable=False)
    weekdays_only: Mapped[bool] = mapped_column(default=True, server_default=text("true"), nullable=False)
    run_time: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    timezone: Mapped[str] = mapped_column(Text, default="America/Los_Angeles", server_default="America/Los_Angeles", nullable=False)
    recipient_email: Mapped[str] = mapped_column(Text, default="jarod@walnutmarkets.com", server_default="jarod@walnutmarkets.com", nullable=False)
    source_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_reference_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    filters_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}", nullable=False)
    output_preferences_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}", nullable=False)
    created_by: Mapped[Optional[int]]
    updated_by: Mapped[Optional[int]]
    last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    next_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    platforms_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    keywords_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    tickers_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    subreddits_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    query_templates_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    minimum_relevance_score: Mapped[int] = mapped_column(default=60, server_default=text("60"), nullable=False)
    max_items_per_run: Mapped[int] = mapped_column(default=10, server_default=text("10"), nullable=False)
    recency: Mapped[str] = mapped_column(Text, default="week", server_default="week", nullable=False)
    default_destination_page: Mapped[str] = mapped_column(Text, default="https://walnutmarkets.com", server_default="https://walnutmarkets.com", nullable=False)
    include_disclosure: Mapped[bool] = mapped_column(default=True, server_default=text("true"), nullable=False)
    scheduled_digest_enabled: Mapped[bool] = mapped_column(default=False, server_default=text("false"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class AiMarketingSetting(Base):
    __tablename__ = "ai_marketing_settings"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_secret: Mapped[bool] = mapped_column(default=False, server_default=text("false"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class AiMarketingOpportunity(Base):
    __tablename__ = "ai_marketing_opportunities"
    __table_args__ = (
        UniqueConstraint("platform", "source_dedupe_key", name="uq_ai_marketing_opportunities_source"),
        Index("ix_ai_marketing_opportunities_campaign_status", "campaign_id", "status"),
        Index("ix_ai_marketing_opportunities_status_created", "status", "created_at"),
        Index("ix_ai_marketing_opportunities_platform_created", "platform", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[Optional[int]]
    platform: Mapped[str] = mapped_column(Text, nullable=False)
    source_provider: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    source_dedupe_key: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    excerpt: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    author: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    community: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_score: Mapped[Optional[int]]
    comment_count: Mapped[Optional[int]]
    source_created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, default="new", server_default="new", nullable=False)
    campaign_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    content_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_platform: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ticker_theme: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    recommended_action: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    matched_keywords_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    matched_tickers_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    fit_score: Mapped[Optional[int]]
    relevance_score: Mapped[Optional[int]]
    spam_risk_score: Mapped[Optional[int]]
    intent: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    suggested_destination_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    short_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    compliance_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    generated_content: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    full_markdown: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    alternate_versions_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}", nullable=False)
    quality_scores_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}", nullable=False)
    source_notes_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    missing_data_notes_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    asset_refs_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    raw_metadata_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    emailed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    opened_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    copied_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    posted_manually_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class AiMarketingSuggestion(Base):
    __tablename__ = "ai_marketing_suggestions"
    __table_args__ = (
        Index("ix_ai_marketing_suggestions_opportunity_created", "opportunity_id", "created_at"),
        Index("ix_ai_marketing_suggestions_campaign_created", "campaign_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    opportunity_id: Mapped[int]
    campaign_id: Mapped[Optional[int]]
    model: Mapped[str] = mapped_column(Text, nullable=False)
    relevance_score: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    spam_risk_score: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    detected_tickers_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    intent: Mapped[str] = mapped_column(Text, default="other", server_default="other", nullable=False)
    campaign_type: Mapped[str] = mapped_column(Text, default="legacy_outreach_campaign", server_default="legacy_outreach_campaign", nullable=False)
    content_type: Mapped[str] = mapped_column(Text, default="reddit_reply", server_default="reddit_reply", nullable=False)
    platform: Mapped[str] = mapped_column(Text, default="reddit", server_default="reddit", nullable=False)
    audience: Mapped[str] = mapped_column(Text, default="", server_default="", nullable=False)
    recommended_action: Mapped[str] = mapped_column(Text, default="reply", server_default="reply", nullable=False)
    reply_angle: Mapped[str] = mapped_column(Text, default="other", server_default="other", nullable=False)
    content_angle: Mapped[str] = mapped_column(Text, default="", server_default="", nullable=False)
    value_added_insight: Mapped[str] = mapped_column(Text, default="", server_default="", nullable=False)
    walnut_feature_to_mention: Mapped[str] = mapped_column(Text, default="", server_default="", nullable=False)
    suggested_destination_url: Mapped[str] = mapped_column(Text, nullable=False)
    suggested_reply: Mapped[str] = mapped_column(Text, nullable=False)
    suggested_post: Mapped[str] = mapped_column(Text, default="", server_default="", nullable=False)
    suggested_ad_variants_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    influencer_outreach_draft: Mapped[str] = mapped_column(Text, default="", server_default="", nullable=False)
    report_pack_outline: Mapped[str] = mapped_column(Text, default="", server_default="", nullable=False)
    alternate_hooks_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    title_options_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    disclosure_text: Mapped[str] = mapped_column(Text, default="", server_default="", nullable=False)
    assets_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    alternate_reply_more_direct: Mapped[str] = mapped_column(Text, default="", server_default="", nullable=False)
    short_reason: Mapped[str] = mapped_column(Text, nullable=False)
    compliance_notes: Mapped[str] = mapped_column(Text, nullable=False)
    prompt_version: Mapped[str] = mapped_column(Text, default="ai_marketing_v2", server_default="ai_marketing_v2", nullable=False)
    raw_response_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class AiMarketingEmailLog(Base):
    __tablename__ = "ai_marketing_email_logs"
    __table_args__ = (
        Index("ix_ai_marketing_email_logs_created", "created_at"),
        Index("ix_ai_marketing_email_logs_status", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    delivery_id: Mapped[Optional[int]]
    to_email: Mapped[str] = mapped_column(Text, nullable=False)
    subject: Mapped[str] = mapped_column(Text, nullable=False)
    opportunity_ids_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    status: Mapped[str] = mapped_column(Text, default="queued", server_default="queued", nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class BillingTransaction(Base):
    __tablename__ = "billing_transactions"
    __table_args__ = (
        Index("ix_billing_transactions_user_charged", "user_id", "charged_at"),
        Index("ix_billing_transactions_customer", "stripe_customer_id"),
        Index("ix_billing_transactions_subscription", "stripe_subscription_id"),
        Index("ix_billing_transactions_invoice", "stripe_invoice_id", unique=True),
        Index("ix_billing_transactions_charged_at", "charged_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stripe_invoice_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stripe_payment_intent_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stripe_charge_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    user_id: Mapped[Optional[int]]
    customer_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    customer_email: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    billing_country: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    billing_state_province: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    billing_postal_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    billing_period_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    service_period_start: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    service_period_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    subtotal_amount: Mapped[Optional[int]]
    tax_amount: Mapped[Optional[int]]
    total_amount: Mapped[Optional[int]]
    currency: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    charged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    payment_status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    access_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    refund_status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tax_breakdown_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class PageViewEvent(Base):
    __tablename__ = "page_view_events"
    __table_args__ = (
        Index("ix_page_view_events_created_at", "created_at"),
        Index("ix_page_view_events_normalized_created", "normalized_path", "created_at"),
        Index("ix_page_view_events_user_created", "user_id", "created_at"),
        Index("ix_page_view_events_session_created", "session_id_hash", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[Optional[int]]
    session_id_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    path: Mapped[str] = mapped_column(Text)
    normalized_path: Mapped[str] = mapped_column(Text)
    route_group: Mapped[str] = mapped_column(Text)
    referrer_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    user_agent_family: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    device_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_authenticated: Mapped[bool] = mapped_column(default=False, server_default=text("false"))
    plan_at_time: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class NotificationSubscription(Base):
    __tablename__ = "notification_subscriptions"
    __table_args__ = (
        Index("ix_notification_subscriptions_source", "source_type", "source_id"),
        Index("ix_notification_subscriptions_active_frequency", "active", "frequency"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(Text)
    source_type: Mapped[str] = mapped_column(Text)
    source_id: Mapped[str] = mapped_column(Text)
    source_name: Mapped[str] = mapped_column(Text)
    source_payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    frequency: Mapped[str] = mapped_column(Text, default="daily", server_default="daily")
    only_if_new: Mapped[bool] = mapped_column(default=True, server_default=text("true"))
    active: Mapped[bool] = mapped_column(default=True, server_default=text("true"))
    alert_triggers_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]")
    min_smart_score: Mapped[Optional[int]]
    large_trade_amount: Mapped[Optional[int]]
    last_delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class GovernmentContract(Base):
    __tablename__ = "government_contracts"
    __table_args__ = (
        Index("ix_government_contracts_symbol", "symbol"),
        Index("ix_government_contracts_symbol_award_date", "symbol", "award_date"),
        Index("ix_government_contracts_award_date", "award_date"),
        Index("ix_government_contracts_award_amount", "award_amount"),
        Index("ix_government_contracts_awarding_agency", "awarding_agency"),
        Index("ix_government_contracts_source_award_id", "source", "award_id", unique=True),
        Index("ix_government_contracts_source_dedupe_key", "source", "dedupe_key", unique=True),
        Index("ix_government_contracts_event_id", "event_id", unique=True),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[Optional[int]]
    award_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    dedupe_key: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    symbol: Mapped[str] = mapped_column(Text)
    recipient_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_recipient_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    award_date: Mapped[date]
    award_amount: Mapped[float]
    awarding_agency: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    awarding_sub_agency: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    funding_agency: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    funding_sub_agency: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    period_start: Mapped[Optional[date]]
    period_end: Mapped[Optional[date]]
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    contract_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source: Mapped[str] = mapped_column(Text, default="usaspending", server_default="usaspending")
    mapping_method: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    mapping_confidence: Mapped[Optional[float]]
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class GovernmentContractAction(Base):
    __tablename__ = "government_contract_actions"
    __table_args__ = (
        Index("ix_government_contract_actions_symbol", "symbol"),
        Index("ix_government_contract_actions_action_date", "action_date"),
        Index("ix_government_contract_actions_obligated_amount", "obligated_amount"),
        Index("ix_government_contract_actions_parent_award_id", "parent_award_id"),
        Index("ix_government_contract_actions_event_id", "event_id", unique=True),
        UniqueConstraint(
            "source",
            "parent_award_id",
            "modification_number",
            name="uq_government_contract_actions_modification",
        ),
        UniqueConstraint(
            "source",
            "parent_award_id",
            "dedupe_key",
            name="uq_government_contract_actions_dedupe",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[Optional[int]]
    parent_award_id: Mapped[str] = mapped_column(Text)
    modification_number: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    dedupe_key: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    recipient_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    company_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    awarding_agency: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    awarding_sub_agency: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    action_date: Mapped[date]
    obligated_amount: Mapped[float]
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    action_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source: Mapped[str] = mapped_column(Text, default="usaspending", server_default="usaspending")
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class SavedScreen(Base):
    __tablename__ = "saved_screens"
    __table_args__ = (
        Index("ix_saved_screens_user_updated", "user_id", "updated_at"),
        Index("ix_saved_screens_user_refreshed", "user_id", "last_refreshed_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int]
    name: Mapped[str] = mapped_column(Text)
    params_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    last_viewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_refreshed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class SavedScreenSnapshot(Base):
    __tablename__ = "saved_screen_snapshots"
    __table_args__ = (
        Index("ix_saved_screen_snapshots_screen_observed", "saved_screen_id", "observed_at"),
        Index("ix_saved_screen_snapshots_scope", "user_id", "saved_screen_id", "ticker", unique=True),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int]
    saved_screen_id: Mapped[int]
    ticker: Mapped[str] = mapped_column(Text)
    confirmation_score: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    confirmation_band: Mapped[str] = mapped_column(Text, default="inactive", server_default="inactive")
    direction: Mapped[str] = mapped_column(Text, default="neutral", server_default="neutral")
    source_count: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    why_now_state: Mapped[str] = mapped_column(Text, default="inactive", server_default="inactive")
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class SavedScreenEvent(Base):
    __tablename__ = "saved_screen_events"
    __table_args__ = (
        Index("ix_saved_screen_events_user_created", "user_id", "created_at"),
        Index("ix_saved_screen_events_screen_created", "saved_screen_id", "created_at"),
        Index("ix_saved_screen_events_dedupe", "user_id", "saved_screen_id", "ticker", "event_type", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int]
    saved_screen_id: Mapped[int]
    ticker: Mapped[str] = mapped_column(Text)
    event_type: Mapped[str] = mapped_column(Text)
    title: Mapped[str] = mapped_column(Text)
    description: Mapped[str] = mapped_column(Text)
    before_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    after_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class MonitoringAlert(Base):
    __tablename__ = "monitoring_alerts"
    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "source_type",
            "source_id",
            "event_id",
            name="uq_monitoring_alert_source_event",
        ),
        Index("ix_monitoring_alerts_user_read", "user_id", "read_at", "created_at"),
        Index("ix_monitoring_alerts_source_read", "user_id", "source_type", "source_id", "read_at"),
        Index("ix_monitoring_alerts_user_dismissed", "user_id", "dismissed_at", "created_at"),
        Index("ix_monitoring_alerts_event_created", "event_created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int]
    source_type: Mapped[str] = mapped_column(Text)
    source_id: Mapped[str] = mapped_column(Text)
    source_name: Mapped[str] = mapped_column(Text)
    event_id: Mapped[int]
    alert_type: Mapped[str] = mapped_column(Text)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    title: Mapped[str] = mapped_column(Text)
    body: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    payload_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}")
    event_created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    read_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    dismissed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class NotificationDelivery(Base):
    __tablename__ = "notification_deliveries"
    __table_args__ = (
        Index("ix_notification_deliveries_subscription_created", "subscription_id", "created_at"),
        Index("ix_notification_deliveries_status", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    subscription_id: Mapped[int]
    channel: Mapped[str] = mapped_column(Text, default="email", server_default="email")
    status: Mapped[str] = mapped_column(Text)
    subject: Mapped[str] = mapped_column(Text)
    body_text: Mapped[str] = mapped_column(Text)
    items_count: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    alerts_count: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


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
    amount_min: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    amount_max: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    data_source: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_provider: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_filing_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_document_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    parser_version: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    provider_priority: Mapped[Optional[int]] = mapped_column(nullable=True)
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
    volume: Mapped[Optional[float]] = mapped_column(nullable=True)
    day_volume: Mapped[Optional[float]] = mapped_column(nullable=True)
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
    sector: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    industry: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    country: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )


class FundamentalsCache(Base):
    __tablename__ = "fundamentals_cache"
    __table_args__ = (
        UniqueConstraint("symbol", "provider", name="uq_fundamentals_cache_symbol_provider"),
        Index("ix_fundamentals_cache_symbol", "symbol"),
        Index("ix_fundamentals_cache_provider_fetched", "provider", "fetched_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    provider: Mapped[str] = mapped_column(Text, default="fmp", server_default="fmp", nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    period_date: Mapped[Optional[date]] = mapped_column(nullable=True)
    status: Mapped[str] = mapped_column(Text, default="ok", server_default="ok", nullable=False)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    company_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sector: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    industry: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    country: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    exchange: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    market_cap: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    avg_volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    beta: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    dividend_yield: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    trailing_pe: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    forward_pe: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    price_to_sales: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ev_to_ebitda: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    gross_margin: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    operating_margin: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    operating_margin_expansion: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    net_margin: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    roe: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    roic: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    revenue_growth: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    eps_growth: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ebitda_growth: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    free_cash_flow: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fcf_yield: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fcf_margin: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fcf_growth: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    debt_to_equity: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    current_ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    net_debt_to_ebitda: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    eps_ttm: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    earnings_yield: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class TickerFinancialsCache(Base):
    __tablename__ = "ticker_financials_cache"
    __table_args__ = (
        Index("ix_ticker_financials_cache_fetched_at", "fetched_at"),
    )

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class TickerContentCache(Base):
    __tablename__ = "ticker_content_cache"
    __table_args__ = (
        UniqueConstraint("content_type", "symbol", "window_key", name="uq_ticker_content_cache_type_symbol_window"),
        Index("ix_ticker_content_cache_symbol_type", "symbol", "content_type"),
        Index("ix_ticker_content_cache_fetched_at", "fetched_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    content_type: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    window_key: Mapped[str] = mapped_column(Text, default="latest", server_default="latest", nullable=False)
    cache_key: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    item_count: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, default="fmp", server_default="fmp", nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class TickerContextBundleCache(Base):
    __tablename__ = "ticker_context_bundle_cache"
    __table_args__ = (
        Index("ix_ticker_context_bundle_symbol", "symbol"),
        Index("ix_ticker_context_bundle_expires_at", "expires_at"),
    )

    cache_key: Mapped[str] = mapped_column(Text, primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    user_segment: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    stale_after: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class InsightsSnapshot(Base):
    __tablename__ = "insights_snapshots"
    __table_args__ = (
        Index("ix_insights_snapshots_kind", "kind", unique=True),
        Index("ix_insights_snapshots_fetched_at", "fetched_at"),
    )

    kind: Mapped[str] = mapped_column(Text, primary_key=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, default="fmp", server_default="fmp", nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class FredObservation(Base):
    __tablename__ = "fred_observations"
    __table_args__ = (
        Index("ix_fred_observations_series_date", "series_id", "observation_date"),
        Index("ix_fred_observations_fetched_at", "fetched_at"),
    )

    series_id: Mapped[str] = mapped_column(Text, primary_key=True)
    observation_date: Mapped[date] = mapped_column(primary_key=True)
    value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(Text, default="fred", server_default="fred", nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}", nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class FredSeriesRefresh(Base):
    __tablename__ = "fred_series_refreshes"
    __table_args__ = (
        Index("ix_fred_series_refreshes_refreshed_at", "last_refreshed_at"),
    )

    series_id: Mapped[str] = mapped_column(Text, primary_key=True)
    source: Mapped[str] = mapped_column(Text, default="fred", server_default="fred", nullable=False)
    status: Mapped[str] = mapped_column(Text, default="pending", server_default="pending", nullable=False)
    observation_count: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    latest_observation_date: Mapped[Optional[date]]
    last_refreshed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
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
    amount_min: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    amount_max: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    scoring_status: Mapped[str] = mapped_column(Text, default="ok", server_default="ok")
    scoring_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    methodology_version: Mapped[str] = mapped_column(Text, default="congress_v1", server_default="congress_v1")
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class ReplicatedPortfolioRun(Base):
    __tablename__ = "replicated_portfolio_runs"
    __table_args__ = (
        Index(
            "ix_replicated_portfolio_runs_lookup",
            "entity_type",
            "entity_id",
            "issuer_cik",
            "issuer_symbol",
            "mode",
            "lookback_days",
            "benchmark_symbol",
            "computed_at",
        ),
        Index("ix_replicated_portfolio_runs_entity", "entity_type", "entity_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    entity_type: Mapped[str] = mapped_column(Text)
    entity_id: Mapped[str] = mapped_column(Text)
    issuer_cik: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    issuer_symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    mode: Mapped[str] = mapped_column(Text)
    lookback_days: Mapped[int]
    benchmark_symbol: Mapped[str] = mapped_column(Text, default="^GSPC", server_default="^GSPC")
    start_date: Mapped[date]
    end_date: Mapped[date]
    starting_value: Mapped[float] = mapped_column(Float, default=100000.0, server_default=text("100000.0"))
    ending_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    benchmark_ending_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    total_return_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    benchmark_return_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    alpha_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    cagr_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    max_drawdown_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    volatility_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    sharpe_ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    win_rate_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    average_exposure_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ending_cash_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    points_count: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    positions_count: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    skipped_events_count: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    status: Mapped[str] = mapped_column(Text, default="ok", server_default="ok")
    status_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    methodology_version: Mapped[str] = mapped_column(Text, default="replicated_portfolio_v4", server_default="replicated_portfolio_v4")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class ReplicatedPortfolioPoint(Base):
    __tablename__ = "replicated_portfolio_points"
    __table_args__ = (
        UniqueConstraint("run_id", "asof_date", name="uq_replicated_portfolio_points_run_date"),
        Index("ix_replicated_portfolio_points_run_date", "run_id", "asof_date"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(index=True)
    asof_date: Mapped[date]
    strategy_value: Mapped[float] = mapped_column(Float)
    benchmark_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    strategy_return_pct: Mapped[float] = mapped_column(Float)
    benchmark_return_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    alpha_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    daily_return_pct: Mapped[float] = mapped_column(Float, default=0.0, server_default=text("0.0"))
    active_positions: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    exposure_pct: Mapped[float] = mapped_column(Float, default=0.0, server_default=text("0.0"))
    cash_pct: Mapped[float] = mapped_column(Float, default=100.0, server_default=text("100.0"))


class ReplicatedPortfolioPosition(Base):
    __tablename__ = "replicated_portfolio_positions"
    __table_args__ = (
        Index("ix_replicated_portfolio_positions_run_symbol", "run_id", "symbol"),
        Index("ix_replicated_portfolio_positions_event", "source_event_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(index=True)
    source_event_id: Mapped[Optional[int]] = mapped_column(nullable=True)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    side: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    entry_date: Mapped[Optional[date]] = mapped_column(nullable=True)
    exit_date: Mapped[Optional[date]] = mapped_column(nullable=True)
    entry_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    exit_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    shares: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    market_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    return_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    amount_min: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    amount_max: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    status: Mapped[str] = mapped_column(Text, default="open", server_default="open")
    skip_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    confidence: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_document_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class ProviderUsageEvent(Base):
    __tablename__ = "provider_usage_events"
    __table_args__ = (
        Index("ix_provider_usage_provider_created", "provider", "created_at"),
        Index("ix_provider_usage_category_created", "category", "created_at"),
        Index("ix_provider_usage_source_created", "source", "created_at"),
        Index("ix_provider_usage_throttled_created", "throttled", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    provider: Mapped[str] = mapped_column(Text, default="fmp", server_default="fmp")
    category: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    endpoint: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    route: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    cache_status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    success: Mapped[bool] = mapped_column(default=True, server_default=text("true"))
    throttled: Mapped[bool] = mapped_column(default=False, server_default=text("false"))
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ProviderSetting(Base):
    __tablename__ = "provider_settings"
    __table_args__ = (
        Index("ix_provider_settings_domain_key", "domain_key", unique=True),
        Index("ix_provider_settings_mode", "mode"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    domain_key: Mapped[str] = mapped_column(Text, nullable=False)
    active_provider: Mapped[str] = mapped_column(Text, nullable=False)
    fallback_provider: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    primary_endpoint_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    fallback_endpoint_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    primary_endpoint_contract_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    fallback_endpoint_contract_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    mode: Mapped[str] = mapped_column(Text, default="primary", server_default="primary", nullable=False)
    is_enabled: Mapped[bool] = mapped_column(default=True, server_default=text("true"), nullable=False)
    allow_external_live_fetch: Mapped[bool] = mapped_column(default=False, server_default=text("false"), nullable=False)
    allow_user_route_sync_fetch: Mapped[bool] = mapped_column(default=False, server_default=text("false"), nullable=False)
    builder_safe_required: Mapped[bool] = mapped_column(default=True, server_default=text("true"), nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class ProviderSettingAuditLog(Base):
    __tablename__ = "provider_setting_audit_log"
    __table_args__ = (
        Index("ix_provider_setting_audit_domain_changed", "domain_key", "changed_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    domain_key: Mapped[str] = mapped_column(Text, nullable=False)
    previous_provider: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    new_provider: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    previous_mode: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    new_mode: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    changed_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class SymbolResolutionOverride(Base):
    __tablename__ = "symbol_resolution_overrides"
    __table_args__ = (
        Index("ix_symbol_resolution_override_lookup", "domain_key", "raw_symbol", "issuer_name", unique=True),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    domain_key: Mapped[str] = mapped_column(Text, nullable=False)
    raw_symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    issuer_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    normalized_symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    asset_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class CongressDisclosureFiling(Base):
    __tablename__ = "congress_disclosure_filings"
    __table_args__ = (
        Index("ix_congress_disclosure_source_filing", "source_provider", "filing_id", unique=True),
        Index("ix_congress_disclosure_chamber_date", "chamber", "filing_date"),
        Index("ix_congress_disclosure_parser_status", "parser_status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source_provider: Mapped[str] = mapped_column(Text, nullable=False)
    chamber: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    filing_id: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    document_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    document_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    member_name_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    member_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    filing_date: Mapped[Optional[date]]
    report_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    amendment_flag: Mapped[bool] = mapped_column(default=False, server_default=text("false"), nullable=False)
    raw_metadata_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}", nullable=False)
    raw_text_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_blob_ref: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    parser_status: Mapped[str] = mapped_column(Text, default="pending", server_default="pending", nullable=False)
    parser_version: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    parser_confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    last_parse_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    parsed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class CongressTransactionNormalized(Base):
    __tablename__ = "congress_transactions_normalized"
    __table_args__ = (
        Index("ix_congress_txn_normalized_hash", "normalized_hash", unique=True),
        Index("ix_congress_txn_symbol_date", "ticker_normalized", "transaction_date"),
        Index("ix_congress_txn_member_date", "member_id", "transaction_date"),
        Index("ix_congress_txn_duplicate", "is_duplicate"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    filing_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    disclosure_filing_id: Mapped[Optional[int]]
    source_provider: Mapped[str] = mapped_column(Text, nullable=False)
    chamber: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    member_name_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    member_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    owner_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    owner_normalized: Mapped[str] = mapped_column(Text, default="unknown", server_default="unknown", nullable=False)
    transaction_date: Mapped[Optional[date]]
    disclosure_date: Mapped[Optional[date]]
    ticker_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ticker_normalized: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    issuer_name_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    security_name_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    asset_type_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    asset_type_normalized: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transaction_type_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transaction_type_normalized: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    amount_range_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    amount_low: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    amount_high: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    document_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_line_ref: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    normalized_hash: Mapped[str] = mapped_column(Text, nullable=False)
    is_duplicate: Mapped[bool] = mapped_column(default=False, server_default=text("false"), nullable=False)
    duplicate_of_id: Mapped[Optional[int]]
    symbol_resolution_status: Mapped[str] = mapped_column(Text, default="unresolved", server_default="unresolved", nullable=False)
    parser_confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class SecForm4Filing(Base):
    __tablename__ = "sec_form4_filings"
    __table_args__ = (
        Index("ix_sec_form4_accession", "accession_number", unique=True),
        Index("ix_sec_form4_issuer_filing_date", "issuer_cik", "filing_date"),
        Index("ix_sec_form4_parser_status", "parser_status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    accession_number: Mapped[str] = mapped_column(Text, nullable=False)
    issuer_cik: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    issuer_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    issuer_trading_symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reporting_owner_cik: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reporting_owner_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    filing_date: Mapped[Optional[date]]
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    xml_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    document_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_metadata_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}", nullable=False)
    raw_xml_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_xml_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    parser_status: Mapped[str] = mapped_column(Text, default="pending", server_default="pending", nullable=False)
    parser_version: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    parser_confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    last_parse_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    parsed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class InsiderTransactionNormalized(Base):
    __tablename__ = "insider_transactions_normalized"
    __table_args__ = (
        Index("ix_insider_txn_normalized_hash", "normalized_hash", unique=True),
        Index("ix_insider_txn_symbol_date", "ticker_normalized", "transaction_date"),
        Index("ix_insider_txn_owner_date", "reporting_owner_cik", "transaction_date"),
        Index("ix_insider_txn_duplicate", "is_duplicate"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    form4_filing_id: Mapped[Optional[int]]
    accession_number: Mapped[str] = mapped_column(Text, nullable=False)
    issuer_cik: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    issuer_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ticker_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ticker_normalized: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reporting_owner_cik: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reporting_owner_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    owner_relationship_json: Mapped[str] = mapped_column(Text, default="{}", server_default="{}", nullable=False)
    officer_title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_director: Mapped[bool] = mapped_column(default=False, server_default=text("false"), nullable=False)
    is_officer: Mapped[bool] = mapped_column(default=False, server_default=text("false"), nullable=False)
    is_ten_percent_owner: Mapped[bool] = mapped_column(default=False, server_default=text("false"), nullable=False)
    transaction_date: Mapped[Optional[date]]
    filing_date: Mapped[Optional[date]]
    security_title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transaction_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transaction_code_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    transaction_type_normalized: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    shares: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    acquired_disposed: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    shares_owned_following: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    direct_or_indirect: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ownership_nature: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_derivative: Mapped[bool] = mapped_column(default=False, server_default=text("false"), nullable=False)
    footnotes_json: Mapped[str] = mapped_column(Text, default="[]", server_default="[]", nullable=False)
    ten_b5_1_flag: Mapped[bool] = mapped_column(default=False, server_default=text("false"), nullable=False)
    normalized_hash: Mapped[str] = mapped_column(Text, nullable=False)
    is_duplicate: Mapped[bool] = mapped_column(default=False, server_default=text("false"), nullable=False)
    duplicate_of_id: Mapped[Optional[int]]
    parser_confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class DataEnrichmentJob(Base):
    __tablename__ = "data_enrichment_jobs"
    __table_args__ = (
        UniqueConstraint("dedupe_key", name="uq_data_enrichment_jobs_dedupe_key"),
        Index("ix_data_enrichment_jobs_type_status", "job_type", "status"),
        Index("ix_data_enrichment_jobs_symbol", "symbol"),
        Index("ix_data_enrichment_jobs_status_next_run", "status", "next_run_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    job_type: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    date_key: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    window_key: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    dedupe_key: Mapped[str] = mapped_column(Text, nullable=False)
    priority: Mapped[int] = mapped_column(default=100, server_default=text("100"), nullable=False)
    status: Mapped[str] = mapped_column(Text, default="queued", server_default="queued", nullable=False)
    attempts: Mapped[int] = mapped_column(default=0, server_default=text("0"), nullable=False)
    max_attempts: Mapped[int] = mapped_column(default=5, server_default=text("5"), nullable=False)
    source: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    next_run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class HouseAnnualDisclosureDocument(Base):
    __tablename__ = "house_annual_disclosure_documents"
    __table_args__ = (
        Index("ix_house_annual_documents_member_year", "member_bioguide_id", "filing_year"),
        Index("ix_house_annual_documents_filing_date", "filing_date"),
        Index("ix_house_annual_documents_doc", "document_id", unique=True),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(Text, default="house_clerk_financial_disclosure", server_default="house_clerk_financial_disclosure")
    member_name: Mapped[str] = mapped_column(Text)
    member_bioguide_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    filing_year: Mapped[int]
    filing_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    report_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    document_id: Mapped[str] = mapped_column(Text)
    filing_date: Mapped[Optional[date]]
    state_district: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class HouseAnnualDisclosureHolding(Base):
    __tablename__ = "house_annual_disclosure_holdings"
    __table_args__ = (
        Index("ix_house_annual_holdings_member_symbol", "member_bioguide_id", "symbol"),
        Index("ix_house_annual_holdings_document", "document_row_id"),
        Index("ix_house_annual_holdings_filing_date", "filing_date"),
        UniqueConstraint(
            "document_id",
            "asset_name",
            "symbol",
            "owner",
            "value_range",
            "income_type",
            "income_range",
            name="uq_house_annual_holding_source_row",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    document_row_id: Mapped[int]
    source: Mapped[str] = mapped_column(Text, default="house_clerk_financial_disclosure", server_default="house_clerk_financial_disclosure")
    member_name: Mapped[str] = mapped_column(Text)
    member_bioguide_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    filing_year: Mapped[int]
    filing_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    filing_date: Mapped[Optional[date]]
    report_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    document_id: Mapped[str] = mapped_column(Text)
    asset_name: Mapped[str] = mapped_column(Text)
    symbol: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    owner: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    asset_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    value_range: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    value_min: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    value_max: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    income_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    income_range: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
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
