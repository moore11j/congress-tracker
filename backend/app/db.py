from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from time import perf_counter
from pathlib import Path

from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.request_priority import get_request_context

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////data/app.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

if DATABASE_URL.startswith("sqlite:////data/"):
    Path("/data").mkdir(parents=True, exist_ok=True)

IS_SQLITE = DATABASE_URL.startswith("sqlite")
connect_args = {"check_same_thread": False, "timeout": 30} if IS_SQLITE else {}
IS_CRON_PROCESS = os.getenv("FLY_PROCESS_GROUP", "").strip().lower() == "cron"


def _pool_env(name: str, default: str) -> int:
    if IS_CRON_PROCESS:
        cron_name = f"CRON_{name}"
        if cron_name in os.environ:
            return int(os.getenv(cron_name, default) or default)
    return int(os.getenv(name, default) or default)


pool_options = (
    {}
    if IS_SQLITE
    else {
        "pool_size": _pool_env("DB_POOL_SIZE", "8"),
        "max_overflow": _pool_env("DB_MAX_OVERFLOW", "4"),
        "pool_timeout": _pool_env("DB_POOL_TIMEOUT", "2"),
        "pool_recycle": _pool_env("DB_POOL_RECYCLE_SECONDS", "1800"),
        "pool_use_lifo": True,
    }
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args=connect_args,
    hide_parameters=True,
    **pool_options,
)

if IS_SQLITE:

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.execute("PRAGMA synchronous=NORMAL")
        finally:
            cursor.close()


SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _set_postgres_ddl_timeouts(conn, *, lock_timeout: str = "2s", statement_timeout: str = "10s") -> None:
    if conn.dialect.name != "postgresql":
        return
    conn.execute(text(f"SET LOCAL lock_timeout = '{lock_timeout}'"))
    conn.execute(text(f"SET LOCAL statement_timeout = '{statement_timeout}'"))


@dataclass(frozen=True)
class OptionalIndexSpec:
    name: str
    table: str
    sqlite_sql: str
    postgres_sql: str


OPTIONAL_PERFORMANCE_INDEXES: tuple[OptionalIndexSpec, ...] = (
    OptionalIndexSpec(
        name="ix_securities_symbol_lower",
        table="securities",
        sqlite_sql="CREATE INDEX IF NOT EXISTS ix_securities_symbol_lower ON securities (lower(symbol))",
        postgres_sql="CREATE INDEX {concurrently}IF NOT EXISTS ix_securities_symbol_lower ON securities ((lower(symbol)))",
    ),
    OptionalIndexSpec(
        name="ix_securities_name_lower",
        table="securities",
        sqlite_sql="CREATE INDEX IF NOT EXISTS ix_securities_name_lower ON securities (lower(name))",
        postgres_sql="CREATE INDEX {concurrently}IF NOT EXISTS ix_securities_name_lower ON securities ((lower(name)))",
    ),
    OptionalIndexSpec(
        name="ix_ticker_meta_symbol_lower",
        table="ticker_meta",
        sqlite_sql="CREATE INDEX IF NOT EXISTS ix_ticker_meta_symbol_lower ON ticker_meta (lower(symbol))",
        postgres_sql="CREATE INDEX {concurrently}IF NOT EXISTS ix_ticker_meta_symbol_lower ON ticker_meta ((lower(symbol)))",
    ),
    OptionalIndexSpec(
        name="ix_ticker_meta_company_name_lower",
        table="ticker_meta",
        sqlite_sql="CREATE INDEX IF NOT EXISTS ix_ticker_meta_company_name_lower ON ticker_meta (lower(company_name))",
        postgres_sql=(
            "CREATE INDEX {concurrently}IF NOT EXISTS ix_ticker_meta_company_name_lower "
            "ON ticker_meta ((lower(company_name)))"
        ),
    ),
    OptionalIndexSpec(
        name="ix_members_name_lower",
        table="members",
        sqlite_sql="CREATE INDEX IF NOT EXISTS ix_members_name_lower ON members (lower(first_name), lower(last_name))",
        postgres_sql=(
            "CREATE INDEX {concurrently}IF NOT EXISTS ix_members_name_lower "
            "ON members ((lower(first_name)), (lower(last_name)))"
        ),
    ),
    OptionalIndexSpec(
        name="ix_events_member_name_lower",
        table="events",
        sqlite_sql="CREATE INDEX IF NOT EXISTS ix_events_member_name_lower ON events (lower(member_name))",
        postgres_sql=(
            "CREATE INDEX {concurrently}IF NOT EXISTS ix_events_member_name_lower "
            "ON events ((lower(member_name)))"
        ),
    ),
    OptionalIndexSpec(
        name="ix_events_symbol_type_effective_ts_id",
        table="events",
        sqlite_sql=(
            "CREATE INDEX IF NOT EXISTS ix_events_symbol_type_effective_ts_id "
            "ON events (symbol, event_type, coalesce(event_date, ts) DESC, id DESC)"
        ),
        postgres_sql=(
            "CREATE INDEX {concurrently}IF NOT EXISTS ix_events_symbol_type_effective_ts_id "
            "ON events (symbol, event_type, (coalesce(event_date, ts)) DESC, id DESC)"
        ),
    ),
    OptionalIndexSpec(
        name="ix_events_symbol_effective_ts_id",
        table="events",
        sqlite_sql=(
            "CREATE INDEX IF NOT EXISTS ix_events_symbol_effective_ts_id "
            "ON events (symbol, coalesce(event_date, ts) DESC, id DESC)"
        ),
        postgres_sql=(
            "CREATE INDEX {concurrently}IF NOT EXISTS ix_events_symbol_effective_ts_id "
            "ON events (symbol, (coalesce(event_date, ts)) DESC, id DESC)"
        ),
    ),
    OptionalIndexSpec(
        name="ix_events_upper_symbol_type_effective_ts_id",
        table="events",
        sqlite_sql=(
            "CREATE INDEX IF NOT EXISTS ix_events_upper_symbol_type_effective_ts_id "
            "ON events (upper(symbol), event_type, coalesce(event_date, ts) DESC, id DESC)"
        ),
        postgres_sql=(
            "CREATE INDEX {concurrently}IF NOT EXISTS ix_events_upper_symbol_type_effective_ts_id "
            "ON events ((upper(symbol)), event_type, (coalesce(event_date, ts)) DESC, id DESC)"
        ),
    ),
    OptionalIndexSpec(
        name="idx_events_effective_date_id_desc",
        table="events",
        sqlite_sql=(
            "CREATE INDEX IF NOT EXISTS idx_events_effective_date_id_desc "
            "ON events (coalesce(event_date, ts) DESC, id DESC)"
        ),
        postgres_sql=(
            "CREATE INDEX {concurrently}IF NOT EXISTS idx_events_effective_date_id_desc "
            "ON events ((coalesce(event_date, ts)) DESC, id DESC)"
        ),
    ),
    OptionalIndexSpec(
        name="ix_events_insider_payload_json_trgm",
        table="events",
        sqlite_sql="CREATE INDEX IF NOT EXISTS ix_events_insider_payload_json_trgm ON events (payload_json)",
        postgres_sql=(
            "CREATE INDEX {concurrently}IF NOT EXISTS ix_events_insider_payload_json_trgm "
            "ON events USING gin (payload_json gin_trgm_ops) WHERE event_type = 'insider_trade'"
        ),
    ),
)


def _optional_index_skip_reason(exc: BaseException) -> str:
    message = str(exc).lower()
    if "lock timeout" in message or "locknotavailable" in message:
        return "lock_timeout"
    if "already exists" in message or "duplicate" in message:
        return "already_exists_race"
    if "does not exist" in message or "undefinedtable" in message or "undefinedcolumn" in message:
        return "missing_relation"
    return "error"


def _optional_index_tables(conn, dialect_name: str) -> set[str]:
    if dialect_name == "sqlite":
        return {
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type='table'
                      AND name IN ('securities', 'ticker_meta', 'members', 'events')
                    """
                )
            ).fetchall()
        }
    if dialect_name == "postgresql":
        return {
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                      AND table_name IN ('securities', 'ticker_meta', 'members', 'events')
                    """
                )
            ).fetchall()
        }
    return set()


def _create_optional_index(conn, spec: OptionalIndexSpec, *, dialect_name: str, concurrent: bool) -> bool:
    if dialect_name == "sqlite":
        statement = spec.sqlite_sql
    elif dialect_name == "postgresql":
        statement = spec.postgres_sql.format(concurrently="CONCURRENTLY " if concurrent else "")
    else:
        logger.info(
            "startup_step_skipped name=optional_index reason=unsupported_dialect index=%s table=%s dialect=%s",
            spec.name,
            spec.table,
            dialect_name,
        )
        return False

    try:
        conn.execute(text(statement))
    except SQLAlchemyError as exc:
        reason = _optional_index_skip_reason(exc)
        logger.warning(
            "startup_step_skipped name=optional_index reason=%s index=%s table=%s",
            reason,
            spec.name,
            spec.table,
        )
        return False

    logger.info("optional_index_complete index=%s table=%s concurrent=%s", spec.name, spec.table, concurrent)
    return True


def ensure_optional_performance_indexes(
    bind=engine,
    *,
    concurrent: bool | None = None,
    index_names: set[str] | None = None,
    lock_timeout: str = "2s",
    statement_timeout: str = "30s",
) -> dict[str, object]:
    """
    Create performance-only indexes. This is intentionally not called from web
    startup because these indexes can touch large/hot production tables.
    """
    with bind.connect() as conn:
        dialect_name = conn.dialect.name
        existing_tables = _optional_index_tables(conn, dialect_name)

    specs = [
        spec
        for spec in OPTIONAL_PERFORMANCE_INDEXES
        if index_names is None or spec.name in index_names
    ]
    use_concurrent = dialect_name == "postgresql" if concurrent is None else concurrent
    attempted = completed = skipped = 0

    if dialect_name == "postgresql" and use_concurrent:
        with bind.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f"SET lock_timeout = '{lock_timeout}'"))
            conn.execute(text(f"SET statement_timeout = '{statement_timeout}'"))
            for spec in specs:
                if spec.table not in existing_tables:
                    logger.info(
                        "startup_step_skipped name=optional_index reason=table_missing index=%s table=%s",
                        spec.name,
                        spec.table,
                    )
                    skipped += 1
                    continue
                attempted += 1
                if _create_optional_index(conn, spec, dialect_name=dialect_name, concurrent=True):
                    completed += 1
                else:
                    skipped += 1
        return {"attempted": attempted, "completed": completed, "skipped": skipped}

    with bind.begin() as conn:
        if dialect_name == "postgresql":
            _set_postgres_ddl_timeouts(conn, lock_timeout=lock_timeout, statement_timeout=statement_timeout)
        for spec in specs:
            if spec.table not in existing_tables:
                logger.info(
                    "startup_step_skipped name=optional_index reason=table_missing index=%s table=%s",
                    spec.name,
                    spec.table,
                )
                skipped += 1
                continue
            attempted += 1
            if _create_optional_index(conn, spec, dialect_name=dialect_name, concurrent=False):
                completed += 1
            else:
                skipped += 1
    return {"attempted": attempted, "completed": completed, "skipped": skipped}


if not IS_SQLITE:

    @event.listens_for(engine, "checkout")
    def _log_slow_db_checkout(_dbapi_connection, _connection_record, _connection_proxy) -> None:
        context = get_request_context()
        context["db_checkout_count"] = int(context.get("db_checkout_count") or 0) + 1
        started_at = context.get("started_at")
        if not isinstance(started_at, (int, float)):
            return
        elapsed_ms = (perf_counter() - started_at) * 1000
        threshold_ms = float(os.getenv("DB_CHECKOUT_SLOW_LOG_MS", "250") or 250)
        if elapsed_ms < threshold_ms:
            return
        context["db_checkout_slow_count"] = int(context.get("db_checkout_slow_count") or 0) + 1
        if context.get("db_checkout_slow_logged"):
            return
        context["db_checkout_slow_logged"] = True
        logger.warning(
            "db_pool_checkout_slow path=%s priority=%s route_family=%s request_source=%s walnut_route=%s walnut_component=%s panel=%s ua_class=%s elapsed_ms=%.1f",
            context.get("path", "unknown"),
            context.get("priority", "unknown"),
            context.get("route_family", "unknown"),
            context.get("request_source", "unknown"),
            context.get("walnut_route", "unknown"),
            context.get("walnut_component", "unknown"),
            context.get("panel", "unknown"),
            context.get("user_agent_class", "unknown"),
            elapsed_ms,
        )


@event.listens_for(engine, "before_cursor_execute")
def _count_request_db_query(_conn, _cursor, _statement, _parameters, _context, _executemany) -> None:
    context = get_request_context()
    if context:
        context["db_query_count"] = int(context.get("db_query_count") or 0) + 1


class Base(DeclarativeBase):
    pass


def ensure_ticker_meta_identity_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        if dialect_name == "sqlite":
            table_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='ticker_meta'")
            ).fetchone()
            if not table_exists:
                return
            existing = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(ticker_meta)")).fetchall()
                if len(row) > 1
            }
            for name in ("sector", "industry", "country"):
                if name not in existing:
                    conn.execute(text(f"ALTER TABLE ticker_meta ADD COLUMN {name} TEXT"))
            return

        if dialect_name == "postgresql":
            table_exists = conn.execute(text("SELECT to_regclass('public.ticker_meta')")).scalar()
            if table_exists is None:
                return
            conn.execute(text("ALTER TABLE ticker_meta ADD COLUMN IF NOT EXISTS sector TEXT"))
            conn.execute(text("ALTER TABLE ticker_meta ADD COLUMN IF NOT EXISTS industry TEXT"))
            conn.execute(text("ALTER TABLE ticker_meta ADD COLUMN IF NOT EXISTS country TEXT"))


def ensure_index_membership_metadata_schema(bind=engine) -> None:
    columns = {
        "source_kind": "TEXT",
        "source_page": "TEXT",
        "source_revision_id": "TEXT",
        "resolved_source_title": "TEXT",
        "parser_version": "TEXT",
    }
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        if dialect_name == "sqlite":
            table_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='index_memberships'")
            ).fetchone()
            if not table_exists:
                return
            existing = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(index_memberships)")).fetchall()
                if len(row) > 1
            }
            for name, column_type in columns.items():
                if name not in existing:
                    conn.execute(text(f"ALTER TABLE index_memberships ADD COLUMN {name} {column_type}"))
            return

        if dialect_name == "postgresql":
            table_exists = conn.execute(text("SELECT to_regclass('public.index_memberships')")).scalar()
            if table_exists is None:
                return
            for name, column_type in columns.items():
                conn.execute(text(f"ALTER TABLE index_memberships ADD COLUMN IF NOT EXISTS {name} {column_type}"))


def ensure_price_cache_volume_columns(bind=engine) -> None:
    columns = {
        "volume": "FLOAT",
        "day_volume": "FLOAT",
        "adjusted_close": "FLOAT",
        "raw_close": "FLOAT",
        "open_price": "FLOAT",
        "high_price": "FLOAT",
        "low_price": "FLOAT",
        "dollar_volume": "FLOAT",
        "split_coefficient": "FLOAT",
        "dividend_amount": "FLOAT",
        "price_source": "TEXT",
        "provider_symbol": "TEXT",
        "adjustment_status": "TEXT",
    }
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn, statement_timeout="5s")
        if dialect_name == "sqlite":
            table_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='price_cache'")
            ).fetchone()
            if not table_exists:
                return
            existing = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(price_cache)")).fetchall()
                if len(row) > 1
            }
            for name, column_type in columns.items():
                if name not in existing:
                    conn.execute(text(f"ALTER TABLE price_cache ADD COLUMN {name} {column_type}"))
            return

        if dialect_name == "postgresql":
            table_exists = conn.execute(text("SELECT to_regclass('public.price_cache')")).scalar()
            if table_exists is None:
                return
            existing = {
                row[0]
                for row in conn.execute(
                    text(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = 'price_cache'
                        """
                    )
                ).fetchall()
            }
            missing = [name for name in columns if name not in existing]
            if not missing:
                return
            try:
                conn.execute(text("SET LOCAL lock_timeout = '2s'"))
                conn.execute(text("SET LOCAL statement_timeout = '5s'"))
                for name in missing:
                    conn.execute(text(f"ALTER TABLE price_cache ADD COLUMN {name} {columns[name]}"))
            except SQLAlchemyError as exc:
                logger.warning("price_cache_volume_schema_update_skipped reason=%s", exc.__class__.__name__)


def ensure_quote_cache_market_cap_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn, statement_timeout="5s")
        if dialect_name == "sqlite":
            table_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='quotes_cache'")
            ).fetchone()
            if not table_exists:
                return
            existing = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(quotes_cache)")).fetchall()
                if len(row) > 1
            }
            if "market_cap" not in existing:
                conn.execute(text("ALTER TABLE quotes_cache ADD COLUMN market_cap FLOAT"))
            return

        if dialect_name == "postgresql":
            table_exists = conn.execute(text("SELECT to_regclass('public.quotes_cache')")).scalar()
            if table_exists is None:
                return
            conn.execute(text("ALTER TABLE quotes_cache ADD COLUMN IF NOT EXISTS market_cap DOUBLE PRECISION"))


def ensure_market_pressure_snapshot_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        timestamp_type = "TIMESTAMP" if dialect_name == "sqlite" else "TIMESTAMPTZ"
        id_type = "INTEGER PRIMARY KEY AUTOINCREMENT" if dialect_name == "sqlite" else "BIGSERIAL PRIMARY KEY"
        now_default = "CURRENT_TIMESTAMP" if dialect_name == "sqlite" else "now()"
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS market_pressure_snapshots (
                    id {id_type},
                    universe TEXT NOT NULL,
                    period TEXT NOT NULL DEFAULT '1d',
                    symbol TEXT NOT NULL,
                    company_name TEXT,
                    sector TEXT,
                    exchange TEXT,
                    price FLOAT,
                    price_change_pct FLOAT,
                    market_cap FLOAT,
                    confirmation_score INTEGER,
                    confirmation_direction TEXT,
                    data_state TEXT,
                    price_as_of {timestamp_type},
                    confirmation_as_of {timestamp_type},
                    generated_at {timestamp_type} NOT NULL,
                    source TEXT NOT NULL DEFAULT 'market_pressure_ingest',
                    tile_json TEXT NOT NULL,
                    created_at {timestamp_type} DEFAULT {now_default},
                    updated_at {timestamp_type} DEFAULT {now_default},
                    CONSTRAINT uq_market_pressure_snapshot_universe_period_symbol UNIQUE (universe, period, symbol)
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_market_pressure_snapshots_universe_period "
                "ON market_pressure_snapshots (universe, period, generated_at)"
            )
        )
        conn.execute(
            text("CREATE INDEX IF NOT EXISTS ix_market_pressure_snapshots_symbol ON market_pressure_snapshots (symbol)")
        )


def ensure_strategy_storage_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        timestamp_type = "TIMESTAMP" if dialect_name == "sqlite" else "TIMESTAMPTZ"
        float_type = "FLOAT" if dialect_name == "sqlite" else "DOUBLE PRECISION"
        id_type = "INTEGER PRIMARY KEY AUTOINCREMENT" if dialect_name == "sqlite" else "BIGSERIAL PRIMARY KEY"
        now_default = "CURRENT_TIMESTAMP" if dialect_name == "sqlite" else "now()"
        false_default = "0" if dialect_name == "sqlite" else "false"
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS strategy_definitions (
                    id {id_type},
                    slug TEXT NOT NULL,
                    name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    family TEXT,
                    status TEXT NOT NULL DEFAULT 'draft',
                    access_tier TEXT NOT NULL DEFAULT 'premium',
                    is_featured BOOLEAN NOT NULL DEFAULT {false_default},
                    sort_order INTEGER NOT NULL DEFAULT 100,
                    short_description TEXT,
                    walnut_take TEXT,
                    methodology TEXT,
                    rule_json TEXT NOT NULL DEFAULT '{{}}',
                    parameters_json TEXT NOT NULL DEFAULT '{{}}',
                    universe_json TEXT NOT NULL DEFAULT '{{}}',
                    tags_json TEXT NOT NULL DEFAULT '[]',
                    risk_notes_json TEXT NOT NULL DEFAULT '[]',
                    data_quality_confidence TEXT NOT NULL DEFAULT 'unknown',
                    methodology_version TEXT NOT NULL,
                    created_by TEXT,
                    created_at {timestamp_type} NOT NULL DEFAULT {now_default},
                    updated_at {timestamp_type} NOT NULL DEFAULT {now_default},
                    published_at {timestamp_type}
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS strategy_backtest_runs (
                    id {id_type},
                    strategy_id INTEGER NOT NULL,
                    run_key TEXT NOT NULL,
                    run_type TEXT NOT NULL DEFAULT 'research',
                    status TEXT NOT NULL DEFAULT 'pending',
                    started_at {timestamp_type},
                    completed_at {timestamp_type},
                    backtest_start_date DATE,
                    backtest_end_date DATE,
                    benchmark TEXT NOT NULL DEFAULT 'SPY',
                    methodology_version TEXT NOT NULL,
                    code_version TEXT,
                    dataset_versions_json TEXT NOT NULL DEFAULT '{{}}',
                    parameters_json TEXT NOT NULL DEFAULT '{{}}',
                    universe_hash TEXT,
                    universe_json TEXT NOT NULL DEFAULT '{{}}',
                    execution_timing TEXT,
                    fee_bps_per_side {float_type} NOT NULL DEFAULT 0,
                    slippage_bps_per_side {float_type} NOT NULL DEFAULT 0,
                    metrics_json TEXT NOT NULL DEFAULT '{{}}',
                    diagnostics_json TEXT NOT NULL DEFAULT '{{}}',
                    walnut_strategy_score {float_type},
                    data_quality_confidence TEXT NOT NULL DEFAULT 'unknown',
                    error TEXT,
                    created_at {timestamp_type} NOT NULL DEFAULT {now_default},
                    updated_at {timestamp_type} NOT NULL DEFAULT {now_default}
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS strategy_performance_snapshots (
                    id {id_type},
                    strategy_id INTEGER NOT NULL,
                    run_id INTEGER NOT NULL,
                    as_of_date DATE NOT NULL,
                    period TEXT NOT NULL,
                    total_return_pct {float_type},
                    cagr_pct {float_type},
                    benchmark_return_pct {float_type},
                    benchmark_cagr_pct {float_type},
                    alpha_cagr_pct {float_type},
                    beta {float_type},
                    sharpe {float_type},
                    sortino {float_type},
                    max_drawdown_pct {float_type},
                    annualized_volatility_pct {float_type},
                    win_rate_pct {float_type},
                    trade_count INTEGER,
                    independent_signal_count INTEGER,
                    avg_holdings {float_type},
                    turnover_events INTEGER,
                    rolling_12m_beating_spy_pct {float_type},
                    walnut_strategy_score {float_type},
                    metrics_json TEXT NOT NULL DEFAULT '{{}}',
                    created_at {timestamp_type} NOT NULL DEFAULT {now_default}
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS strategy_equity_curve_points (
                    id {id_type},
                    strategy_id INTEGER NOT NULL,
                    run_id INTEGER NOT NULL,
                    date DATE NOT NULL,
                    strategy_value {float_type} NOT NULL,
                    benchmark_value {float_type},
                    drawdown_pct {float_type},
                    active_holdings INTEGER,
                    created_at {timestamp_type} NOT NULL DEFAULT {now_default}
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS strategy_holdings_snapshots (
                    id {id_type},
                    strategy_id INTEGER NOT NULL,
                    run_id INTEGER NOT NULL,
                    as_of_date DATE NOT NULL,
                    holdings_count INTEGER NOT NULL DEFAULT 0,
                    total_weight_pct {float_type},
                    cash_weight_pct {float_type},
                    generated_at {timestamp_type} NOT NULL DEFAULT {now_default},
                    diagnostics_json TEXT NOT NULL DEFAULT '{{}}'
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS strategy_holding_rows (
                    id {id_type},
                    strategy_id INTEGER NOT NULL,
                    snapshot_id INTEGER NOT NULL,
                    run_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    company_name TEXT,
                    sector TEXT,
                    rank INTEGER,
                    weight_pct {float_type},
                    entry_date DATE,
                    avg_entry_price {float_type},
                    last_price {float_type},
                    return_pct {float_type},
                    source_signal_count INTEGER NOT NULL DEFAULT 0,
                    source_signals_json TEXT NOT NULL DEFAULT '[]',
                    payload_json TEXT NOT NULL DEFAULT '{{}}',
                    created_at {timestamp_type} NOT NULL DEFAULT {now_default}
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS strategy_current_holdings (
                    id {id_type},
                    strategy_id INTEGER NOT NULL,
                    run_id INTEGER NOT NULL,
                    as_of_date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    company_name TEXT,
                    sector TEXT,
                    rank INTEGER,
                    weight_pct {float_type},
                    entry_date DATE,
                    last_price {float_type},
                    return_pct {float_type},
                    source_signal_count INTEGER NOT NULL DEFAULT 0,
                    source_signals_json TEXT NOT NULL DEFAULT '[]',
                    payload_json TEXT NOT NULL DEFAULT '{{}}',
                    updated_at {timestamp_type} NOT NULL DEFAULT {now_default}
                )
                """
            )
        )
        for statement in (
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_strategy_definitions_slug ON strategy_definitions (slug)",
            "CREATE INDEX IF NOT EXISTS ix_strategy_definitions_category_status ON strategy_definitions (category, status)",
            "CREATE INDEX IF NOT EXISTS ix_strategy_definitions_featured ON strategy_definitions (is_featured, sort_order)",
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_backtest_runs_strategy_run_key ON strategy_backtest_runs (strategy_id, run_key)",
            "CREATE INDEX IF NOT EXISTS ix_strategy_backtest_runs_strategy_completed ON strategy_backtest_runs (strategy_id, completed_at)",
            "CREATE INDEX IF NOT EXISTS ix_strategy_backtest_runs_status ON strategy_backtest_runs (status)",
            "CREATE INDEX IF NOT EXISTS ix_strategy_backtest_runs_score ON strategy_backtest_runs (walnut_strategy_score)",
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_perf_snapshot_period ON strategy_performance_snapshots (strategy_id, run_id, as_of_date, period)",
            "CREATE INDEX IF NOT EXISTS ix_strategy_perf_snapshots_strategy_period ON strategy_performance_snapshots (strategy_id, period, as_of_date)",
            "CREATE INDEX IF NOT EXISTS ix_strategy_perf_snapshots_score ON strategy_performance_snapshots (walnut_strategy_score)",
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_equity_curve_run_date ON strategy_equity_curve_points (run_id, date)",
            "CREATE INDEX IF NOT EXISTS ix_strategy_equity_curve_strategy_date ON strategy_equity_curve_points (strategy_id, date)",
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_holdings_snapshot_date ON strategy_holdings_snapshots (strategy_id, run_id, as_of_date)",
            "CREATE INDEX IF NOT EXISTS ix_strategy_holdings_snapshots_strategy_date ON strategy_holdings_snapshots (strategy_id, as_of_date)",
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_holding_rows_snapshot_symbol ON strategy_holding_rows (snapshot_id, symbol)",
            "CREATE INDEX IF NOT EXISTS ix_strategy_holding_rows_strategy_symbol ON strategy_holding_rows (strategy_id, symbol)",
            "CREATE INDEX IF NOT EXISTS ix_strategy_holding_rows_snapshot_rank ON strategy_holding_rows (snapshot_id, rank)",
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_current_holdings_strategy_symbol ON strategy_current_holdings (strategy_id, symbol)",
            "CREATE INDEX IF NOT EXISTS ix_strategy_current_holdings_strategy_rank ON strategy_current_holdings (strategy_id, rank)",
            "CREATE INDEX IF NOT EXISTS ix_strategy_current_holdings_symbol ON strategy_current_holdings (symbol)",
        ):
            conn.execute(text(statement))


def ensure_fundamentals_cache_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        if dialect_name == "sqlite":
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS fundamentals_cache (
                        id INTEGER PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        provider TEXT NOT NULL DEFAULT 'fmp',
                        fetched_at TIMESTAMP NOT NULL,
                        period_date DATE,
                        status TEXT NOT NULL DEFAULT 'ok',
                        error TEXT,
                        company_name TEXT,
                        sector TEXT,
                        industry TEXT,
                        country TEXT,
                        exchange TEXT,
                        market_cap FLOAT,
                        price FLOAT,
                        volume FLOAT,
                        avg_volume FLOAT,
                        beta FLOAT,
                        dividend_yield FLOAT,
                        trailing_pe FLOAT,
                        forward_pe FLOAT,
                        price_to_sales FLOAT,
                        ev_to_ebitda FLOAT,
                        gross_margin FLOAT,
                        operating_margin FLOAT,
                        operating_margin_expansion FLOAT,
                        net_margin FLOAT,
                        roe FLOAT,
                        roic FLOAT,
                        revenue_growth FLOAT,
                        eps_growth FLOAT,
                        ebitda_growth FLOAT,
                        free_cash_flow FLOAT,
                        fcf_yield FLOAT,
                        fcf_margin FLOAT,
                        fcf_growth FLOAT,
                        debt_to_equity FLOAT,
                        current_ratio FLOAT,
                        net_debt_to_ebitda FLOAT,
                        eps_ttm FLOAT,
                        earnings_yield FLOAT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            existing = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(fundamentals_cache)")).fetchall()
                if len(row) > 1
            }
            columns = {
                "period_date": "DATE",
                "status": "TEXT NOT NULL DEFAULT 'ok'",
                "error": "TEXT",
                "company_name": "TEXT",
                "sector": "TEXT",
                "industry": "TEXT",
                "country": "TEXT",
                "exchange": "TEXT",
                "market_cap": "FLOAT",
                "price": "FLOAT",
                "volume": "FLOAT",
                "avg_volume": "FLOAT",
                "beta": "FLOAT",
                "dividend_yield": "FLOAT",
                "trailing_pe": "FLOAT",
                "forward_pe": "FLOAT",
                "price_to_sales": "FLOAT",
                "ev_to_ebitda": "FLOAT",
                "gross_margin": "FLOAT",
                "operating_margin": "FLOAT",
                "operating_margin_expansion": "FLOAT",
                "net_margin": "FLOAT",
                "roe": "FLOAT",
                "roic": "FLOAT",
                "revenue_growth": "FLOAT",
                "eps_growth": "FLOAT",
                "ebitda_growth": "FLOAT",
                "free_cash_flow": "FLOAT",
                "fcf_yield": "FLOAT",
                "fcf_margin": "FLOAT",
                "fcf_growth": "FLOAT",
                "debt_to_equity": "FLOAT",
                "current_ratio": "FLOAT",
                "net_debt_to_ebitda": "FLOAT",
                "eps_ttm": "FLOAT",
                "earnings_yield": "FLOAT",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            }
            for name, column_type in columns.items():
                if name not in existing:
                    conn.execute(text(f"ALTER TABLE fundamentals_cache ADD COLUMN {name} {column_type}"))
        else:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS fundamentals_cache (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        provider TEXT NOT NULL DEFAULT 'fmp',
                        fetched_at TIMESTAMPTZ NOT NULL,
                        period_date DATE,
                        status TEXT NOT NULL DEFAULT 'ok',
                        error TEXT,
                        company_name TEXT,
                        sector TEXT,
                        industry TEXT,
                        country TEXT,
                        exchange TEXT,
                        market_cap DOUBLE PRECISION,
                        price DOUBLE PRECISION,
                        volume DOUBLE PRECISION,
                        avg_volume DOUBLE PRECISION,
                        beta DOUBLE PRECISION,
                        dividend_yield DOUBLE PRECISION,
                        trailing_pe DOUBLE PRECISION,
                        forward_pe DOUBLE PRECISION,
                        price_to_sales DOUBLE PRECISION,
                        ev_to_ebitda DOUBLE PRECISION,
                        gross_margin DOUBLE PRECISION,
                        operating_margin DOUBLE PRECISION,
                        operating_margin_expansion DOUBLE PRECISION,
                        net_margin DOUBLE PRECISION,
                        roe DOUBLE PRECISION,
                        roic DOUBLE PRECISION,
                        revenue_growth DOUBLE PRECISION,
                        eps_growth DOUBLE PRECISION,
                        ebitda_growth DOUBLE PRECISION,
                        free_cash_flow DOUBLE PRECISION,
                        fcf_yield DOUBLE PRECISION,
                        fcf_margin DOUBLE PRECISION,
                        fcf_growth DOUBLE PRECISION,
                        debt_to_equity DOUBLE PRECISION,
                        current_ratio DOUBLE PRECISION,
                        net_debt_to_ebitda DOUBLE PRECISION,
                        eps_ttm DOUBLE PRECISION,
                        earnings_yield DOUBLE PRECISION,
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            conn.execute(text("ALTER TABLE fundamentals_cache ADD COLUMN IF NOT EXISTS period_date DATE"))
            conn.execute(text("ALTER TABLE fundamentals_cache ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'ok'"))
            conn.execute(text("ALTER TABLE fundamentals_cache ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now()"))
            for name in ("error", "company_name", "sector", "industry", "country", "exchange"):
                conn.execute(text(f"ALTER TABLE fundamentals_cache ADD COLUMN IF NOT EXISTS {name} TEXT"))
            for name in (
                "market_cap",
                "price",
                "volume",
                "avg_volume",
                "beta",
                "dividend_yield",
                "trailing_pe",
                "forward_pe",
                "price_to_sales",
                "ev_to_ebitda",
                "gross_margin",
                "operating_margin",
                "operating_margin_expansion",
                "net_margin",
                "roe",
                "roic",
                "revenue_growth",
                "eps_growth",
                "ebitda_growth",
                "free_cash_flow",
                "fcf_yield",
                "fcf_margin",
                "fcf_growth",
                "debt_to_equity",
                "current_ratio",
                "net_debt_to_ebitda",
                "eps_ttm",
                "earnings_yield",
            ):
                conn.execute(text(f"ALTER TABLE fundamentals_cache ADD COLUMN IF NOT EXISTS {name} DOUBLE PRECISION"))
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_fundamentals_cache_symbol_provider "
                "ON fundamentals_cache (symbol, provider)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_fundamentals_cache_provider_fetched "
                "ON fundamentals_cache (provider, fetched_at)"
            )
        )


def ensure_fundamentals_snapshot_schema(bind=engine) -> None:
    text_columns = (
        "error",
        "source_payload_hash",
        "source_kind",
        "availability_basis",
        "data_quality_confidence",
        "methodology_version",
        "company_name",
        "sector",
        "industry",
        "country",
        "exchange",
    )
    numeric_columns = (
        "market_cap",
        "price",
        "volume",
        "avg_volume",
        "beta",
        "dividend_yield",
        "trailing_pe",
        "forward_pe",
        "price_to_sales",
        "ev_to_ebitda",
        "gross_margin",
        "operating_margin",
        "operating_margin_expansion",
        "net_margin",
        "roe",
        "roic",
        "revenue_growth",
        "eps_growth",
        "ebitda_growth",
        "free_cash_flow",
        "fcf_yield",
        "fcf_margin",
        "fcf_growth",
        "debt_to_equity",
        "current_ratio",
        "net_debt_to_ebitda",
        "eps_ttm",
        "earnings_yield",
    )
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        if dialect_name == "sqlite":
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS fundamentals_snapshots (
                        id INTEGER PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        provider TEXT NOT NULL DEFAULT 'fmp',
                        snapshot_date DATE NOT NULL,
                        observed_at TIMESTAMP NOT NULL,
                        source_fetched_at TIMESTAMP NOT NULL,
                        period_date DATE,
                        status TEXT NOT NULL DEFAULT 'ok',
                        error TEXT,
                        source_payload_hash TEXT,
                        source_kind TEXT,
                        availability_basis TEXT,
                        data_quality_confidence TEXT,
                        methodology_version TEXT NOT NULL DEFAULT 'fundamentals_snapshot_v1',
                        company_name TEXT,
                        sector TEXT,
                        industry TEXT,
                        country TEXT,
                        exchange TEXT,
                        market_cap FLOAT,
                        price FLOAT,
                        volume FLOAT,
                        avg_volume FLOAT,
                        beta FLOAT,
                        dividend_yield FLOAT,
                        trailing_pe FLOAT,
                        forward_pe FLOAT,
                        price_to_sales FLOAT,
                        ev_to_ebitda FLOAT,
                        gross_margin FLOAT,
                        operating_margin FLOAT,
                        operating_margin_expansion FLOAT,
                        net_margin FLOAT,
                        roe FLOAT,
                        roic FLOAT,
                        revenue_growth FLOAT,
                        eps_growth FLOAT,
                        ebitda_growth FLOAT,
                        free_cash_flow FLOAT,
                        fcf_yield FLOAT,
                        fcf_margin FLOAT,
                        fcf_growth FLOAT,
                        debt_to_equity FLOAT,
                        current_ratio FLOAT,
                        net_debt_to_ebitda FLOAT,
                        eps_ttm FLOAT,
                        earnings_yield FLOAT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            existing = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(fundamentals_snapshots)")).fetchall()
                if len(row) > 1
            }
            columns = {
                "provider": "TEXT NOT NULL DEFAULT 'fmp'",
                "snapshot_date": "DATE",
                "observed_at": "TIMESTAMP",
                "source_fetched_at": "TIMESTAMP",
                "period_date": "DATE",
                "status": "TEXT NOT NULL DEFAULT 'ok'",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                **{name: "TEXT" for name in text_columns},
                **{name: "FLOAT" for name in numeric_columns},
            }
            for name, column_type in columns.items():
                if name not in existing:
                    conn.execute(text(f"ALTER TABLE fundamentals_snapshots ADD COLUMN {name} {column_type}"))
        else:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS fundamentals_snapshots (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        provider TEXT NOT NULL DEFAULT 'fmp',
                        snapshot_date DATE NOT NULL,
                        observed_at TIMESTAMPTZ NOT NULL,
                        source_fetched_at TIMESTAMPTZ NOT NULL,
                        period_date DATE,
                        status TEXT NOT NULL DEFAULT 'ok',
                        error TEXT,
                        source_payload_hash TEXT,
                        source_kind TEXT,
                        availability_basis TEXT,
                        data_quality_confidence TEXT,
                        methodology_version TEXT NOT NULL DEFAULT 'fundamentals_snapshot_v1',
                        company_name TEXT,
                        sector TEXT,
                        industry TEXT,
                        country TEXT,
                        exchange TEXT,
                        market_cap DOUBLE PRECISION,
                        price DOUBLE PRECISION,
                        volume DOUBLE PRECISION,
                        avg_volume DOUBLE PRECISION,
                        beta DOUBLE PRECISION,
                        dividend_yield DOUBLE PRECISION,
                        trailing_pe DOUBLE PRECISION,
                        forward_pe DOUBLE PRECISION,
                        price_to_sales DOUBLE PRECISION,
                        ev_to_ebitda DOUBLE PRECISION,
                        gross_margin DOUBLE PRECISION,
                        operating_margin DOUBLE PRECISION,
                        operating_margin_expansion DOUBLE PRECISION,
                        net_margin DOUBLE PRECISION,
                        roe DOUBLE PRECISION,
                        roic DOUBLE PRECISION,
                        revenue_growth DOUBLE PRECISION,
                        eps_growth DOUBLE PRECISION,
                        ebitda_growth DOUBLE PRECISION,
                        free_cash_flow DOUBLE PRECISION,
                        fcf_yield DOUBLE PRECISION,
                        fcf_margin DOUBLE PRECISION,
                        fcf_growth DOUBLE PRECISION,
                        debt_to_equity DOUBLE PRECISION,
                        current_ratio DOUBLE PRECISION,
                        net_debt_to_ebitda DOUBLE PRECISION,
                        eps_ttm DOUBLE PRECISION,
                        earnings_yield DOUBLE PRECISION,
                        created_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            conn.execute(text("ALTER TABLE fundamentals_snapshots ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'fmp'"))
            conn.execute(text("ALTER TABLE fundamentals_snapshots ADD COLUMN IF NOT EXISTS snapshot_date DATE"))
            conn.execute(text("ALTER TABLE fundamentals_snapshots ADD COLUMN IF NOT EXISTS observed_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE fundamentals_snapshots ADD COLUMN IF NOT EXISTS source_fetched_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE fundamentals_snapshots ADD COLUMN IF NOT EXISTS period_date DATE"))
            conn.execute(text("ALTER TABLE fundamentals_snapshots ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'ok'"))
            conn.execute(text("ALTER TABLE fundamentals_snapshots ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now()"))
            conn.execute(text("ALTER TABLE fundamentals_snapshots ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now()"))
            for name in text_columns:
                conn.execute(text(f"ALTER TABLE fundamentals_snapshots ADD COLUMN IF NOT EXISTS {name} TEXT"))
            for name in numeric_columns:
                conn.execute(text(f"ALTER TABLE fundamentals_snapshots ADD COLUMN IF NOT EXISTS {name} DOUBLE PRECISION"))
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_fundamentals_snapshots_symbol_provider_date "
                "ON fundamentals_snapshots (symbol, provider, snapshot_date)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_fundamentals_snapshots_symbol_date "
                "ON fundamentals_snapshots (symbol, snapshot_date)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_fundamentals_snapshots_provider_observed "
                "ON fundamentals_snapshots (provider, observed_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_fundamentals_snapshots_period_date "
                "ON fundamentals_snapshots (period_date)"
            )
        )


def ensure_analyst_consensus_schema(bind=engine) -> None:
    snapshot_numeric_columns = (
        "weighted_rating_value",
        "price_target_high",
        "price_target_low",
        "price_target_median",
        "price_target_consensus",
        "price_target_average",
        "current_price_at_snapshot",
        "median_implied_upside_pct",
        "consensus_implied_upside_pct",
        "target_dispersion_pct",
    )
    snapshot_int_columns = (
        "strong_buy_count",
        "buy_count",
        "hold_count",
        "sell_count",
        "strong_sell_count",
        "total_rating_count",
        "price_target_analyst_count",
    )
    snapshot_text_columns = (
        "provider_symbol",
        "recommendation_label",
        "current_price_source",
        "availability_status",
        "provider_status",
        "source",
        "provider_error",
        "methodology_version",
        "raw_payload_json",
    )
    event_text_columns = (
        "provider_symbol",
        "grading_company",
        "analyst_name",
        "previous_grade",
        "new_grade",
        "action",
        "provider_action",
        "source_url",
        "provider_event_id",
        "event_fingerprint",
        "source",
        "raw_payload_json",
    )
    price_target_numeric_columns = (
        "price_target",
        "adjusted_price_target",
        "price_when_posted",
    )
    price_target_text_columns = (
        "provider_symbol",
        "analyst_company",
        "analyst_name",
        "news_title",
        "news_publisher",
        "news_url",
        "provider_event_id",
        "event_fingerprint",
        "source",
        "raw_payload_json",
    )
    run_int_columns = (
        "symbols_attempted",
        "symbols_succeeded",
        "symbols_failed",
        "records_inserted",
        "records_updated",
    )
    run_text_columns = (
        "job_name",
        "status",
        "provider_errors_json",
        "rate_limit_response",
        "error_summary",
        "metadata_json",
    )
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        if dialect_name == "sqlite":
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS analyst_consensus_snapshots (
                        id INTEGER PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        provider_symbol TEXT,
                        snapshot_date DATE NOT NULL,
                        strong_buy_count INTEGER,
                        buy_count INTEGER,
                        hold_count INTEGER,
                        sell_count INTEGER,
                        strong_sell_count INTEGER,
                        total_rating_count INTEGER,
                        weighted_rating_value FLOAT,
                        recommendation_label TEXT,
                        price_target_high FLOAT,
                        price_target_low FLOAT,
                        price_target_median FLOAT,
                        price_target_consensus FLOAT,
                        price_target_average FLOAT,
                        price_target_analyst_count INTEGER,
                        current_price_at_snapshot FLOAT,
                        current_price_source TEXT,
                        current_price_as_of TIMESTAMP,
                        median_implied_upside_pct FLOAT,
                        consensus_implied_upside_pct FLOAT,
                        target_dispersion_pct FLOAT,
                        availability_status TEXT NOT NULL DEFAULT 'unavailable',
                        provider_status TEXT NOT NULL DEFAULT 'available',
                        source TEXT NOT NULL DEFAULT 'fmp',
                        source_updated_at TIMESTAMP,
                        provider_error TEXT,
                        methodology_version TEXT NOT NULL DEFAULT 'analyst_consensus_v1',
                        raw_payload_json TEXT NOT NULL DEFAULT '{}',
                        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS analyst_grade_events (
                        id INTEGER PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        provider_symbol TEXT,
                        grading_company TEXT,
                        analyst_name TEXT,
                        previous_grade TEXT,
                        new_grade TEXT,
                        action TEXT,
                        provider_action TEXT,
                        published_date DATE,
                        source_url TEXT,
                        provider_event_id TEXT,
                        event_fingerprint TEXT NOT NULL,
                        source TEXT NOT NULL DEFAULT 'fmp',
                        raw_payload_json TEXT NOT NULL DEFAULT '{}',
                        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS analyst_consensus_ingestion_runs (
                        id INTEGER PRIMARY KEY,
                        job_name TEXT NOT NULL,
                        started_at TIMESTAMP NOT NULL,
                        completed_at TIMESTAMP,
                        status TEXT NOT NULL DEFAULT 'running',
                        symbols_attempted INTEGER NOT NULL DEFAULT 0,
                        symbols_succeeded INTEGER NOT NULL DEFAULT 0,
                        symbols_failed INTEGER NOT NULL DEFAULT 0,
                        records_inserted INTEGER NOT NULL DEFAULT 0,
                        records_updated INTEGER NOT NULL DEFAULT 0,
                        provider_errors_json TEXT NOT NULL DEFAULT '[]',
                        rate_limit_response TEXT,
                        error_summary TEXT,
                        metadata_json TEXT NOT NULL DEFAULT '{}'
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS analyst_price_target_events (
                        id INTEGER PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        provider_symbol TEXT,
                        analyst_company TEXT,
                        analyst_name TEXT,
                        price_target FLOAT,
                        adjusted_price_target FLOAT,
                        price_when_posted FLOAT,
                        published_at TIMESTAMP,
                        published_date DATE,
                        news_title TEXT,
                        news_publisher TEXT,
                        news_url TEXT,
                        provider_event_id TEXT,
                        event_fingerprint TEXT NOT NULL,
                        source TEXT NOT NULL DEFAULT 'fmp',
                        raw_payload_json TEXT NOT NULL DEFAULT '{}',
                        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            table_columns = {
                table: {
                    row[1]
                    for row in conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
                    if len(row) > 1
                }
                for table in (
                    "analyst_consensus_snapshots",
                    "analyst_grade_events",
                    "analyst_price_target_events",
                    "analyst_consensus_ingestion_runs",
                )
            }
            snapshot_columns = {
                "snapshot_date": "DATE",
                "current_price_as_of": "TIMESTAMP",
                "source_updated_at": "TIMESTAMP",
                "ingested_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                **{name: "INTEGER" for name in snapshot_int_columns},
                **{name: "FLOAT" for name in snapshot_numeric_columns},
                **{name: "TEXT" for name in snapshot_text_columns},
            }
            for name, column_type in snapshot_columns.items():
                if name not in table_columns["analyst_consensus_snapshots"]:
                    conn.execute(text(f"ALTER TABLE analyst_consensus_snapshots ADD COLUMN {name} {column_type}"))
            for name, column_type in {
                "published_date": "DATE",
                "ingested_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                **{name: "TEXT" for name in event_text_columns},
            }.items():
                if name not in table_columns["analyst_grade_events"]:
                    conn.execute(text(f"ALTER TABLE analyst_grade_events ADD COLUMN {name} {column_type}"))
            for name, column_type in {
                "published_at": "TIMESTAMP",
                "published_date": "DATE",
                "ingested_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                **{name: "FLOAT" for name in price_target_numeric_columns},
                **{name: "TEXT" for name in price_target_text_columns},
            }.items():
                if name not in table_columns["analyst_price_target_events"]:
                    conn.execute(text(f"ALTER TABLE analyst_price_target_events ADD COLUMN {name} {column_type}"))
            for name, column_type in {
                "started_at": "TIMESTAMP",
                "completed_at": "TIMESTAMP",
                **{name: "INTEGER NOT NULL DEFAULT 0" for name in run_int_columns},
                **{name: "TEXT" for name in run_text_columns},
            }.items():
                if name not in table_columns["analyst_consensus_ingestion_runs"]:
                    conn.execute(text(f"ALTER TABLE analyst_consensus_ingestion_runs ADD COLUMN {name} {column_type}"))
        else:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS analyst_consensus_snapshots (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        provider_symbol TEXT,
                        snapshot_date DATE NOT NULL,
                        strong_buy_count INTEGER,
                        buy_count INTEGER,
                        hold_count INTEGER,
                        sell_count INTEGER,
                        strong_sell_count INTEGER,
                        total_rating_count INTEGER,
                        weighted_rating_value DOUBLE PRECISION,
                        recommendation_label TEXT,
                        price_target_high DOUBLE PRECISION,
                        price_target_low DOUBLE PRECISION,
                        price_target_median DOUBLE PRECISION,
                        price_target_consensus DOUBLE PRECISION,
                        price_target_average DOUBLE PRECISION,
                        price_target_analyst_count INTEGER,
                        current_price_at_snapshot DOUBLE PRECISION,
                        current_price_source TEXT,
                        current_price_as_of TIMESTAMPTZ,
                        median_implied_upside_pct DOUBLE PRECISION,
                        consensus_implied_upside_pct DOUBLE PRECISION,
                        target_dispersion_pct DOUBLE PRECISION,
                        availability_status TEXT NOT NULL DEFAULT 'unavailable',
                        provider_status TEXT NOT NULL DEFAULT 'available',
                        source TEXT NOT NULL DEFAULT 'fmp',
                        source_updated_at TIMESTAMPTZ,
                        provider_error TEXT,
                        methodology_version TEXT NOT NULL DEFAULT 'analyst_consensus_v1',
                        raw_payload_json TEXT NOT NULL DEFAULT '{}',
                        ingested_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS analyst_grade_events (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        provider_symbol TEXT,
                        grading_company TEXT,
                        analyst_name TEXT,
                        previous_grade TEXT,
                        new_grade TEXT,
                        action TEXT,
                        provider_action TEXT,
                        published_date DATE,
                        source_url TEXT,
                        provider_event_id TEXT,
                        event_fingerprint TEXT NOT NULL,
                        source TEXT NOT NULL DEFAULT 'fmp',
                        raw_payload_json TEXT NOT NULL DEFAULT '{}',
                        ingested_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS analyst_consensus_ingestion_runs (
                        id SERIAL PRIMARY KEY,
                        job_name TEXT NOT NULL,
                        started_at TIMESTAMPTZ NOT NULL,
                        completed_at TIMESTAMPTZ,
                        status TEXT NOT NULL DEFAULT 'running',
                        symbols_attempted INTEGER NOT NULL DEFAULT 0,
                        symbols_succeeded INTEGER NOT NULL DEFAULT 0,
                        symbols_failed INTEGER NOT NULL DEFAULT 0,
                        records_inserted INTEGER NOT NULL DEFAULT 0,
                        records_updated INTEGER NOT NULL DEFAULT 0,
                        provider_errors_json TEXT NOT NULL DEFAULT '[]',
                        rate_limit_response TEXT,
                        error_summary TEXT,
                        metadata_json TEXT NOT NULL DEFAULT '{}'
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS analyst_price_target_events (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        provider_symbol TEXT,
                        analyst_company TEXT,
                        analyst_name TEXT,
                        price_target DOUBLE PRECISION,
                        adjusted_price_target DOUBLE PRECISION,
                        price_when_posted DOUBLE PRECISION,
                        published_at TIMESTAMPTZ,
                        published_date DATE,
                        news_title TEXT,
                        news_publisher TEXT,
                        news_url TEXT,
                        provider_event_id TEXT,
                        event_fingerprint TEXT NOT NULL,
                        source TEXT NOT NULL DEFAULT 'fmp',
                        raw_payload_json TEXT NOT NULL DEFAULT '{}',
                        ingested_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            table_columns = {
                "analyst_consensus_snapshots": set(),
                "analyst_grade_events": set(),
                "analyst_price_target_events": set(),
                "analyst_consensus_ingestion_runs": set(),
            }
            rows = conn.execute(
                text(
                    """
                    SELECT table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name IN (
                        'analyst_consensus_snapshots',
                        'analyst_grade_events',
                        'analyst_price_target_events',
                        'analyst_consensus_ingestion_runs'
                      )
                    """
                )
            )
            for table_name, column_name in rows:
                if table_name in table_columns:
                    table_columns[table_name].add(column_name)

            def add_pg_column(table_name: str, column_name: str, column_spec: str) -> None:
                if column_name not in table_columns.get(table_name, set()):
                    conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_spec}"))

            for name in snapshot_text_columns:
                default = " DEFAULT 'unavailable'" if name == "availability_status" else " DEFAULT 'available'" if name == "provider_status" else " DEFAULT 'fmp'" if name == "source" else " DEFAULT 'analyst_consensus_v1'" if name == "methodology_version" else " DEFAULT '{}'" if name == "raw_payload_json" else ""
                not_null = " NOT NULL" if name in {"availability_status", "provider_status", "source", "methodology_version", "raw_payload_json"} else ""
                add_pg_column("analyst_consensus_snapshots", name, f"TEXT{default}{not_null}")
            for name in snapshot_int_columns:
                add_pg_column("analyst_consensus_snapshots", name, "INTEGER")
            for name in snapshot_numeric_columns:
                add_pg_column("analyst_consensus_snapshots", name, "DOUBLE PRECISION")
            add_pg_column("analyst_consensus_snapshots", "snapshot_date", "DATE")
            add_pg_column("analyst_consensus_snapshots", "current_price_as_of", "TIMESTAMPTZ")
            add_pg_column("analyst_consensus_snapshots", "source_updated_at", "TIMESTAMPTZ")
            add_pg_column("analyst_consensus_snapshots", "ingested_at", "TIMESTAMPTZ DEFAULT now()")
            add_pg_column("analyst_consensus_snapshots", "updated_at", "TIMESTAMPTZ DEFAULT now()")
            for name in event_text_columns:
                default = " DEFAULT 'fmp'" if name == "source" else " DEFAULT '{}'" if name == "raw_payload_json" else ""
                not_null = " NOT NULL" if name in {"event_fingerprint", "source", "raw_payload_json"} else ""
                add_pg_column("analyst_grade_events", name, f"TEXT{default}{not_null}")
            add_pg_column("analyst_grade_events", "published_date", "DATE")
            add_pg_column("analyst_grade_events", "ingested_at", "TIMESTAMPTZ DEFAULT now()")
            add_pg_column("analyst_grade_events", "updated_at", "TIMESTAMPTZ DEFAULT now()")
            for name in price_target_text_columns:
                default = " DEFAULT 'fmp'" if name == "source" else " DEFAULT '{}'" if name == "raw_payload_json" else ""
                not_null = " NOT NULL" if name in {"event_fingerprint", "source", "raw_payload_json"} else ""
                add_pg_column("analyst_price_target_events", name, f"TEXT{default}{not_null}")
            for name in price_target_numeric_columns:
                add_pg_column("analyst_price_target_events", name, "DOUBLE PRECISION")
            add_pg_column("analyst_price_target_events", "published_at", "TIMESTAMPTZ")
            add_pg_column("analyst_price_target_events", "published_date", "DATE")
            add_pg_column("analyst_price_target_events", "ingested_at", "TIMESTAMPTZ DEFAULT now()")
            add_pg_column("analyst_price_target_events", "updated_at", "TIMESTAMPTZ DEFAULT now()")
            for name in run_text_columns:
                default = " DEFAULT 'running'" if name == "status" else " DEFAULT '[]'" if name == "provider_errors_json" else " DEFAULT '{}'" if name == "metadata_json" else ""
                not_null = " NOT NULL" if name in {"job_name", "status", "provider_errors_json", "metadata_json"} else ""
                add_pg_column("analyst_consensus_ingestion_runs", name, f"TEXT{default}{not_null}")
            for name in run_int_columns:
                add_pg_column("analyst_consensus_ingestion_runs", name, "INTEGER NOT NULL DEFAULT 0")
            add_pg_column("analyst_consensus_ingestion_runs", "started_at", "TIMESTAMPTZ")
            add_pg_column("analyst_consensus_ingestion_runs", "completed_at", "TIMESTAMPTZ")
        index_statements = {
            "ix_analyst_consensus_snapshots_symbol_date": (
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_analyst_consensus_snapshots_symbol_date "
                "ON analyst_consensus_snapshots (symbol, snapshot_date)"
            ),
            "ix_analyst_consensus_snapshots_source_ingested": (
                "CREATE INDEX IF NOT EXISTS ix_analyst_consensus_snapshots_source_ingested "
                "ON analyst_consensus_snapshots (source, ingested_at)"
            ),
            "ix_analyst_consensus_snapshots_availability": (
                "CREATE INDEX IF NOT EXISTS ix_analyst_consensus_snapshots_availability "
                "ON analyst_consensus_snapshots (availability_status)"
            ),
            "ix_analyst_grade_events_source_fingerprint": (
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_analyst_grade_events_source_fingerprint "
                "ON analyst_grade_events (source, event_fingerprint)"
            ),
            "ix_analyst_grade_events_symbol_date": (
                "CREATE INDEX IF NOT EXISTS ix_analyst_grade_events_symbol_date "
                "ON analyst_grade_events (symbol, published_date)"
            ),
            "ix_analyst_grade_events_action": "CREATE INDEX IF NOT EXISTS ix_analyst_grade_events_action ON analyst_grade_events (action)",
            "ix_analyst_grade_events_provider_id": (
                "CREATE INDEX IF NOT EXISTS ix_analyst_grade_events_provider_id "
                "ON analyst_grade_events (provider_event_id)"
            ),
            "ix_analyst_price_target_events_source_fingerprint": (
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_analyst_price_target_events_source_fingerprint "
                "ON analyst_price_target_events (source, event_fingerprint)"
            ),
            "ix_analyst_price_target_events_symbol_date": (
                "CREATE INDEX IF NOT EXISTS ix_analyst_price_target_events_symbol_date "
                "ON analyst_price_target_events (symbol, published_date)"
            ),
            "ix_analyst_price_target_events_provider_id": (
                "CREATE INDEX IF NOT EXISTS ix_analyst_price_target_events_provider_id "
                "ON analyst_price_target_events (provider_event_id)"
            ),
            "ix_analyst_consensus_runs_job_started": (
                "CREATE INDEX IF NOT EXISTS ix_analyst_consensus_runs_job_started "
                "ON analyst_consensus_ingestion_runs (job_name, started_at)"
            ),
            "ix_analyst_consensus_runs_status": (
                "CREATE INDEX IF NOT EXISTS ix_analyst_consensus_runs_status "
                "ON analyst_consensus_ingestion_runs (status)"
            ),
        }
        existing_indexes: set[str] = set()
        if dialect_name != "sqlite":
            existing_indexes = {
                row[0]
                for row in conn.execute(
                    text(
                        """
                        SELECT indexname
                        FROM pg_indexes
                        WHERE schemaname = 'public'
                          AND indexname = ANY(:index_names)
                        """
                    ),
                    {"index_names": list(index_statements.keys())},
                )
            }
        for index_name, statement in index_statements.items():
            if dialect_name == "sqlite" or index_name not in existing_indexes:
                conn.execute(text(statement))


def ensure_search_and_insights_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        if dialect_name == "sqlite":
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS insights_snapshots (
                        kind TEXT PRIMARY KEY,
                        payload_json TEXT NOT NULL,
                        source TEXT NOT NULL DEFAULT 'fmp',
                        fetched_at TIMESTAMP NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS fred_observations (
                        series_id TEXT NOT NULL,
                        observation_date DATE NOT NULL,
                        value FLOAT,
                        source TEXT NOT NULL DEFAULT 'fred',
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        fetched_at TIMESTAMP NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (series_id, observation_date)
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS fred_series_refreshes (
                        series_id TEXT PRIMARY KEY,
                        source TEXT NOT NULL DEFAULT 'fred',
                        status TEXT NOT NULL DEFAULT 'pending',
                        observation_count INTEGER NOT NULL DEFAULT 0,
                        latest_observation_date DATE,
                        last_refreshed_at TIMESTAMP,
                        error TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_insights_snapshots_fetched_at ON insights_snapshots (fetched_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_fred_observations_series_date ON fred_observations (series_id, observation_date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_fred_observations_fetched_at ON fred_observations (fetched_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_fred_series_refreshes_refreshed_at ON fred_series_refreshes (last_refreshed_at)"))
            return

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS insights_snapshots (
                    kind TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'fmp',
                    fetched_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT now()
                )
                """
            )
        )
        conn.execute(text("ALTER TABLE insights_snapshots ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'fmp'"))
        conn.execute(text("ALTER TABLE insights_snapshots ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()"))
        conn.execute(text("ALTER TABLE insights_snapshots ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now()"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_insights_snapshots_kind ON insights_snapshots (kind)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_insights_snapshots_fetched_at ON insights_snapshots (fetched_at)"))
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS fred_observations (
                    series_id TEXT NOT NULL,
                    observation_date DATE NOT NULL,
                    value DOUBLE PRECISION,
                    source TEXT NOT NULL DEFAULT 'fred',
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    fetched_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT now(),
                    PRIMARY KEY (series_id, observation_date)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS fred_series_refreshes (
                    series_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL DEFAULT 'fred',
                    status TEXT NOT NULL DEFAULT 'pending',
                    observation_count INTEGER NOT NULL DEFAULT 0,
                    latest_observation_date DATE,
                    last_refreshed_at TIMESTAMPTZ,
                    error TEXT,
                    updated_at TIMESTAMPTZ DEFAULT now()
                )
                """
            )
        )
        conn.execute(text("ALTER TABLE fred_observations ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'fred'"))
        conn.execute(text("ALTER TABLE fred_observations ADD COLUMN IF NOT EXISTS payload_json TEXT NOT NULL DEFAULT '{}'"))
        conn.execute(text("ALTER TABLE fred_observations ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()"))
        conn.execute(text("ALTER TABLE fred_observations ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now()"))
        conn.execute(text("ALTER TABLE fred_series_refreshes ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'fred'"))
        conn.execute(text("ALTER TABLE fred_series_refreshes ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending'"))
        conn.execute(text("ALTER TABLE fred_series_refreshes ADD COLUMN IF NOT EXISTS observation_count INTEGER NOT NULL DEFAULT 0"))
        conn.execute(text("ALTER TABLE fred_series_refreshes ADD COLUMN IF NOT EXISTS latest_observation_date DATE"))
        conn.execute(text("ALTER TABLE fred_series_refreshes ADD COLUMN IF NOT EXISTS last_refreshed_at TIMESTAMPTZ"))
        conn.execute(text("ALTER TABLE fred_series_refreshes ADD COLUMN IF NOT EXISTS error TEXT"))
        conn.execute(text("ALTER TABLE fred_series_refreshes ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now()"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_fred_observations_series_date ON fred_observations (series_id, observation_date)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_fred_observations_fetched_at ON fred_observations (fetched_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_fred_series_refreshes_refreshed_at ON fred_series_refreshes (last_refreshed_at)"))


def ensure_confirmation_monitoring_snapshot_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        if dialect_name == "sqlite":
            existing_columns = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(confirmation_monitoring_snapshots)")).fetchall()
                if len(row) > 1
            }
            if existing_columns and "source_states_json" not in existing_columns:
                conn.execute(text("ALTER TABLE confirmation_monitoring_snapshots ADD COLUMN source_states_json TEXT NOT NULL DEFAULT '{}'"))
            return
        conn.execute(text("ALTER TABLE confirmation_monitoring_snapshots ADD COLUMN IF NOT EXISTS source_states_json TEXT NOT NULL DEFAULT '{}'"))


def ensure_macro_positioning_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        timestamp_type = "TIMESTAMP" if dialect_name == "sqlite" else "TIMESTAMPTZ"
        float_int_pk = "TEXT PRIMARY KEY"
        now_default = "CURRENT_TIMESTAMP" if dialect_name == "sqlite" else "now()"
        false_default = "0" if dialect_name == "sqlite" else "false"
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS macro_positioning_assets (
                    asset_key {float_int_pk},
                    display_name TEXT NOT NULL,
                    bias TEXT NOT NULL DEFAULT 'neutral',
                    rating INTEGER NOT NULL DEFAULT 3,
                    positioning_date DATE NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{{}}',
                    fetched_at {timestamp_type} NOT NULL,
                    updated_at {timestamp_type} DEFAULT {now_default}
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS macro_positioning_cache (
                    symbol TEXT PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'ok',
                    overall TEXT NOT NULL DEFAULT 'neutral',
                    rating INTEGER NOT NULL DEFAULT 3,
                    summary TEXT NOT NULL,
                    drivers_json TEXT NOT NULL DEFAULT '[]',
                    mapped_sector TEXT,
                    mapped_asset_class TEXT,
                    updated DATE NOT NULL,
                    generated_at {timestamp_type} NOT NULL,
                    source_refresh_at {timestamp_type},
                    updated_at {timestamp_type} DEFAULT {now_default}
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS macro_positioning_feed_events (
                    event_id TEXT PRIMARY KEY,
                    report_date DATE NOT NULL,
                    market_id TEXT NOT NULL,
                    market_name TEXT NOT NULL,
                    market_group TEXT NOT NULL,
                    positioning TEXT NOT NULL,
                    crowded BOOLEAN NOT NULL DEFAULT {false_default},
                    weekly_change TEXT,
                    percentile DOUBLE PRECISION,
                    trend TEXT,
                    trend_weeks INTEGER,
                    event_kind TEXT NOT NULL,
                    insight TEXT,
                    summary TEXT,
                    significance INTEGER NOT NULL DEFAULT 0,
                    is_summary BOOLEAN NOT NULL DEFAULT {false_default},
                    generated_at {timestamp_type} NOT NULL,
                    updated_at {timestamp_type} DEFAULT {now_default}
                )
                """
            )
        )
        for statement in (
            "ALTER TABLE macro_positioning_assets ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE macro_positioning_assets ADD COLUMN IF NOT EXISTS bias TEXT NOT NULL DEFAULT 'neutral'",
            "ALTER TABLE macro_positioning_assets ADD COLUMN IF NOT EXISTS rating INTEGER NOT NULL DEFAULT 3",
            "ALTER TABLE macro_positioning_assets ADD COLUMN IF NOT EXISTS positioning_date DATE",
            "ALTER TABLE macro_positioning_assets ADD COLUMN IF NOT EXISTS payload_json TEXT NOT NULL DEFAULT '{}'",
            f"ALTER TABLE macro_positioning_assets ADD COLUMN IF NOT EXISTS fetched_at {timestamp_type}",
            f"ALTER TABLE macro_positioning_assets ADD COLUMN IF NOT EXISTS updated_at {timestamp_type} DEFAULT {now_default}",
            "ALTER TABLE macro_positioning_cache ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'ok'",
            "ALTER TABLE macro_positioning_cache ADD COLUMN IF NOT EXISTS overall TEXT NOT NULL DEFAULT 'neutral'",
            "ALTER TABLE macro_positioning_cache ADD COLUMN IF NOT EXISTS rating INTEGER NOT NULL DEFAULT 3",
            "ALTER TABLE macro_positioning_cache ADD COLUMN IF NOT EXISTS summary TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE macro_positioning_cache ADD COLUMN IF NOT EXISTS drivers_json TEXT NOT NULL DEFAULT '[]'",
            "ALTER TABLE macro_positioning_cache ADD COLUMN IF NOT EXISTS mapped_sector TEXT",
            "ALTER TABLE macro_positioning_cache ADD COLUMN IF NOT EXISTS mapped_asset_class TEXT",
            "ALTER TABLE macro_positioning_cache ADD COLUMN IF NOT EXISTS updated DATE",
            f"ALTER TABLE macro_positioning_cache ADD COLUMN IF NOT EXISTS generated_at {timestamp_type}",
            f"ALTER TABLE macro_positioning_cache ADD COLUMN IF NOT EXISTS source_refresh_at {timestamp_type}",
            f"ALTER TABLE macro_positioning_cache ADD COLUMN IF NOT EXISTS updated_at {timestamp_type} DEFAULT {now_default}",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS report_date DATE",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS market_id TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS market_name TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS market_group TEXT NOT NULL DEFAULT 'other'",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS positioning TEXT NOT NULL DEFAULT 'unavailable'",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS crowded BOOLEAN NOT NULL DEFAULT false",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS weekly_change TEXT",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS percentile DOUBLE PRECISION",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS trend TEXT",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS trend_weeks INTEGER",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS event_kind TEXT NOT NULL DEFAULT 'current_state'",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS insight TEXT",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS summary TEXT",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS significance INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS is_summary BOOLEAN NOT NULL DEFAULT false",
            f"ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS generated_at {timestamp_type}",
            f"ALTER TABLE macro_positioning_feed_events ADD COLUMN IF NOT EXISTS updated_at {timestamp_type} DEFAULT {now_default}",
        ):
            if dialect_name != "sqlite":
                conn.execute(text(statement))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_macro_positioning_assets_asset_key ON macro_positioning_assets (asset_key)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_macro_positioning_assets_positioning_date ON macro_positioning_assets (positioning_date)"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_macro_positioning_cache_symbol ON macro_positioning_cache (symbol)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_macro_positioning_cache_updated ON macro_positioning_cache (updated)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_macro_positioning_feed_events_report_date ON macro_positioning_feed_events (report_date)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_macro_positioning_feed_events_market_id ON macro_positioning_feed_events (market_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_macro_positioning_feed_events_event_kind ON macro_positioning_feed_events (event_kind)"))


def ensure_ticker_financials_cache_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        timestamp_type = "TIMESTAMPTZ" if dialect_name != "sqlite" else "TIMESTAMP"
        timestamp_default = "now()" if dialect_name != "sqlite" else "CURRENT_TIMESTAMP"
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS ticker_financials_cache (
                    symbol TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    fetched_at {timestamp_type} NOT NULL,
                    updated_at {timestamp_type} DEFAULT {timestamp_default}
                )
                """
            )
        )
        if dialect_name != "sqlite":
            for column, spec in {
                "status": "TEXT NOT NULL DEFAULT 'ok'",
                "payload_json": "TEXT NOT NULL DEFAULT '{}'",
                "fetched_at": "TIMESTAMPTZ NOT NULL DEFAULT now()",
                "updated_at": "TIMESTAMPTZ DEFAULT now()",
            }.items():
                conn.execute(text(f"ALTER TABLE ticker_financials_cache ADD COLUMN IF NOT EXISTS {column} {spec}"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ticker_financials_cache_fetched_at ON ticker_financials_cache (fetched_at)"))


def ensure_ticker_content_cache_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        timestamp_type = "TIMESTAMPTZ" if dialect_name != "sqlite" else "TIMESTAMP"
        timestamp_default = "now()" if dialect_name != "sqlite" else "CURRENT_TIMESTAMP"
        id_type = "BIGSERIAL PRIMARY KEY" if dialect_name != "sqlite" else "INTEGER PRIMARY KEY AUTOINCREMENT"
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS ticker_content_cache (
                    id {id_type},
                    content_type TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    window_key TEXT NOT NULL DEFAULT 'latest',
                    cache_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    item_count INTEGER NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'fmp',
                    fetched_at {timestamp_type} NOT NULL,
                    updated_at {timestamp_type} DEFAULT {timestamp_default}
                )
                """
            )
        )
        if dialect_name != "sqlite":
            for column, spec in {
                "window_key": "TEXT NOT NULL DEFAULT 'latest'",
                "cache_key": "TEXT NOT NULL DEFAULT ''",
                "status": "TEXT NOT NULL DEFAULT 'ok'",
                "item_count": "INTEGER NOT NULL DEFAULT 0",
                "payload_json": "TEXT NOT NULL DEFAULT '{}'",
                "source": "TEXT NOT NULL DEFAULT 'fmp'",
                "fetched_at": "TIMESTAMPTZ NOT NULL DEFAULT now()",
                "updated_at": "TIMESTAMPTZ DEFAULT now()",
            }.items():
                conn.execute(text(f"ALTER TABLE ticker_content_cache ADD COLUMN IF NOT EXISTS {column} {spec}"))
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_ticker_content_cache_type_symbol_window "
                "ON ticker_content_cache (content_type, symbol, window_key)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_ticker_content_cache_symbol_type "
                "ON ticker_content_cache (symbol, content_type)"
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ticker_content_cache_fetched_at ON ticker_content_cache (fetched_at)"))


def ensure_provider_usage_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        created_at_type = "TIMESTAMPTZ" if dialect_name != "sqlite" else "TIMESTAMP"
        bool_type = "BOOLEAN" if dialect_name != "sqlite" else "BOOLEAN"
        default_true = "true" if dialect_name != "sqlite" else "1"
        default_false = "false" if dialect_name != "sqlite" else "0"
        id_type = "BIGSERIAL PRIMARY KEY" if dialect_name != "sqlite" else "INTEGER PRIMARY KEY AUTOINCREMENT"
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS provider_usage_events (
                    id {id_type},
                    provider TEXT NOT NULL DEFAULT 'fmp',
                    category TEXT,
                    endpoint TEXT,
                    symbol TEXT,
                    source TEXT,
                    route TEXT,
                    cache_status TEXT,
                    status_code TEXT,
                    duration_ms DOUBLE PRECISION,
                    success {bool_type} NOT NULL DEFAULT {default_true},
                    throttled {bool_type} NOT NULL DEFAULT {default_false},
                    error TEXT,
                    created_at {created_at_type} DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        if dialect_name != "sqlite":
            for column, spec in {
                "endpoint": "TEXT",
                "source": "TEXT",
                "cache_status": "TEXT",
                "status_code": "TEXT",
                "duration_ms": "DOUBLE PRECISION",
                "success": "BOOLEAN NOT NULL DEFAULT true",
                "throttled": "BOOLEAN NOT NULL DEFAULT false",
                "error": "TEXT",
            }.items():
                conn.execute(text(f"ALTER TABLE provider_usage_events ADD COLUMN IF NOT EXISTS {column} {spec}"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_provider_usage_provider_created ON provider_usage_events (provider, created_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_provider_usage_category_created ON provider_usage_events (category, created_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_provider_usage_source_created ON provider_usage_events (source, created_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_provider_usage_throttled_created ON provider_usage_events (throttled, created_at)"))

def ensure_provider_control_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        created_at_type = "TIMESTAMPTZ" if dialect_name != "sqlite" else "TIMESTAMP"
        id_type = "BIGSERIAL PRIMARY KEY" if dialect_name != "sqlite" else "INTEGER PRIMARY KEY AUTOINCREMENT"
        bool_type = "BOOLEAN"
        default_true = "true" if dialect_name != "sqlite" else "1"
        default_false = "false" if dialect_name != "sqlite" else "0"
        float_type = "DOUBLE PRECISION" if dialect_name != "sqlite" else "FLOAT"

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS provider_settings (
                    id {id_type},
                    domain_key TEXT NOT NULL,
                    active_provider TEXT NOT NULL,
                    fallback_provider TEXT,
                    primary_endpoint_url TEXT,
                    fallback_endpoint_url TEXT,
                    primary_endpoint_contract_json TEXT,
                    fallback_endpoint_contract_json TEXT,
                    mode TEXT NOT NULL DEFAULT 'primary',
                    is_enabled {bool_type} NOT NULL DEFAULT {default_true},
                    allow_external_live_fetch {bool_type} NOT NULL DEFAULT {default_false},
                    allow_user_route_sync_fetch {bool_type} NOT NULL DEFAULT {default_false},
                    builder_safe_required {bool_type} NOT NULL DEFAULT {default_true},
                    notes TEXT,
                    updated_by TEXT,
                    created_at {created_at_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {created_at_type} DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS provider_setting_audit_log (
                    id {id_type},
                    domain_key TEXT NOT NULL,
                    previous_provider TEXT,
                    new_provider TEXT,
                    previous_mode TEXT,
                    new_mode TEXT,
                    changed_by TEXT,
                    changed_at {created_at_type} DEFAULT CURRENT_TIMESTAMP,
                    reason TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS symbol_resolution_overrides (
                    id {id_type},
                    domain_key TEXT NOT NULL,
                    raw_symbol TEXT,
                    issuer_name TEXT,
                    normalized_symbol TEXT,
                    asset_type TEXT,
                    reason TEXT,
                    updated_by TEXT,
                    updated_at {created_at_type} DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS congress_disclosure_filings (
                    id {id_type},
                    source_provider TEXT NOT NULL,
                    chamber TEXT,
                    filing_id TEXT NOT NULL,
                    source_url TEXT,
                    document_url TEXT,
                    document_hash TEXT,
                    member_name_raw TEXT,
                    member_id TEXT,
                    filing_date DATE,
                    report_type TEXT,
                    amendment_flag {bool_type} NOT NULL DEFAULT {default_false},
                    raw_metadata_json TEXT NOT NULL DEFAULT '{{}}',
                    raw_text_path TEXT,
                    raw_blob_ref TEXT,
                    parser_status TEXT NOT NULL DEFAULT 'pending',
                    parser_version TEXT,
                    parser_confidence {float_type},
                    last_parse_error TEXT,
                    discovered_at {created_at_type} DEFAULT CURRENT_TIMESTAMP,
                    parsed_at {created_at_type},
                    created_at {created_at_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {created_at_type} DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS congress_transactions_normalized (
                    id {id_type},
                    filing_id TEXT,
                    disclosure_filing_id INTEGER,
                    source_provider TEXT NOT NULL,
                    chamber TEXT,
                    member_name_raw TEXT,
                    member_id TEXT,
                    owner_raw TEXT,
                    owner_normalized TEXT NOT NULL DEFAULT 'unknown',
                    transaction_date DATE,
                    disclosure_date DATE,
                    ticker_raw TEXT,
                    ticker_normalized TEXT,
                    issuer_name_raw TEXT,
                    security_name_raw TEXT,
                    asset_type_raw TEXT,
                    asset_type_normalized TEXT,
                    transaction_type_raw TEXT,
                    transaction_type_normalized TEXT,
                    amount_range_raw TEXT,
                    amount_low {float_type},
                    amount_high {float_type},
                    source_url TEXT,
                    document_url TEXT,
                    source_line_ref TEXT,
                    normalized_hash TEXT NOT NULL,
                    is_duplicate {bool_type} NOT NULL DEFAULT {default_false},
                    duplicate_of_id INTEGER,
                    symbol_resolution_status TEXT NOT NULL DEFAULT 'unresolved',
                    parser_confidence {float_type},
                    created_at {created_at_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {created_at_type} DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS sec_form4_filings (
                    id {id_type},
                    accession_number TEXT NOT NULL,
                    issuer_cik TEXT,
                    issuer_name TEXT,
                    issuer_trading_symbol TEXT,
                    reporting_owner_cik TEXT,
                    reporting_owner_name TEXT,
                    filing_date DATE,
                    source_url TEXT,
                    xml_url TEXT,
                    document_hash TEXT,
                    raw_metadata_json TEXT NOT NULL DEFAULT '{{}}',
                    raw_xml_path TEXT,
                    raw_xml_text TEXT,
                    parser_status TEXT NOT NULL DEFAULT 'pending',
                    parser_version TEXT,
                    parser_confidence {float_type},
                    last_parse_error TEXT,
                    discovered_at {created_at_type} DEFAULT CURRENT_TIMESTAMP,
                    parsed_at {created_at_type},
                    created_at {created_at_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {created_at_type} DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS insider_transactions_normalized (
                    id {id_type},
                    form4_filing_id INTEGER,
                    accession_number TEXT NOT NULL,
                    issuer_cik TEXT,
                    issuer_name TEXT,
                    ticker_raw TEXT,
                    ticker_normalized TEXT,
                    reporting_owner_cik TEXT,
                    reporting_owner_name TEXT,
                    owner_relationship_json TEXT NOT NULL DEFAULT '{{}}',
                    officer_title TEXT,
                    is_director {bool_type} NOT NULL DEFAULT {default_false},
                    is_officer {bool_type} NOT NULL DEFAULT {default_false},
                    is_ten_percent_owner {bool_type} NOT NULL DEFAULT {default_false},
                    transaction_date DATE,
                    filing_date DATE,
                    security_title TEXT,
                    transaction_code TEXT,
                    transaction_code_description TEXT,
                    transaction_type_normalized TEXT,
                    shares {float_type},
                    price {float_type},
                    value {float_type},
                    acquired_disposed TEXT,
                    shares_owned_following {float_type},
                    direct_or_indirect TEXT,
                    ownership_nature TEXT,
                    is_derivative {bool_type} NOT NULL DEFAULT {default_false},
                    footnotes_json TEXT NOT NULL DEFAULT '[]',
                    ten_b5_1_flag {bool_type} NOT NULL DEFAULT {default_false},
                    normalized_hash TEXT NOT NULL,
                    is_duplicate {bool_type} NOT NULL DEFAULT {default_false},
                    duplicate_of_id INTEGER,
                    parser_confidence {float_type},
                    created_at {created_at_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {created_at_type} DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        if dialect_name != "sqlite":
            table_columns = {
                "provider_settings": {
                    "fallback_provider": "TEXT",
                    "primary_endpoint_url": "TEXT",
                    "fallback_endpoint_url": "TEXT",
                    "primary_endpoint_contract_json": "TEXT",
                    "fallback_endpoint_contract_json": "TEXT",
                    "mode": "TEXT NOT NULL DEFAULT 'primary'",
                    "is_enabled": "BOOLEAN NOT NULL DEFAULT true",
                    "allow_external_live_fetch": "BOOLEAN NOT NULL DEFAULT false",
                    "allow_user_route_sync_fetch": "BOOLEAN NOT NULL DEFAULT false",
                    "builder_safe_required": "BOOLEAN NOT NULL DEFAULT true",
                    "notes": "TEXT",
                    "updated_by": "TEXT",
                    "created_at": "TIMESTAMPTZ DEFAULT now()",
                    "updated_at": "TIMESTAMPTZ DEFAULT now()",
                },
                "events": {
                    "data_source": "TEXT",
                    "source_provider": "TEXT",
                    "source_filing_id": "TEXT",
                    "source_document_url": "TEXT",
                    "parser_version": "TEXT",
                    "provider_priority": "INTEGER",
                },
            }
            existing_columns = {
                table_name: set()
                for table_name in table_columns
            }
            rows = conn.execute(
                text(
                    """
                    SELECT table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name IN ('provider_settings', 'events')
                    """
                )
            ).fetchall()
            for table_name, column_name in rows:
                if table_name in existing_columns:
                    existing_columns[table_name].add(column_name)
            for table_name, columns in table_columns.items():
                for column, spec in columns.items():
                    if column not in existing_columns.get(table_name, set()):
                        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column} {spec}"))
        else:
            provider_settings_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='provider_settings'")
            ).fetchone()
            if provider_settings_exists:
                provider_existing = {
                    row[1]
                    for row in conn.execute(text("PRAGMA table_info(provider_settings)")).fetchall()
                    if len(row) > 1
                }
                for name, column_type in {
                    "primary_endpoint_url": "TEXT",
                    "fallback_endpoint_url": "TEXT",
                    "primary_endpoint_contract_json": "TEXT",
                    "fallback_endpoint_contract_json": "TEXT",
                }.items():
                    if name not in provider_existing:
                        conn.execute(text(f"ALTER TABLE provider_settings ADD COLUMN {name} {column_type}"))

            table_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='events'")
            ).fetchone()
            if table_exists:
                existing = {
                    row[1]
                    for row in conn.execute(text("PRAGMA table_info(events)")).fetchall()
                    if len(row) > 1
                }
                for name, column_type in {
                    "data_source": "TEXT",
                    "source_provider": "TEXT",
                    "source_filing_id": "TEXT",
                    "source_document_url": "TEXT",
                    "parser_version": "TEXT",
                    "provider_priority": "INTEGER",
                }.items():
                    if name not in existing:
                        conn.execute(text(f"ALTER TABLE events ADD COLUMN {name} {column_type}"))

        indexes = (
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_provider_settings_domain_key ON provider_settings (domain_key)",
            "CREATE INDEX IF NOT EXISTS ix_provider_settings_mode ON provider_settings (mode)",
            "CREATE INDEX IF NOT EXISTS ix_provider_setting_audit_domain_changed ON provider_setting_audit_log (domain_key, changed_at)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_symbol_resolution_override_lookup ON symbol_resolution_overrides (domain_key, raw_symbol, issuer_name)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_congress_disclosure_source_filing ON congress_disclosure_filings (source_provider, filing_id)",
            "CREATE INDEX IF NOT EXISTS ix_congress_disclosure_chamber_date ON congress_disclosure_filings (chamber, filing_date)",
            "CREATE INDEX IF NOT EXISTS ix_congress_disclosure_parser_status ON congress_disclosure_filings (parser_status)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_congress_txn_normalized_hash ON congress_transactions_normalized (normalized_hash)",
            "CREATE INDEX IF NOT EXISTS ix_congress_txn_symbol_date ON congress_transactions_normalized (ticker_normalized, transaction_date)",
            "CREATE INDEX IF NOT EXISTS ix_congress_txn_member_date ON congress_transactions_normalized (member_id, transaction_date)",
            "CREATE INDEX IF NOT EXISTS ix_congress_txn_duplicate ON congress_transactions_normalized (is_duplicate)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_sec_form4_accession ON sec_form4_filings (accession_number)",
            "CREATE INDEX IF NOT EXISTS ix_sec_form4_issuer_filing_date ON sec_form4_filings (issuer_cik, filing_date)",
            "CREATE INDEX IF NOT EXISTS ix_sec_form4_parser_status ON sec_form4_filings (parser_status)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_insider_txn_normalized_hash ON insider_transactions_normalized (normalized_hash)",
            "CREATE INDEX IF NOT EXISTS ix_insider_txn_symbol_date ON insider_transactions_normalized (ticker_normalized, transaction_date)",
            "CREATE INDEX IF NOT EXISTS ix_insider_txn_owner_date ON insider_transactions_normalized (reporting_owner_cik, transaction_date)",
            "CREATE INDEX IF NOT EXISTS ix_insider_transactions_name_lower ON insider_transactions (lower(insider_name))",
            "CREATE INDEX IF NOT EXISTS ix_insider_txn_owner_name_lower ON insider_transactions_normalized (lower(reporting_owner_name))",
            "CREATE INDEX IF NOT EXISTS ix_insider_txn_duplicate ON insider_transactions_normalized (is_duplicate)",
        )
        for statement in indexes:
            conn.execute(text(statement))


def ensure_data_enrichment_jobs_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn)
        created_at_type = "TIMESTAMPTZ" if dialect_name != "sqlite" else "TIMESTAMP"
        id_type = "BIGSERIAL PRIMARY KEY" if dialect_name != "sqlite" else "INTEGER PRIMARY KEY AUTOINCREMENT"
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS data_enrichment_jobs (
                    id {id_type},
                    job_type TEXT NOT NULL,
                    symbol TEXT,
                    date_key TEXT,
                    window_key TEXT,
                    dedupe_key TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 100,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 5,
                    source TEXT,
                    reason TEXT,
                    error TEXT,
                    payload_json TEXT,
                    next_run_at {created_at_type} DEFAULT CURRENT_TIMESTAMP,
                    created_at {created_at_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {created_at_type} DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        if dialect_name != "sqlite":
            for column, spec in {
                "date_key": "TEXT",
                "window_key": "TEXT",
                "priority": "INTEGER NOT NULL DEFAULT 100",
                "attempts": "INTEGER NOT NULL DEFAULT 0",
                "max_attempts": "INTEGER NOT NULL DEFAULT 5",
                "source": "TEXT",
                "reason": "TEXT",
                "error": "TEXT",
                "payload_json": "TEXT",
                "next_run_at": "TIMESTAMPTZ DEFAULT now()",
                "created_at": "TIMESTAMPTZ DEFAULT now()",
                "updated_at": "TIMESTAMPTZ DEFAULT now()",
            }.items():
                conn.execute(text(f"ALTER TABLE data_enrichment_jobs ADD COLUMN IF NOT EXISTS {column} {spec}"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_data_enrichment_jobs_dedupe_key ON data_enrichment_jobs (dedupe_key)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_data_enrichment_jobs_type_status ON data_enrichment_jobs (job_type, status)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_data_enrichment_jobs_symbol ON data_enrichment_jobs (symbol)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_data_enrichment_jobs_status_next_run ON data_enrichment_jobs (status, next_run_at)"))


def ensure_institutional_activity_schema(bind=engine) -> None:
    from app.models import (
        InstitutionalActivityEvent,
        InstitutionalFiling,
        InstitutionalHolder,
        InstitutionalHolderIndustryBreakdown,
        InstitutionalHolderPerformanceMetric,
        InstitutionalIndustrySummary,
        InstitutionalIngestJobRun,
        InstitutionalIngestJobState,
        InstitutionalPosition,
        InstitutionalPositionChange,
        InstitutionalSymbolSummary,
    )

    tables = [
        InstitutionalHolder.__table__,
        InstitutionalFiling.__table__,
        InstitutionalPosition.__table__,
        InstitutionalPositionChange.__table__,
        InstitutionalSymbolSummary.__table__,
        InstitutionalActivityEvent.__table__,
        InstitutionalIndustrySummary.__table__,
        InstitutionalHolderIndustryBreakdown.__table__,
        InstitutionalHolderPerformanceMetric.__table__,
        InstitutionalIngestJobState.__table__,
        InstitutionalIngestJobRun.__table__,
    ]
    with bind.begin() as conn:
        _set_postgres_ddl_timeouts(conn)
        # This project rolls additive schema forward at startup; keep 13F tables
        # on the same create-if-missing path unless a formal migration system is adopted.
        logger.info("institutional_activity_schema_ensure_start table_count=%s", len(tables))
        Base.metadata.create_all(bind=conn, tables=tables)
        for index_name, statement in (
            (
                "ix_institutional_holders_holder_name_lower",
                "CREATE INDEX IF NOT EXISTS ix_institutional_holders_holder_name_lower ON institutional_holders (lower(holder_name))",
            ),
            (
                "ix_institutional_holders_cik_lower",
                "CREATE INDEX IF NOT EXISTS ix_institutional_holders_cik_lower ON institutional_holders (lower(cik))",
            ),
            (
                "ix_institutional_transactions_name_lower",
                "CREATE INDEX IF NOT EXISTS ix_institutional_transactions_name_lower ON institutional_transactions (lower(institution_name))",
            ),
        ):
            try:
                with conn.begin_nested():
                    conn.execute(text(statement))
            except SQLAlchemyError as exc:
                logger.warning(
                    "institutional_activity_optional_index_skipped index=%s reason=%s",
                    index_name,
                    _optional_index_skip_reason(exc),
                )
        try:
            with conn.begin_nested():
                if bind.dialect.name == "postgresql":
                    conn.execute(text("ALTER TABLE institutional_ingest_job_state ADD COLUMN IF NOT EXISTS metadata_json TEXT"))
                else:
                    existing = {
                        row[1]
                        for row in conn.execute(text("PRAGMA table_info(institutional_ingest_job_state)")).fetchall()
                        if len(row) > 1
                    }
                    if "metadata_json" not in existing:
                        conn.execute(text("ALTER TABLE institutional_ingest_job_state ADD COLUMN metadata_json TEXT"))
        except SQLAlchemyError as exc:
            logger.warning(
                "institutional_activity_optional_column_skipped table=institutional_ingest_job_state column=metadata_json reason=%s",
                _optional_index_skip_reason(exc),
            )
        logger.info("institutional_activity_schema_ensure_complete table_count=%s", len(tables))


def ensure_event_columns() -> None:
    if not DATABASE_URL.startswith("sqlite"):
        return
    with engine.begin() as conn:
        table_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='events'")
        ).fetchone()
        if not table_exists:
            return
        existing = {
            row[1]
            for row in conn.execute(text("PRAGMA table_info(events)")).fetchall()
            if len(row) > 1
        }
        columns = {
            "member_name": "TEXT",
            "member_bioguide_id": "TEXT",
            "chamber": "TEXT",
            "party": "TEXT",
            "transaction_type": "TEXT",
            "trade_type": "TEXT",
            "amount_min": "INTEGER",
            "amount_max": "INTEGER",
            "symbol": "TEXT",
            "event_date": "TIMESTAMP",
        }
        for name, column_type in columns.items():
            if name not in existing:
                conn.execute(text(f"ALTER TABLE events ADD COLUMN {name} {column_type}"))
        if "ticker" in existing:
            conn.execute(
                text(
                    "UPDATE events "
                    "SET symbol = UPPER(ticker) "
                    "WHERE (symbol IS NULL OR symbol = '') AND ticker IS NOT NULL"
                )
            )
        conn.execute(
            text(
                "UPDATE events "
                "SET event_date = ts "
                "WHERE event_date IS NULL AND ts IS NOT NULL"
            )
        )
        conn.execute(
            text(
                "UPDATE events "
                "SET trade_type = transaction_type "
                "WHERE trade_type IS NULL AND transaction_type IS NOT NULL"
            )
        )
        conn.execute(
            text("CREATE INDEX IF NOT EXISTS ix_events_event_date ON events (event_date)")
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_events_ts ON events (ts)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_events_symbol ON events (symbol)"))
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_events_member_bioguide_id "
                "ON events (member_bioguide_id)"
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_events_party ON events (party)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_events_chamber ON events (chamber)"))
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_events_trade_type "
                "ON events (trade_type)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_events_symbol_event_date "
                "ON events (symbol, event_date)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_events_symbol_ts "
                "ON events (symbol, ts)"
            )
        )
        trade_outcomes_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_outcomes'")
        ).fetchone()
        if trade_outcomes_exists:
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_trade_outcomes_benchmark_trade_date_member "
                    "ON trade_outcomes (benchmark_symbol, trade_date, member_id)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_trade_outcomes_benchmark_status_trade_date_member "
                    "ON trade_outcomes (benchmark_symbol, scoring_status, trade_date, member_id)"
                )
            )
        congress_aliases_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='congress_member_aliases'")
        ).fetchone()
        if congress_aliases_exists:
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_congress_member_aliases_group_key "
                    "ON congress_member_aliases (group_key)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_congress_member_aliases_chamber_group_key "
                    "ON congress_member_aliases (chamber, group_key)"
                )
            )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS watchlist_view_states (
                    watchlist_id INTEGER PRIMARY KEY,
                    last_seen_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        watchlists_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='watchlists'")
        ).fetchone()
        if watchlists_exists:
            existing_watchlist_columns = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(watchlists)")).fetchall()
                if len(row) > 1
            }
            if "owner_user_id" not in existing_watchlist_columns:
                conn.execute(text("ALTER TABLE watchlists ADD COLUMN owner_user_id INTEGER"))
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_watchlists_owner_user_id "
                    "ON watchlists (owner_user_id)"
                )
            )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_watchlist_view_states_last_seen_at "
                "ON watchlist_view_states (last_seen_at)"
            )
        )
        watchlist_items_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='watchlist_items'")
        ).fetchone()
        if watchlist_items_exists:
            existing_watchlist_item_columns = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(watchlist_items)")).fetchall()
                if len(row) > 1
            }
            if "target_type" not in existing_watchlist_item_columns:
                conn.execute(text("ALTER TABLE watchlist_items ADD COLUMN target_type TEXT NOT NULL DEFAULT 'ticker'"))
            if "target_value" not in existing_watchlist_item_columns:
                conn.execute(text("ALTER TABLE watchlist_items ADD COLUMN target_value TEXT"))
            if "target_label" not in existing_watchlist_item_columns:
                conn.execute(text("ALTER TABLE watchlist_items ADD COLUMN target_label TEXT"))
            conn.execute(
                text(
                    """
                    UPDATE watchlist_items
                    SET target_type = COALESCE(NULLIF(target_type, ''), 'ticker'),
                        target_value = COALESCE(
                            NULLIF(target_value, ''),
                            (
                                SELECT upper(securities.symbol)
                                FROM securities
                                WHERE securities.id = watchlist_items.security_id
                            )
                        )
                    WHERE security_id IS NOT NULL
                    """
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_watchlist_items_watchlist_security "
                    "ON watchlist_items (watchlist_id, security_id)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_watchlist_items_security_id "
                    "ON watchlist_items (security_id)"
                )
            )
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ix_watchlist_items_target_scope "
                    "ON watchlist_items (watchlist_id, target_type, target_value)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_watchlist_items_watchlist_target "
                    "ON watchlist_items (watchlist_id, target_type, target_value)"
                )
            )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS confirmation_monitoring_snapshots (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    watchlist_id INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    score INTEGER NOT NULL DEFAULT 0,
                    band TEXT NOT NULL DEFAULT 'inactive',
                    direction TEXT NOT NULL DEFAULT 'neutral',
                    source_count INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'Inactive',
                    source_states_json TEXT NOT NULL DEFAULT '{}',
                    observed_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        existing_confirmation_snapshot_columns = {
            row[1]
            for row in conn.execute(text("PRAGMA table_info(confirmation_monitoring_snapshots)")).fetchall()
            if len(row) > 1
        }
        if "source_states_json" not in existing_confirmation_snapshot_columns:
            conn.execute(text("ALTER TABLE confirmation_monitoring_snapshots ADD COLUMN source_states_json TEXT NOT NULL DEFAULT '{}'"))
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_confirmation_monitoring_snapshot_scope "
                "ON confirmation_monitoring_snapshots (user_id, watchlist_id, ticker)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_confirmation_monitoring_snapshot_observed "
                "ON confirmation_monitoring_snapshots (observed_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS confirmation_monitoring_events (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    watchlist_id INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    body TEXT,
                    score_before INTEGER,
                    score_after INTEGER NOT NULL DEFAULT 0,
                    band_before TEXT,
                    band_after TEXT NOT NULL,
                    direction_before TEXT,
                    direction_after TEXT NOT NULL,
                    source_count_before INTEGER,
                    source_count_after INTEGER NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_confirmation_monitoring_events_watchlist_created "
                "ON confirmation_monitoring_events (user_id, watchlist_id, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_confirmation_monitoring_events_dedupe "
                "ON confirmation_monitoring_events (user_id, watchlist_id, ticker, event_type, created_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS saved_screens (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    params_json TEXT NOT NULL DEFAULT '{}',
                    last_viewed_at TIMESTAMP,
                    last_refreshed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_saved_screens_user_updated "
                "ON saved_screens (user_id, updated_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_saved_screens_user_refreshed "
                "ON saved_screens (user_id, last_refreshed_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS saved_screen_snapshots (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    saved_screen_id INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    confirmation_score INTEGER NOT NULL DEFAULT 0,
                    confirmation_band TEXT NOT NULL DEFAULT 'inactive',
                    direction TEXT NOT NULL DEFAULT 'neutral',
                    source_count INTEGER NOT NULL DEFAULT 0,
                    why_now_state TEXT NOT NULL DEFAULT 'inactive',
                    observed_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_saved_screen_snapshots_scope "
                "ON saved_screen_snapshots (user_id, saved_screen_id, ticker)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_saved_screen_snapshots_screen_observed "
                "ON saved_screen_snapshots (saved_screen_id, observed_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS saved_screen_events (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    saved_screen_id INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    before_json TEXT,
                    after_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_saved_screen_events_user_created "
                "ON saved_screen_events (user_id, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_saved_screen_events_screen_created "
                "ON saved_screen_events (saved_screen_id, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_saved_screen_events_dedupe "
                "ON saved_screen_events (user_id, saved_screen_id, ticker, event_type, created_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS monitoring_alerts (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    event_id INTEGER NOT NULL,
                    alert_type TEXT NOT NULL,
                    symbol TEXT,
                    title TEXT NOT NULL,
                    body TEXT,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    event_created_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    read_at TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_monitoring_alert_source_event "
                "ON monitoring_alerts (user_id, source_type, source_id, event_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_monitoring_alerts_user_read "
                "ON monitoring_alerts (user_id, read_at, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_monitoring_alerts_source_read "
                "ON monitoring_alerts (user_id, source_type, source_id, read_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_monitoring_alerts_event_created "
                "ON monitoring_alerts (event_created_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS notification_subscriptions (
                    id INTEGER PRIMARY KEY,
                    email TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    source_payload_json TEXT,
                    frequency TEXT NOT NULL DEFAULT 'daily',
                    only_if_new BOOLEAN NOT NULL DEFAULT 1,
                    active BOOLEAN NOT NULL DEFAULT 1,
                    alert_triggers_json TEXT NOT NULL DEFAULT '[]',
                    min_smart_score INTEGER,
                    large_trade_amount INTEGER,
                    last_delivered_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_notification_subscriptions_source "
                "ON notification_subscriptions (source_type, source_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_notification_subscriptions_active_frequency "
                "ON notification_subscriptions (active, frequency)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS notification_deliveries (
                    id INTEGER PRIMARY KEY,
                    subscription_id INTEGER NOT NULL,
                    channel TEXT NOT NULL DEFAULT 'email',
                    status TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    body_text TEXT NOT NULL,
                    items_count INTEGER NOT NULL DEFAULT 0,
                    alerts_count INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    delivered_at TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_notification_deliveries_subscription_created "
                "ON notification_deliveries (subscription_id, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_notification_deliveries_status "
                "ON notification_deliveries (status)"
            )
        )
        user_accounts_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='user_accounts'")
        ).fetchone()
        if user_accounts_exists:
            existing_user_columns = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(user_accounts)")).fetchall()
                if len(row) > 1
            }
            user_columns = {
                "first_name": "TEXT",
                "last_name": "TEXT",
                "country": "TEXT",
                "state_province": "TEXT",
                "postal_code": "TEXT",
                "city": "TEXT",
                "address_line1": "TEXT",
                "address_line2": "TEXT",
                "auth_provider": "TEXT NOT NULL DEFAULT 'email'",
                "google_sub": "TEXT",
                "avatar_url": "TEXT",
                "password_hash": "TEXT",
                "password_reset_token_hash": "TEXT",
                "password_reset_expires_at": "TIMESTAMP",
                "alerts_enabled": "BOOLEAN NOT NULL DEFAULT 1",
                "email_notifications_enabled": "BOOLEAN NOT NULL DEFAULT 1",
                "watchlist_activity_notifications": "BOOLEAN NOT NULL DEFAULT 1",
                "signals_notifications": "BOOLEAN NOT NULL DEFAULT 1",
                "subscription_cancel_at_period_end": "BOOLEAN NOT NULL DEFAULT 0",
                "access_expires_at": "TIMESTAMP",
                "monthly_price_override": "INTEGER",
                "annual_price_override": "INTEGER",
                "override_currency": "TEXT",
                "override_note": "TEXT",
            }
            for name, column_type in user_columns.items():
                if name not in existing_user_columns:
                    conn.execute(text(f"ALTER TABLE user_accounts ADD COLUMN {name} {column_type}"))
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ix_user_accounts_google_sub "
                    "ON user_accounts (google_sub)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_user_accounts_created_at "
                    "ON user_accounts (created_at)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_user_accounts_last_seen_at "
                    "ON user_accounts (last_seen_at)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_user_accounts_subscription_status "
                    "ON user_accounts (subscription_status)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_user_accounts_manual_tier_override "
                    "ON user_accounts (manual_tier_override)"
                )
            )
            watchlists_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='watchlists'")
            ).fetchone()
            if watchlists_exists:
                legacy_owner_email = "moore11j@gmail.com"
                owner = conn.execute(
                    text("SELECT id FROM user_accounts WHERE lower(email) = :email"),
                    {"email": legacy_owner_email},
                ).fetchone()
                if owner is None:
                    conn.execute(
                        text(
                            "INSERT INTO user_accounts "
                            "(email, name, auth_provider, role, entitlement_tier, last_seen_at) "
                            "VALUES (:email, :name, 'email', 'admin', 'free', CURRENT_TIMESTAMP)"
                        ),
                        {"email": legacy_owner_email, "name": "Moore"},
                    )
                    owner = conn.execute(
                        text("SELECT id FROM user_accounts WHERE lower(email) = :email"),
                        {"email": legacy_owner_email},
                    ).fetchone()
                if owner is not None:
                    conn.execute(
                        text(
                            "UPDATE watchlists "
                            "SET owner_user_id = :owner_user_id "
                            "WHERE owner_user_id IS NULL"
                        ),
                        {"owner_user_id": owner[0]},
                    )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS billing_transactions (
                    id INTEGER PRIMARY KEY,
                    stripe_customer_id TEXT,
                    stripe_subscription_id TEXT,
                    stripe_invoice_id TEXT,
                    stripe_payment_intent_id TEXT,
                    stripe_charge_id TEXT,
                    user_id INTEGER,
                    customer_name TEXT,
                    customer_email TEXT,
                    billing_country TEXT,
                    billing_state_province TEXT,
                    billing_postal_code TEXT,
                    description TEXT,
                    billing_period_type TEXT,
                    service_period_start TIMESTAMP,
                    service_period_end TIMESTAMP,
                    subtotal_amount INTEGER,
                    tax_amount INTEGER,
                    total_amount INTEGER,
                    currency TEXT,
                    charged_at TIMESTAMP,
                    payment_status TEXT,
                    access_expires_at TIMESTAMP,
                    refund_status TEXT,
                    tax_breakdown_json TEXT,
                    payload_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_billing_transactions_invoice "
                "ON billing_transactions (stripe_invoice_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_billing_transactions_user_charged "
                "ON billing_transactions (user_id, charged_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_billing_transactions_customer "
                "ON billing_transactions (stripe_customer_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_billing_transactions_subscription "
                "ON billing_transactions (stripe_subscription_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_billing_transactions_charged_at "
                "ON billing_transactions (charged_at)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS plan_limits (
                    tier TEXT NOT NULL,
                    feature_key TEXT NOT NULL,
                    limit_value INTEGER NOT NULL DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (tier, feature_key)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS plan_prices (
                    tier TEXT NOT NULL,
                    billing_interval TEXT NOT NULL,
                    amount_cents INTEGER NOT NULL DEFAULT 0,
                    currency TEXT NOT NULL DEFAULT 'USD',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (tier, billing_interval)
                )
                """
            )
        )


def ensure_watchlist_item_target_schema(bind=engine) -> None:
    with bind.begin() as conn:
        if conn.dialect.name == "sqlite":
            exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='watchlist_items'")
            ).fetchone()
            if not exists:
                return
            existing = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(watchlist_items)")).fetchall()
                if len(row) > 1
            }
            if "target_type" not in existing:
                conn.execute(text("ALTER TABLE watchlist_items ADD COLUMN target_type TEXT NOT NULL DEFAULT 'ticker'"))
            if "target_value" not in existing:
                conn.execute(text("ALTER TABLE watchlist_items ADD COLUMN target_value TEXT"))
            if "target_label" not in existing:
                conn.execute(text("ALTER TABLE watchlist_items ADD COLUMN target_label TEXT"))
            conn.execute(
                text(
                    """
                    UPDATE watchlist_items
                    SET target_type = COALESCE(NULLIF(target_type, ''), 'ticker'),
                        target_value = COALESCE(
                            NULLIF(target_value, ''),
                            (
                                SELECT upper(securities.symbol)
                                FROM securities
                                WHERE securities.id = watchlist_items.security_id
                            )
                        )
                    WHERE security_id IS NOT NULL
                    """
                )
            )
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_watchlist_items_target_scope ON watchlist_items (watchlist_id, target_type, target_value)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_watchlist_items_watchlist_target ON watchlist_items (watchlist_id, target_type, target_value)"))
            return

        if conn.dialect.name == "postgresql":
            _set_postgres_ddl_timeouts(conn)
            exists = conn.execute(text("SELECT to_regclass('public.watchlist_items')")).scalar_one_or_none()
            if not exists:
                return
            conn.execute(text("ALTER TABLE watchlist_items ADD COLUMN IF NOT EXISTS target_type TEXT NOT NULL DEFAULT 'ticker'"))
            conn.execute(text("ALTER TABLE watchlist_items ADD COLUMN IF NOT EXISTS target_value TEXT"))
            conn.execute(text("ALTER TABLE watchlist_items ADD COLUMN IF NOT EXISTS target_label TEXT"))
            conn.execute(text("ALTER TABLE watchlist_items ALTER COLUMN security_id DROP NOT NULL"))
            conn.execute(
                text(
                    """
                    UPDATE watchlist_items wi
                    SET target_type = COALESCE(NULLIF(wi.target_type, ''), 'ticker'),
                        target_value = COALESCE(NULLIF(wi.target_value, ''), upper(securities.symbol))
                    FROM securities
                    WHERE wi.security_id = securities.id
                    """
                )
            )
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_watchlist_items_target_scope ON watchlist_items (watchlist_id, target_type, target_value)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_watchlist_items_watchlist_target ON watchlist_items (watchlist_id, target_type, target_value)"))


def ensure_monitoring_alert_columns() -> None:
    with engine.begin() as conn:
        _set_postgres_ddl_timeouts(conn)
        if DATABASE_URL.startswith("sqlite"):
            table_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='monitoring_alerts'")
            ).fetchone()
        else:
            table_exists = conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_name = 'monitoring_alerts'"
                )
            ).fetchone()
        if not table_exists:
            return
        if DATABASE_URL.startswith("sqlite"):
            existing = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(monitoring_alerts)")).fetchall()
                if len(row) > 1
            }
            if "dismissed_at" not in existing:
                conn.execute(text("ALTER TABLE monitoring_alerts ADD COLUMN dismissed_at TIMESTAMP"))
        else:
            conn.execute(text("ALTER TABLE monitoring_alerts ADD COLUMN IF NOT EXISTS dismissed_at TIMESTAMP"))
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_monitoring_alerts_user_dismissed "
                "ON monitoring_alerts (user_id, dismissed_at, created_at)"
            )
        )


def ensure_house_annual_disclosure_schema() -> None:
    with engine.begin() as conn:
        _set_postgres_ddl_timeouts(conn)
        if DATABASE_URL.startswith("sqlite"):
            positions_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='replicated_portfolio_positions'")
            ).fetchone()
            if positions_exists:
                existing_position_columns = {
                    row[1]
                    for row in conn.execute(text("PRAGMA table_info(replicated_portfolio_positions)")).fetchall()
                    if len(row) > 1
                }
                for name in ("source_type", "source_reason", "confidence", "source_document_id", "source_url"):
                    if name not in existing_position_columns:
                        conn.execute(text(f"ALTER TABLE replicated_portfolio_positions ADD COLUMN {name} TEXT"))

            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS house_annual_disclosure_documents (
                        id INTEGER PRIMARY KEY,
                        source TEXT NOT NULL DEFAULT 'house_clerk_financial_disclosure',
                        member_name TEXT NOT NULL,
                        member_bioguide_id TEXT,
                        filing_year INTEGER NOT NULL,
                        filing_type TEXT,
                        report_url TEXT,
                        document_id TEXT NOT NULL,
                        filing_date DATE,
                        state_district TEXT,
                        payload_json TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS house_annual_disclosure_holdings (
                        id INTEGER PRIMARY KEY,
                        document_row_id INTEGER NOT NULL,
                        source TEXT NOT NULL DEFAULT 'house_clerk_financial_disclosure',
                        member_name TEXT NOT NULL,
                        member_bioguide_id TEXT,
                        filing_year INTEGER NOT NULL,
                        filing_type TEXT,
                        filing_date DATE,
                        report_url TEXT,
                        document_id TEXT NOT NULL,
                        asset_name TEXT NOT NULL,
                        symbol TEXT,
                        owner TEXT,
                        asset_type TEXT,
                        value_range TEXT,
                        value_min REAL,
                        value_max REAL,
                        income_type TEXT,
                        income_range TEXT,
                        payload_json TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
        else:
            conn.execute(text("ALTER TABLE replicated_portfolio_positions ADD COLUMN IF NOT EXISTS source_type TEXT"))
            conn.execute(text("ALTER TABLE replicated_portfolio_positions ADD COLUMN IF NOT EXISTS source_reason TEXT"))
            conn.execute(text("ALTER TABLE replicated_portfolio_positions ADD COLUMN IF NOT EXISTS confidence TEXT"))
            conn.execute(text("ALTER TABLE replicated_portfolio_positions ADD COLUMN IF NOT EXISTS source_document_id TEXT"))
            conn.execute(text("ALTER TABLE replicated_portfolio_positions ADD COLUMN IF NOT EXISTS source_url TEXT"))

            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS house_annual_disclosure_documents (
                        id SERIAL PRIMARY KEY,
                        source TEXT NOT NULL DEFAULT 'house_clerk_financial_disclosure',
                        member_name TEXT NOT NULL,
                        member_bioguide_id TEXT,
                        filing_year INTEGER NOT NULL,
                        filing_type TEXT,
                        report_url TEXT,
                        document_id TEXT NOT NULL,
                        filing_date DATE,
                        state_district TEXT,
                        payload_json TEXT,
                        created_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS house_annual_disclosure_holdings (
                        id SERIAL PRIMARY KEY,
                        document_row_id INTEGER NOT NULL,
                        source TEXT NOT NULL DEFAULT 'house_clerk_financial_disclosure',
                        member_name TEXT NOT NULL,
                        member_bioguide_id TEXT,
                        filing_year INTEGER NOT NULL,
                        filing_type TEXT,
                        filing_date DATE,
                        report_url TEXT,
                        document_id TEXT NOT NULL,
                        asset_name TEXT NOT NULL,
                        symbol TEXT,
                        owner TEXT,
                        asset_type TEXT,
                        value_range TEXT,
                        value_min DOUBLE PRECISION,
                        value_max DOUBLE PRECISION,
                        income_type TEXT,
                        income_range TEXT,
                        payload_json TEXT,
                        created_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_house_annual_documents_doc "
                "ON house_annual_disclosure_documents (document_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_house_annual_documents_member_year "
                "ON house_annual_disclosure_documents (member_bioguide_id, filing_year)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_house_annual_documents_filing_date "
                "ON house_annual_disclosure_documents (filing_date)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_house_annual_holdings_member_symbol "
                "ON house_annual_disclosure_holdings (member_bioguide_id, symbol)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_house_annual_holdings_document "
                "ON house_annual_disclosure_holdings (document_row_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_house_annual_holdings_filing_date "
                "ON house_annual_disclosure_holdings (filing_date)"
            )
        )


def ensure_trade_outcomes_amount_bigint() -> None:
    if IS_SQLITE:
        return
    with engine.begin() as conn:
        _set_postgres_ddl_timeouts(conn)
        table_exists = conn.execute(
            text(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name = 'trade_outcomes'
                """
            )
        ).scalar_one_or_none()
        if not table_exists:
            return

        for column_name in ("amount_min", "amount_max"):
            data_type = conn.execute(
                text(
                    """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = 'trade_outcomes'
                      AND column_name = :column_name
                    """
                ),
                {"column_name": column_name},
            ).scalar_one_or_none()
            if data_type in {"smallint", "integer"}:
                conn.execute(
                    text(
                        f"ALTER TABLE trade_outcomes "
                        f"ALTER COLUMN {column_name} TYPE BIGINT"
                    )
                )


def ensure_user_account_billing_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        if dialect_name == "sqlite":
            user_accounts_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='user_accounts'")
            ).fetchone()
            if not user_accounts_exists:
                return
            existing = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(user_accounts)")).fetchall()
                if len(row) > 1
            }
            user_columns = {
                "stripe_price_id": "TEXT",
                "current_plan_amount_cents": "INTEGER",
                "current_plan_currency": "TEXT",
                "subscription_interval": "TEXT",
                "original_email": "TEXT",
                "deleted_at": "TIMESTAMP",
                "deleted_by_user": "BOOLEAN NOT NULL DEFAULT 0",
                "deletion_reason": "TEXT",
                "deletion_plan": "TEXT",
                "reactivation_token_hash": "TEXT",
                "reactivation_expires_at": "TIMESTAMP",
            }
            for name, column_type in user_columns.items():
                if name not in existing:
                    conn.execute(text(f"ALTER TABLE user_accounts ADD COLUMN {name} {column_type}"))
            stripe_webhook_events_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='stripe_webhook_events'")
            ).fetchone()
            if stripe_webhook_events_exists:
                webhook_existing = {
                    row[1]
                    for row in conn.execute(text("PRAGMA table_info(stripe_webhook_events)")).fetchall()
                    if len(row) > 1
                }
                if "status" not in webhook_existing:
                    conn.execute(text("ALTER TABLE stripe_webhook_events ADD COLUMN status TEXT NOT NULL DEFAULT 'processed'"))
                if "error_message" not in webhook_existing:
                    conn.execute(text("ALTER TABLE stripe_webhook_events ADD COLUMN error_message TEXT"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_user_accounts_deleted_at ON user_accounts (deleted_at)"))
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ix_user_accounts_reactivation_token "
                    "ON user_accounts (reactivation_token_hash)"
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS admin_billing_override_audit_log (
                        id INTEGER PRIMARY KEY,
                        admin_user_id INTEGER,
                        admin_email TEXT,
                        target_user_id INTEGER NOT NULL,
                        target_email TEXT,
                        override_type TEXT NOT NULL,
                        previous_state_json TEXT NOT NULL DEFAULT '{}',
                        requested_state_json TEXT NOT NULL DEFAULT '{}',
                        stripe_customer_id TEXT,
                        stripe_subscription_id TEXT,
                        stripe_sync_status TEXT NOT NULL DEFAULT 'pending',
                        error_message TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_admin_billing_override_target_created "
                    "ON admin_billing_override_audit_log (target_user_id, created_at)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_admin_billing_override_admin_created "
                    "ON admin_billing_override_audit_log (admin_user_id, created_at)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_admin_billing_override_status_created "
                    "ON admin_billing_override_audit_log (stripe_sync_status, created_at)"
                )
            )
            return

        if dialect_name == "postgresql":
            conn.execute(text("SET LOCAL lock_timeout = '2s'"))
            conn.execute(text("SET LOCAL statement_timeout = '10s'"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS stripe_price_id TEXT"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS current_plan_amount_cents INTEGER"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS current_plan_currency TEXT"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS subscription_interval TEXT"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS original_email TEXT"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS deleted_by_user BOOLEAN NOT NULL DEFAULT false"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS deletion_reason TEXT"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS deletion_plan TEXT"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS reactivation_token_hash TEXT"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS reactivation_expires_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE stripe_webhook_events ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'processed'"))
            conn.execute(text("ALTER TABLE stripe_webhook_events ADD COLUMN IF NOT EXISTS error_message TEXT"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_user_accounts_deleted_at ON user_accounts (deleted_at)"))
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ix_user_accounts_reactivation_token "
                    "ON user_accounts (reactivation_token_hash)"
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS admin_billing_override_audit_log (
                        id BIGSERIAL PRIMARY KEY,
                        admin_user_id INTEGER,
                        admin_email TEXT,
                        target_user_id INTEGER NOT NULL,
                        target_email TEXT,
                        override_type TEXT NOT NULL,
                        previous_state_json TEXT NOT NULL DEFAULT '{}',
                        requested_state_json TEXT NOT NULL DEFAULT '{}',
                        stripe_customer_id TEXT,
                        stripe_subscription_id TEXT,
                        stripe_sync_status TEXT NOT NULL DEFAULT 'pending',
                        error_message TEXT,
                        created_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_admin_billing_override_target_created "
                    "ON admin_billing_override_audit_log (target_user_id, created_at)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_admin_billing_override_admin_created "
                    "ON admin_billing_override_audit_log (admin_user_id, created_at)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_admin_billing_override_status_created "
                    "ON admin_billing_override_audit_log (stripe_sync_status, created_at)"
                )
            )


def ensure_page_analytics_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        if dialect_name == "sqlite":
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS page_view_events (
                        id INTEGER PRIMARY KEY,
                        user_id INTEGER,
                        session_id_hash TEXT,
                        path TEXT NOT NULL,
                        normalized_path TEXT NOT NULL,
                        route_group TEXT NOT NULL,
                        referrer_path TEXT,
                        user_agent_family TEXT,
                        device_type TEXT,
                        is_authenticated BOOLEAN NOT NULL DEFAULT 0,
                        plan_at_time TEXT,
                        metadata_json TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_page_view_events_created_at ON page_view_events (created_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_page_view_events_normalized_created ON page_view_events (normalized_path, created_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_page_view_events_user_created ON page_view_events (user_id, created_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_page_view_events_session_created ON page_view_events (session_id_hash, created_at)"))
            return

        conn.execute(text("SET LOCAL lock_timeout = '2s'"))
        conn.execute(text("SET LOCAL statement_timeout = '10s'"))
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS page_view_events (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER,
                    session_id_hash TEXT,
                    path TEXT NOT NULL,
                    normalized_path TEXT NOT NULL,
                    route_group TEXT NOT NULL,
                    referrer_path TEXT,
                    user_agent_family TEXT,
                    device_type TEXT,
                    is_authenticated BOOLEAN NOT NULL DEFAULT false,
                    plan_at_time TEXT,
                    metadata_json TEXT,
                    created_at TIMESTAMPTZ DEFAULT now()
                )
                """
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_page_view_events_created_at ON page_view_events (created_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_page_view_events_normalized_created ON page_view_events (normalized_path, created_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_page_view_events_user_created ON page_view_events (user_id, created_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_page_view_events_session_created ON page_view_events (session_id_hash, created_at)"))


def ensure_email_notification_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        if dialect_name == "sqlite":
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS email_templates (
                        id INTEGER PRIMARY KEY,
                        template_key TEXT NOT NULL,
                        name TEXT NOT NULL,
                        category TEXT NOT NULL,
                        from_name TEXT NOT NULL,
                        from_email TEXT NOT NULL,
                        reply_to TEXT,
                        subject TEXT NOT NULL,
                        preheader TEXT,
                        body_text TEXT NOT NULL,
                        body_html TEXT,
                        variables_json TEXT NOT NULL DEFAULT '[]',
                        enabled BOOLEAN NOT NULL DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS email_deliveries (
                        id INTEGER PRIMARY KEY,
                        user_id INTEGER,
                        to_email TEXT NOT NULL,
                        from_email TEXT NOT NULL,
                        template_key TEXT,
                        category TEXT NOT NULL,
                        subject TEXT NOT NULL,
                        provider TEXT NOT NULL,
                        provider_message_id TEXT,
                        status TEXT NOT NULL DEFAULT 'queued',
                        idempotency_key TEXT,
                        error TEXT,
                        payload_json TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        sent_at TIMESTAMP
                    )
                    """
                )
            )
            user_accounts_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='user_accounts'")
            ).fetchone()
            if user_accounts_exists:
                existing = {
                    row[1]
                    for row in conn.execute(text("PRAGMA table_info(user_accounts)")).fetchall()
                    if len(row) > 1
                }
                user_columns = {
                    "email_verified_at": "TIMESTAMP",
                    "email_verification_token_hash": "TEXT",
                    "email_verification_expires_at": "TIMESTAMP",
                }
                for name, column_type in user_columns.items():
                    if name not in existing:
                        conn.execute(text(f"ALTER TABLE user_accounts ADD COLUMN {name} {column_type}"))
        else:
            conn.execute(text("SET LOCAL lock_timeout = '2s'"))
            conn.execute(text("SET LOCAL statement_timeout = '10s'"))
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS email_templates (
                        id SERIAL PRIMARY KEY,
                        template_key TEXT NOT NULL,
                        name TEXT NOT NULL,
                        category TEXT NOT NULL,
                        from_name TEXT NOT NULL,
                        from_email TEXT NOT NULL,
                        reply_to TEXT,
                        subject TEXT NOT NULL,
                        preheader TEXT,
                        body_text TEXT NOT NULL,
                        body_html TEXT,
                        variables_json TEXT NOT NULL DEFAULT '[]',
                        enabled BOOLEAN NOT NULL DEFAULT true,
                        created_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS email_deliveries (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER,
                        to_email TEXT NOT NULL,
                        from_email TEXT NOT NULL,
                        template_key TEXT,
                        category TEXT NOT NULL,
                        subject TEXT NOT NULL,
                        provider TEXT NOT NULL,
                        provider_message_id TEXT,
                        status TEXT NOT NULL DEFAULT 'queued',
                        idempotency_key TEXT,
                        error TEXT,
                        payload_json TEXT,
                        created_at TIMESTAMPTZ DEFAULT now(),
                        sent_at TIMESTAMPTZ
                    )
                    """
                )
            )
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS email_verified_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS email_verification_token_hash TEXT"))
            conn.execute(text("ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS email_verification_expires_at TIMESTAMPTZ"))

            template_columns = {
                "reply_to": "TEXT",
                "preheader": "TEXT",
                "body_html": "TEXT",
                "variables_json": "TEXT NOT NULL DEFAULT '[]'",
                "enabled": "BOOLEAN NOT NULL DEFAULT true",
                "created_at": "TIMESTAMPTZ DEFAULT now()",
                "updated_at": "TIMESTAMPTZ DEFAULT now()",
            }
            for name, column_type in template_columns.items():
                conn.execute(text(f"ALTER TABLE email_templates ADD COLUMN IF NOT EXISTS {name} {column_type}"))

            delivery_columns = {
                "user_id": "INTEGER",
                "provider_message_id": "TEXT",
                "idempotency_key": "TEXT",
                "error": "TEXT",
                "payload_json": "TEXT",
                "created_at": "TIMESTAMPTZ DEFAULT now()",
                "sent_at": "TIMESTAMPTZ",
            }
            for name, column_type in delivery_columns.items():
                conn.execute(text(f"ALTER TABLE email_deliveries ADD COLUMN IF NOT EXISTS {name} {column_type}"))

        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_email_templates_template_key "
                "ON email_templates (template_key)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_email_templates_category "
                "ON email_templates (category)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_email_deliveries_user_created "
                "ON email_deliveries (user_id, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_email_deliveries_status "
                "ON email_deliveries (status)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_email_deliveries_template_key "
                "ON email_deliveries (template_key)"
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_email_deliveries_idempotency_key "
                "ON email_deliveries (idempotency_key)"
            )
        )


def ensure_ai_marketing_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        if dialect_name == "sqlite":
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_settings (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        is_secret BOOLEAN NOT NULL DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_campaigns (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        enabled BOOLEAN NOT NULL DEFAULT 1,
                        mode TEXT NOT NULL,
                        campaign_type TEXT,
                        content_type TEXT,
                        status TEXT NOT NULL DEFAULT 'active',
                        schedule_config_json TEXT NOT NULL DEFAULT '{}',
                        weekdays_only BOOLEAN NOT NULL DEFAULT 1,
                        run_time TEXT,
                        timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles',
                        recipient_email TEXT NOT NULL DEFAULT 'jarod@walnutmarkets.com',
                        source_type TEXT,
                        source_reference_id TEXT,
                        filters_json TEXT NOT NULL DEFAULT '{}',
                        output_preferences_json TEXT NOT NULL DEFAULT '{}',
                        created_by INTEGER,
                        updated_by INTEGER,
                        last_run_at TIMESTAMP,
                        next_run_at TIMESTAMP,
                        platforms_json TEXT NOT NULL DEFAULT '[]',
                        keywords_json TEXT NOT NULL DEFAULT '[]',
                        tickers_json TEXT NOT NULL DEFAULT '[]',
                        subreddits_json TEXT NOT NULL DEFAULT '[]',
                        query_templates_json TEXT NOT NULL DEFAULT '[]',
                        minimum_relevance_score INTEGER NOT NULL DEFAULT 60,
                        max_items_per_run INTEGER NOT NULL DEFAULT 10,
                        max_drafts_per_day INTEGER NOT NULL DEFAULT 1,
                        recency TEXT NOT NULL DEFAULT 'week',
                        default_destination_page TEXT NOT NULL DEFAULT 'https://walnutmarkets.com',
                        include_disclosure BOOLEAN NOT NULL DEFAULT 1,
                        scheduled_digest_enabled BOOLEAN NOT NULL DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_article_candidates (
                        id INTEGER PRIMARY KEY,
                        provider TEXT NOT NULL DEFAULT 'fmp',
                        provider_article_id TEXT,
                        title TEXT NOT NULL,
                        url TEXT NOT NULL,
                        site TEXT,
                        published_at TIMESTAMP,
                        tickers_json TEXT NOT NULL DEFAULT '[]',
                        image_url TEXT,
                        summary TEXT,
                        raw_metadata_json TEXT NOT NULL DEFAULT '{}',
                        first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        dedupe_hash TEXT NOT NULL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_campaign_runs (
                        id INTEGER PRIMARY KEY,
                        campaign_id INTEGER NOT NULL,
                        campaign_type TEXT NOT NULL,
                        run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status TEXT NOT NULL,
                        candidates_considered INTEGER NOT NULL DEFAULT 0,
                        drafts_generated INTEGER NOT NULL DEFAULT 0,
                        emails_sent INTEGER NOT NULL DEFAULT 0,
                        failure_reason TEXT,
                        payload_json TEXT NOT NULL DEFAULT '{}'
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_opportunities (
                        id INTEGER PRIMARY KEY,
                        campaign_id INTEGER,
                        platform TEXT NOT NULL,
                        source_provider TEXT,
                        source_id TEXT,
                        source_url TEXT NOT NULL,
                        source_dedupe_key TEXT NOT NULL,
                        title TEXT NOT NULL,
                        excerpt TEXT,
                        author TEXT,
                        community TEXT,
                        source_score INTEGER,
                        comment_count INTEGER,
                        source_created_at TIMESTAMP,
                        status TEXT NOT NULL DEFAULT 'new',
                        campaign_type TEXT,
                        content_type TEXT,
                        source_platform TEXT,
                        ticker_theme TEXT,
                        recommended_action TEXT,
                        matched_keywords_json TEXT NOT NULL DEFAULT '[]',
                        matched_tickers_json TEXT NOT NULL DEFAULT '[]',
                        fit_score INTEGER,
                        relevance_score INTEGER,
                        spam_risk_score INTEGER,
                        intent TEXT,
                        suggested_destination_url TEXT,
                        short_reason TEXT,
                        compliance_notes TEXT,
                        generated_content TEXT,
                        full_markdown TEXT,
                        alternate_versions_json TEXT NOT NULL DEFAULT '{}',
                        quality_scores_json TEXT NOT NULL DEFAULT '{}',
                        source_notes_json TEXT NOT NULL DEFAULT '[]',
                        missing_data_notes_json TEXT NOT NULL DEFAULT '[]',
                        asset_refs_json TEXT NOT NULL DEFAULT '[]',
                        raw_metadata_json TEXT NOT NULL DEFAULT '{}',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_seen_at TIMESTAMP,
                        emailed_at TIMESTAMP,
                        opened_at TIMESTAMP,
                        copied_at TIMESTAMP,
                        posted_manually_at TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_suggestions (
                        id INTEGER PRIMARY KEY,
                        opportunity_id INTEGER NOT NULL,
                        campaign_id INTEGER,
                        model TEXT NOT NULL,
                        relevance_score INTEGER NOT NULL DEFAULT 0,
                        spam_risk_score INTEGER NOT NULL DEFAULT 0,
                        detected_tickers_json TEXT NOT NULL DEFAULT '[]',
                        intent TEXT NOT NULL DEFAULT 'other',
                        campaign_type TEXT NOT NULL DEFAULT 'legacy_outreach_campaign',
                        content_type TEXT NOT NULL DEFAULT 'reddit_reply',
                        platform TEXT NOT NULL DEFAULT 'reddit',
                        audience TEXT NOT NULL DEFAULT '',
                        recommended_action TEXT NOT NULL DEFAULT 'reply',
                        reply_angle TEXT NOT NULL DEFAULT 'other',
                        content_angle TEXT NOT NULL DEFAULT '',
                        value_added_insight TEXT NOT NULL DEFAULT '',
                        walnut_feature_to_mention TEXT NOT NULL DEFAULT '',
                        suggested_destination_url TEXT NOT NULL,
                        suggested_reply TEXT NOT NULL,
                        suggested_post TEXT NOT NULL DEFAULT '',
                        suggested_ad_variants_json TEXT NOT NULL DEFAULT '[]',
                        influencer_outreach_draft TEXT NOT NULL DEFAULT '',
                        report_pack_outline TEXT NOT NULL DEFAULT '',
                        alternate_hooks_json TEXT NOT NULL DEFAULT '[]',
                        title_options_json TEXT NOT NULL DEFAULT '[]',
                        disclosure_text TEXT NOT NULL DEFAULT '',
                        assets_json TEXT NOT NULL DEFAULT '[]',
                        alternate_reply_more_direct TEXT NOT NULL DEFAULT '',
                        short_reason TEXT NOT NULL,
                        compliance_notes TEXT NOT NULL,
                        prompt_version TEXT NOT NULL DEFAULT 'ai_marketing_v2',
                        raw_response_json TEXT NOT NULL DEFAULT '{}',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_email_logs (
                        id INTEGER PRIMARY KEY,
                        delivery_id INTEGER,
                        to_email TEXT NOT NULL,
                        subject TEXT NOT NULL,
                        opportunity_ids_json TEXT NOT NULL DEFAULT '[]',
                        status TEXT NOT NULL DEFAULT 'queued',
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        sent_at TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_growth_email_action_tokens (
                        id INTEGER PRIMARY KEY,
                        token_id TEXT NOT NULL UNIQUE,
                        draft_id INTEGER NOT NULL,
                        action TEXT NOT NULL,
                        actor_email TEXT,
                        nonce_hash TEXT NOT NULL,
                        expires_at TIMESTAMP,
                        used_at TIMESTAMP,
                        result TEXT,
                        ip_address TEXT,
                        user_agent TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
        else:
            conn.execute(text("SET LOCAL lock_timeout = '2s'"))
            conn.execute(text("SET LOCAL statement_timeout = '10s'"))
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_settings (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        is_secret BOOLEAN NOT NULL DEFAULT false,
                        created_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_campaigns (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        enabled BOOLEAN NOT NULL DEFAULT true,
                        mode TEXT NOT NULL,
                        campaign_type TEXT,
                        content_type TEXT,
                        status TEXT NOT NULL DEFAULT 'active',
                        schedule_config_json TEXT NOT NULL DEFAULT '{}',
                        weekdays_only BOOLEAN NOT NULL DEFAULT true,
                        run_time TEXT,
                        timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles',
                        recipient_email TEXT NOT NULL DEFAULT 'jarod@walnutmarkets.com',
                        source_type TEXT,
                        source_reference_id TEXT,
                        filters_json TEXT NOT NULL DEFAULT '{}',
                        output_preferences_json TEXT NOT NULL DEFAULT '{}',
                        created_by INTEGER,
                        updated_by INTEGER,
                        last_run_at TIMESTAMPTZ,
                        next_run_at TIMESTAMPTZ,
                        platforms_json TEXT NOT NULL DEFAULT '[]',
                        keywords_json TEXT NOT NULL DEFAULT '[]',
                        tickers_json TEXT NOT NULL DEFAULT '[]',
                        subreddits_json TEXT NOT NULL DEFAULT '[]',
                        query_templates_json TEXT NOT NULL DEFAULT '[]',
                        minimum_relevance_score INTEGER NOT NULL DEFAULT 60,
                        max_items_per_run INTEGER NOT NULL DEFAULT 10,
                        max_drafts_per_day INTEGER NOT NULL DEFAULT 1,
                        recency TEXT NOT NULL DEFAULT 'week',
                        default_destination_page TEXT NOT NULL DEFAULT 'https://walnutmarkets.com',
                        include_disclosure BOOLEAN NOT NULL DEFAULT true,
                        scheduled_digest_enabled BOOLEAN NOT NULL DEFAULT false,
                        created_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_article_candidates (
                        id SERIAL PRIMARY KEY,
                        provider TEXT NOT NULL DEFAULT 'fmp',
                        provider_article_id TEXT,
                        title TEXT NOT NULL,
                        url TEXT NOT NULL,
                        site TEXT,
                        published_at TIMESTAMPTZ,
                        tickers_json TEXT NOT NULL DEFAULT '[]',
                        image_url TEXT,
                        summary TEXT,
                        raw_metadata_json TEXT NOT NULL DEFAULT '{}',
                        first_seen_at TIMESTAMPTZ DEFAULT now(),
                        last_seen_at TIMESTAMPTZ DEFAULT now(),
                        dedupe_hash TEXT NOT NULL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_campaign_runs (
                        id SERIAL PRIMARY KEY,
                        campaign_id INTEGER NOT NULL,
                        campaign_type TEXT NOT NULL,
                        run_at TIMESTAMPTZ DEFAULT now(),
                        status TEXT NOT NULL,
                        candidates_considered INTEGER NOT NULL DEFAULT 0,
                        drafts_generated INTEGER NOT NULL DEFAULT 0,
                        emails_sent INTEGER NOT NULL DEFAULT 0,
                        failure_reason TEXT,
                        payload_json TEXT NOT NULL DEFAULT '{}'
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_opportunities (
                        id SERIAL PRIMARY KEY,
                        campaign_id INTEGER,
                        platform TEXT NOT NULL,
                        source_provider TEXT,
                        source_id TEXT,
                        source_url TEXT NOT NULL,
                        source_dedupe_key TEXT NOT NULL,
                        title TEXT NOT NULL,
                        excerpt TEXT,
                        author TEXT,
                        community TEXT,
                        source_score INTEGER,
                        comment_count INTEGER,
                        source_created_at TIMESTAMPTZ,
                        status TEXT NOT NULL DEFAULT 'new',
                        campaign_type TEXT,
                        content_type TEXT,
                        source_platform TEXT,
                        ticker_theme TEXT,
                        recommended_action TEXT,
                        matched_keywords_json TEXT NOT NULL DEFAULT '[]',
                        matched_tickers_json TEXT NOT NULL DEFAULT '[]',
                        fit_score INTEGER,
                        relevance_score INTEGER,
                        spam_risk_score INTEGER,
                        intent TEXT,
                        suggested_destination_url TEXT,
                        short_reason TEXT,
                        compliance_notes TEXT,
                        generated_content TEXT,
                        full_markdown TEXT,
                        alternate_versions_json TEXT NOT NULL DEFAULT '{}',
                        quality_scores_json TEXT NOT NULL DEFAULT '{}',
                        source_notes_json TEXT NOT NULL DEFAULT '[]',
                        missing_data_notes_json TEXT NOT NULL DEFAULT '[]',
                        asset_refs_json TEXT NOT NULL DEFAULT '[]',
                        raw_metadata_json TEXT NOT NULL DEFAULT '{}',
                        created_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now(),
                        last_seen_at TIMESTAMPTZ,
                        emailed_at TIMESTAMPTZ,
                        opened_at TIMESTAMPTZ,
                        copied_at TIMESTAMPTZ,
                        posted_manually_at TIMESTAMPTZ
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_suggestions (
                        id SERIAL PRIMARY KEY,
                        opportunity_id INTEGER NOT NULL,
                        campaign_id INTEGER,
                        model TEXT NOT NULL,
                        relevance_score INTEGER NOT NULL DEFAULT 0,
                        spam_risk_score INTEGER NOT NULL DEFAULT 0,
                        detected_tickers_json TEXT NOT NULL DEFAULT '[]',
                        intent TEXT NOT NULL DEFAULT 'other',
                        campaign_type TEXT NOT NULL DEFAULT 'legacy_outreach_campaign',
                        content_type TEXT NOT NULL DEFAULT 'reddit_reply',
                        platform TEXT NOT NULL DEFAULT 'reddit',
                        audience TEXT NOT NULL DEFAULT '',
                        recommended_action TEXT NOT NULL DEFAULT 'reply',
                        reply_angle TEXT NOT NULL DEFAULT 'other',
                        content_angle TEXT NOT NULL DEFAULT '',
                        value_added_insight TEXT NOT NULL DEFAULT '',
                        walnut_feature_to_mention TEXT NOT NULL DEFAULT '',
                        suggested_destination_url TEXT NOT NULL,
                        suggested_reply TEXT NOT NULL,
                        suggested_post TEXT NOT NULL DEFAULT '',
                        suggested_ad_variants_json TEXT NOT NULL DEFAULT '[]',
                        influencer_outreach_draft TEXT NOT NULL DEFAULT '',
                        report_pack_outline TEXT NOT NULL DEFAULT '',
                        alternate_hooks_json TEXT NOT NULL DEFAULT '[]',
                        title_options_json TEXT NOT NULL DEFAULT '[]',
                        disclosure_text TEXT NOT NULL DEFAULT '',
                        assets_json TEXT NOT NULL DEFAULT '[]',
                        alternate_reply_more_direct TEXT NOT NULL DEFAULT '',
                        short_reason TEXT NOT NULL,
                        compliance_notes TEXT NOT NULL,
                        prompt_version TEXT NOT NULL DEFAULT 'ai_marketing_v2',
                        raw_response_json TEXT NOT NULL DEFAULT '{}',
                        created_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_email_logs (
                        id SERIAL PRIMARY KEY,
                        delivery_id INTEGER,
                        to_email TEXT NOT NULL,
                        subject TEXT NOT NULL,
                        opportunity_ids_json TEXT NOT NULL DEFAULT '[]',
                        status TEXT NOT NULL DEFAULT 'queued',
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        created_at TIMESTAMPTZ DEFAULT now(),
                        sent_at TIMESTAMPTZ
                    )
                    """
                )
            )

        if conn.dialect.name == "sqlite":
            existing_campaign_columns = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(ai_marketing_campaigns)")).fetchall()
                if len(row) > 1
            }
            for name, column_type in {
                "campaign_type": "TEXT",
                "content_type": "TEXT",
                "status": "TEXT NOT NULL DEFAULT 'active'",
                "schedule_config_json": "TEXT NOT NULL DEFAULT '{}'",
                "weekdays_only": "BOOLEAN NOT NULL DEFAULT 1",
                "run_time": "TEXT",
                "timezone": "TEXT NOT NULL DEFAULT 'America/Los_Angeles'",
                "recipient_email": "TEXT NOT NULL DEFAULT 'jarod@walnutmarkets.com'",
                "source_type": "TEXT",
                "source_reference_id": "TEXT",
                "filters_json": "TEXT NOT NULL DEFAULT '{}'",
                "output_preferences_json": "TEXT NOT NULL DEFAULT '{}'",
                "created_by": "INTEGER",
                "updated_by": "INTEGER",
                "last_run_at": "TIMESTAMP",
                "next_run_at": "TIMESTAMP",
                "query_templates_json": "TEXT NOT NULL DEFAULT '[]'",
                "max_drafts_per_day": "INTEGER NOT NULL DEFAULT 1",
                "recency": "TEXT NOT NULL DEFAULT 'week'",
            }.items():
                if name not in existing_campaign_columns:
                    conn.execute(text(f"ALTER TABLE ai_marketing_campaigns ADD COLUMN {name} {column_type}"))
            existing_opportunity_columns = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(ai_marketing_opportunities)")).fetchall()
                if len(row) > 1
            }
            if "source_provider" not in existing_opportunity_columns:
                conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN source_provider TEXT"))
            for name, column_type in {
                "campaign_type": "TEXT",
                "content_type": "TEXT",
                "source_platform": "TEXT",
                "ticker_theme": "TEXT",
                "recommended_action": "TEXT",
                "fit_score": "INTEGER",
                "generated_content": "TEXT",
                "full_markdown": "TEXT",
                "alternate_versions_json": "TEXT NOT NULL DEFAULT '{}'",
                "quality_scores_json": "TEXT NOT NULL DEFAULT '{}'",
                "source_notes_json": "TEXT NOT NULL DEFAULT '[]'",
                "missing_data_notes_json": "TEXT NOT NULL DEFAULT '[]'",
                "asset_refs_json": "TEXT NOT NULL DEFAULT '[]'",
                "emailed_at": "TIMESTAMP",
                "opened_at": "TIMESTAMP",
                "copied_at": "TIMESTAMP",
                "posted_manually_at": "TIMESTAMP",
            }.items():
                if name not in existing_opportunity_columns:
                    conn.execute(text(f"ALTER TABLE ai_marketing_opportunities ADD COLUMN {name} {column_type}"))
            existing_suggestion_columns = {
                row[1]
                for row in conn.execute(text("PRAGMA table_info(ai_marketing_suggestions)")).fetchall()
                if len(row) > 1
            }
            for name, column_type in {
                "campaign_type": "TEXT NOT NULL DEFAULT 'legacy_outreach_campaign'",
                "content_type": "TEXT NOT NULL DEFAULT 'reddit_reply'",
                "platform": "TEXT NOT NULL DEFAULT 'reddit'",
                "audience": "TEXT NOT NULL DEFAULT ''",
                "recommended_action": "TEXT NOT NULL DEFAULT 'reply'",
                "reply_angle": "TEXT NOT NULL DEFAULT 'other'",
                "content_angle": "TEXT NOT NULL DEFAULT ''",
                "value_added_insight": "TEXT NOT NULL DEFAULT ''",
                "walnut_feature_to_mention": "TEXT NOT NULL DEFAULT ''",
                "suggested_post": "TEXT NOT NULL DEFAULT ''",
                "suggested_ad_variants_json": "TEXT NOT NULL DEFAULT '[]'",
                "influencer_outreach_draft": "TEXT NOT NULL DEFAULT ''",
                "report_pack_outline": "TEXT NOT NULL DEFAULT ''",
                "alternate_hooks_json": "TEXT NOT NULL DEFAULT '[]'",
                "title_options_json": "TEXT NOT NULL DEFAULT '[]'",
                "disclosure_text": "TEXT NOT NULL DEFAULT ''",
                "assets_json": "TEXT NOT NULL DEFAULT '[]'",
                "alternate_reply_more_direct": "TEXT NOT NULL DEFAULT ''",
            }.items():
                if name not in existing_suggestion_columns:
                    conn.execute(text(f"ALTER TABLE ai_marketing_suggestions ADD COLUMN {name} {column_type}"))
        else:
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS campaign_type TEXT"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS content_type TEXT"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS schedule_config_json TEXT NOT NULL DEFAULT '{}'"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS weekdays_only BOOLEAN NOT NULL DEFAULT true"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS run_time TEXT"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles'"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS recipient_email TEXT NOT NULL DEFAULT 'jarod@walnutmarkets.com'"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS source_type TEXT"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS source_reference_id TEXT"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS filters_json TEXT NOT NULL DEFAULT '{}'"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS output_preferences_json TEXT NOT NULL DEFAULT '{}'"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS created_by INTEGER"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS updated_by INTEGER"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS last_run_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS query_templates_json TEXT NOT NULL DEFAULT '[]'"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS max_drafts_per_day INTEGER NOT NULL DEFAULT 1"))
            conn.execute(text("ALTER TABLE ai_marketing_campaigns ADD COLUMN IF NOT EXISTS recency TEXT NOT NULL DEFAULT 'week'"))
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS ai_marketing_campaign_runs (
                        id SERIAL PRIMARY KEY,
                        campaign_id INTEGER NOT NULL,
                        campaign_type TEXT NOT NULL,
                        run_at TIMESTAMPTZ DEFAULT now(),
                        status TEXT NOT NULL,
                        candidates_considered INTEGER NOT NULL DEFAULT 0,
                        drafts_generated INTEGER NOT NULL DEFAULT 0,
                        emails_sent INTEGER NOT NULL DEFAULT 0,
                        failure_reason TEXT,
                        payload_json TEXT NOT NULL DEFAULT '{}'
                    )
                    """
                )
            )
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS source_provider TEXT"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS campaign_type TEXT"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS content_type TEXT"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS source_platform TEXT"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS ticker_theme TEXT"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS recommended_action TEXT"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS fit_score INTEGER"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS generated_content TEXT"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS full_markdown TEXT"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS alternate_versions_json TEXT NOT NULL DEFAULT '{}'"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS quality_scores_json TEXT NOT NULL DEFAULT '{}'"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS source_notes_json TEXT NOT NULL DEFAULT '[]'"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS missing_data_notes_json TEXT NOT NULL DEFAULT '[]'"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS asset_refs_json TEXT NOT NULL DEFAULT '[]'"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS emailed_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS opened_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS copied_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE ai_marketing_opportunities ADD COLUMN IF NOT EXISTS posted_manually_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS campaign_type TEXT NOT NULL DEFAULT 'legacy_outreach_campaign'"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS content_type TEXT NOT NULL DEFAULT 'reddit_reply'"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS platform TEXT NOT NULL DEFAULT 'reddit'"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS audience TEXT NOT NULL DEFAULT ''"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS recommended_action TEXT NOT NULL DEFAULT 'reply'"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS reply_angle TEXT NOT NULL DEFAULT 'other'"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS content_angle TEXT NOT NULL DEFAULT ''"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS value_added_insight TEXT NOT NULL DEFAULT ''"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS walnut_feature_to_mention TEXT NOT NULL DEFAULT ''"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS suggested_post TEXT NOT NULL DEFAULT ''"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS suggested_ad_variants_json TEXT NOT NULL DEFAULT '[]'"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS influencer_outreach_draft TEXT NOT NULL DEFAULT ''"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS report_pack_outline TEXT NOT NULL DEFAULT ''"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS alternate_hooks_json TEXT NOT NULL DEFAULT '[]'"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS title_options_json TEXT NOT NULL DEFAULT '[]'"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS disclosure_text TEXT NOT NULL DEFAULT ''"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS assets_json TEXT NOT NULL DEFAULT '[]'"))
            conn.execute(text("ALTER TABLE ai_marketing_suggestions ADD COLUMN IF NOT EXISTS alternate_reply_more_direct TEXT NOT NULL DEFAULT ''"))

        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ai_marketing_settings_secret ON ai_marketing_settings (is_secret)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ai_marketing_campaigns_enabled ON ai_marketing_campaigns (enabled)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ai_marketing_campaigns_mode ON ai_marketing_campaigns (mode)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ai_marketing_campaign_runs_campaign_time ON ai_marketing_campaign_runs (campaign_id, run_at)"))
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_ai_marketing_article_candidates_provider_hash "
                "ON ai_marketing_article_candidates (provider, dedupe_hash)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_ai_marketing_article_candidates_published "
                "ON ai_marketing_article_candidates (published_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_ai_marketing_article_candidates_seen "
                "ON ai_marketing_article_candidates (last_seen_at)"
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_ai_marketing_opportunities_source "
                "ON ai_marketing_opportunities (platform, source_dedupe_key)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_ai_marketing_opportunities_campaign_status "
                "ON ai_marketing_opportunities (campaign_id, status)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_ai_marketing_opportunities_status_created "
                "ON ai_marketing_opportunities (status, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_ai_marketing_opportunities_platform_created "
                "ON ai_marketing_opportunities (platform, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_ai_marketing_suggestions_opportunity_created "
                "ON ai_marketing_suggestions (opportunity_id, created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_ai_marketing_suggestions_campaign_created "
                "ON ai_marketing_suggestions (campaign_id, created_at)"
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ai_marketing_email_logs_created ON ai_marketing_email_logs (created_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ai_marketing_email_logs_status ON ai_marketing_email_logs (status)"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_ai_growth_email_action_tokens_token ON ai_growth_email_action_tokens (token_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ai_growth_email_action_tokens_draft ON ai_growth_email_action_tokens (draft_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ai_growth_email_action_tokens_created ON ai_growth_email_action_tokens (created_at)"))


def ensure_reddit_ads_assistant_schema(bind=engine) -> None:
    with bind.begin() as conn:
        if conn.dialect.name == "postgresql":
            conn.execute(text("SET LOCAL lock_timeout = '2s'"))
            conn.execute(text("SET LOCAL statement_timeout = '10s'"))
            id_column = "BIGSERIAL PRIMARY KEY"
            timestamp_type = "TIMESTAMPTZ"
            timestamp_default = "now()"
        else:
            id_column = "INTEGER PRIMARY KEY"
            timestamp_type = "TIMESTAMP"
            timestamp_default = "CURRENT_TIMESTAMP"

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS reddit_ad_drafts (
                    id {id_column},
                    status TEXT NOT NULL DEFAULT 'draft',
                    campaign_objective TEXT NOT NULL,
                    audience TEXT NOT NULL,
                    geography TEXT NOT NULL,
                    product_angle TEXT NOT NULL,
                    plan TEXT NOT NULL,
                    tone TEXT NOT NULL,
                    destination_type TEXT NOT NULL,
                    destination_url TEXT NOT NULL,
                    ticker_symbols_json TEXT NOT NULL DEFAULT '[]',
                    research_urls_json TEXT NOT NULL DEFAULT '[]',
                    input_settings_json TEXT NOT NULL DEFAULT '{{}}',
                    generated_draft_json TEXT NOT NULL DEFAULT '{{}}',
                    final_draft_json TEXT NOT NULL DEFAULT '{{}}',
                    compliance_warnings_json TEXT NOT NULL DEFAULT '[]',
                    creative_reference_json TEXT NOT NULL DEFAULT '{{}}',
                    created_by INTEGER,
                    updated_by INTEGER,
                    approved_by INTEGER,
                    approver_email TEXT,
                    approved_at {timestamp_type},
                    archived_at {timestamp_type},
                    extension_token_hash TEXT,
                    extension_token_expires_at {timestamp_type},
                    raw_openai_response_json TEXT NOT NULL DEFAULT '{{}}',
                    audit_log_json TEXT NOT NULL DEFAULT '[]',
                    created_at {timestamp_type} DEFAULT {timestamp_default},
                    updated_at {timestamp_type} DEFAULT {timestamp_default}
                )
                """
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_reddit_ad_drafts_status_created ON reddit_ad_drafts (status, created_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_reddit_ad_drafts_approver ON reddit_ad_drafts (approved_by, approved_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_reddit_ad_drafts_token_hash ON reddit_ad_drafts (extension_token_hash)"))


def ensure_outcome_ledger_schema(bind=engine) -> None:
    with bind.begin() as conn:
        dialect_name = conn.dialect.name
        _set_postgres_ddl_timeouts(conn, statement_timeout="10s")
        if dialect_name == "postgresql":
            id_column = "BIGSERIAL PRIMARY KEY"
            timestamp_type = "TIMESTAMPTZ"
            bool_type = "BOOLEAN"
            bool_false = "false"
            bool_true = "true"
            current_unique_sql = (
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_confirmation_methodology_versions_current "
                "ON confirmation_methodology_versions (is_current) WHERE is_current"
            )
        else:
            id_column = "INTEGER PRIMARY KEY"
            timestamp_type = "TIMESTAMP"
            bool_type = "BOOLEAN"
            bool_false = "0"
            bool_true = "1"
            current_unique_sql = (
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_confirmation_methodology_versions_current "
                "ON confirmation_methodology_versions (is_current) WHERE is_current = 1"
            )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS confirmation_methodology_versions (
                    id {id_column},
                    version TEXT NOT NULL,
                    description TEXT NOT NULL,
                    configuration_json TEXT NOT NULL DEFAULT '{{}}',
                    code_commit_sha TEXT,
                    deployed_at {timestamp_type} NOT NULL,
                    retired_at {timestamp_type},
                    is_current {bool_type} NOT NULL DEFAULT {bool_false},
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS confirmation_score_snapshots (
                    id {id_column},
                    security_id INTEGER NOT NULL,
                    ticker_at_time TEXT NOT NULL,
                    calculated_at {timestamp_type} NOT NULL,
                    market_date DATE NOT NULL,
                    score INTEGER NOT NULL,
                    direction TEXT NOT NULL,
                    strength TEXT NOT NULL,
                    reference_price FLOAT,
                    reference_price_at {timestamp_type},
                    reference_price_source TEXT,
                    active_source_count INTEGER NOT NULL DEFAULT 0,
                    active_sources_json TEXT NOT NULL DEFAULT '[]',
                    source_contributions_json TEXT NOT NULL DEFAULT '{{}}',
                    source_freshness_json TEXT NOT NULL DEFAULT '{{}}',
                    input_hash TEXT NOT NULL,
                    methodology_version_id INTEGER NOT NULL,
                    calculation_type TEXT NOT NULL DEFAULT 'live',
                    code_commit_sha TEXT,
                    supersedes_snapshot_id INTEGER,
                    correction_reason TEXT,
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_confirmation_methodology_versions_version ON confirmation_methodology_versions (version)"))
        conn.execute(text(current_unique_sql))
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_confirmation_score_snapshots_live_input "
                "ON confirmation_score_snapshots (security_id, methodology_version_id, market_date, input_hash, calculation_type)"
            )
        )
        for statement in (
            "CREATE INDEX IF NOT EXISTS ix_confirmation_score_snapshots_security_calculated ON confirmation_score_snapshots (security_id, calculated_at)",
            "CREATE INDEX IF NOT EXISTS ix_confirmation_score_snapshots_market_date ON confirmation_score_snapshots (market_date)",
            "CREATE INDEX IF NOT EXISTS ix_confirmation_score_snapshots_methodology ON confirmation_score_snapshots (methodology_version_id)",
            "CREATE INDEX IF NOT EXISTS ix_confirmation_score_snapshots_type ON confirmation_score_snapshots (calculation_type)",
            "CREATE INDEX IF NOT EXISTS ix_confirmation_score_snapshots_input_hash ON confirmation_score_snapshots (input_hash)",
            "CREATE INDEX IF NOT EXISTS ix_confirmation_score_snapshots_created ON confirmation_score_snapshots (created_at)",
            "CREATE INDEX IF NOT EXISTS ix_confirmation_score_snapshots_ticker ON confirmation_score_snapshots (ticker_at_time)",
        ):
            conn.execute(text(statement))

        existing_current = conn.execute(
            text("SELECT id FROM confirmation_methodology_versions WHERE is_current = :current LIMIT 1"),
            {"current": True if dialect_name == "postgresql" else 1},
        ).fetchone()
        if existing_current is None:
            configuration_json = (
                '{"classification_version":"confirmation_direction_v3","lookback_days":30,'
                '"source_order":["congress","insiders","signals","analysts","price_volume","fundamentals",'
                '"options_flow","government_contracts","institutional_activity","macro_positioning"],'
                '"phase":"outcome-ledger-phase-1"}'
            )
            commit_sha = (
                os.getenv("SOURCE_COMMIT_SHA")
                or os.getenv("GIT_COMMIT_SHA")
                or os.getenv("VERCEL_GIT_COMMIT_SHA")
                or os.getenv("FLY_COMMIT_SHA")
                or "unknown"
            )
            conn.execute(
                text(
                    """
                    INSERT INTO confirmation_methodology_versions
                        (version, description, configuration_json, code_commit_sha, deployed_at, is_current)
                    VALUES
                        (:version, :description, :configuration_json, :code_commit_sha, CURRENT_TIMESTAMP, :is_current)
                    """
                ),
                {
                    "version": "confirmation-v1",
                    "description": "Initial version record for the currently deployed Walnut confirmation score methodology.",
                    "configuration_json": configuration_json,
                    "code_commit_sha": commit_sha,
                    "is_current": True if dialect_name == "postgresql" else 1,
                },
            )


def get_db():
    context = get_request_context()
    session_started = perf_counter()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        if context:
            elapsed_ms = (perf_counter() - session_started) * 1000
            threshold_ms = float(os.getenv("DB_SESSION_SLOW_LOG_MS", "2000") or 2000)
            if elapsed_ms >= threshold_ms:
                logger.info(
                    "db_session_timing path=%s priority=%s walnut_route=%s walnut_component=%s duration_ms=%.1f",
                    context.get("path", "unknown"),
                    context.get("priority", "unknown"),
                    context.get("walnut_route", "unknown"),
                    context.get("walnut_component", "unknown"),
                    elapsed_ms,
                )


def is_database_locked_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return "database is locked" in message or "database is busy" in message
