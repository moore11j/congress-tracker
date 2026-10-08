"""Transaction-bound feed ownership shared by publishers and source switches.

PostgreSQL advisory transaction locks survive flushes and release only with the
canonical transaction. Every writer must deploy this guard before activation.
SQLite uses its single-writer transaction for local development/rehearsals.
"""
from contextlib import contextmanager
from datetime import date, datetime, timezone
from functools import wraps

from sqlalchemy import Date, DateTime, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


LOCK_KEYS = {'sec_form4': 84193648, 'sec_13f': 84193649, 'house_ptr': 84193650, 'senate_ptr': 84193651}
DIRECT_PROVIDERS = {'sec_form4': 'sec_edgar', 'sec_13f': 'sec_edgar',
                    'house_ptr': 'official_house', 'senate_ptr': 'official_senate'}


class FeedSourceControl(Base):
    __tablename__ = 'feed_source_controls'
    feed: Mapped[str] = mapped_column(primary_key=True)
    provider: Mapped[str] = mapped_column(default='fmp')
    publish_since: Mapped[date | None] = mapped_column(Date)
    generation: Mapped[int] = mapped_column(default=0)
    reason: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class FeedWriterBusy(RuntimeError):
    pass


class FeedSourceMismatch(RuntimeError):
    pass


def lock_feed(db, feed):
    key = LOCK_KEYS.get(feed)
    if key is None:
        raise ValueError('Feed has no validated publication implementation')
    bind = db.get_bind()
    if bind.dialect.name == 'postgresql':
        if not db.scalar(text('SELECT pg_try_advisory_xact_lock(:key)'), {'key': key}):
            raise FeedWriterBusy('Another feed writer or source switch is active')
    elif bind.dialect.name != 'sqlite':
        raise ValueError('Feed ownership requires PostgreSQL or SQLite')
    else:
        # Python's legacy SQLite transaction mode otherwise RELEASEs a first
        # SAVEPOINT without an outer transaction, defeating failure rollback.
        connection = db.connection()
        if not connection.connection.driver_connection.in_transaction:
            connection.exec_driver_sql('BEGIN IMMEDIATE')
        elif bind.engine.url.database != ':memory:' and db.info.get('feed_lock_transaction') is not db.get_transaction():
            raise ValueError('Acquire feed ownership before starting a SQLite write transaction')
        db.info['feed_lock_transaction'] = db.get_transaction()


def require_selected_source(db, feed, provider):
    cached = db.info.get('feed_source_checks', {}).get((feed, provider))
    if cached and db.get_transaction() is not None and cached[0] is db.get_transaction():
        return cached[1]
    lock_feed(db, feed)
    # Missing schema fails closed for both sources. Install the additive
    # migration before updated workers; never interpret DB failure as FMP.
    row = db.get(FeedSourceControl, feed, populate_existing=True)
    if (row.provider if row else 'fmp') != provider:
        raise FeedSourceMismatch('Requested publisher is not the selected feed source')
    if provider == DIRECT_PROVIDERS.get(feed) and (row is None or row.publish_since is None):
        raise FeedSourceMismatch('Direct publication has no immutable filing boundary')
    db.info.setdefault('feed_source_checks', {})[(feed, provider)] = (db.get_transaction(), row)
    return row


def canonical_writer(feed):
    """Guard canonical helpers, defaulting unscoped/legacy callers to FMP."""
    def decorate(fn):
        @wraps(fn)
        def guarded(db, *args, **kwargs):
            scope = db.info.get('feed_writer_scope', {}).get(feed)
            provider = scope[1] if scope and scope[0] is db.get_transaction() else 'fmp'
            require_selected_source(db, feed, provider)
            return fn(db, *args, **kwargs)
        return guarded
    return decorate


@contextmanager
def writer_transaction(db, feed, provider):
    if db.new or db.dirty or db.deleted:
        raise ValueError('Feed writer session has unrelated pending changes')
    previous = db.info.get('feed_writer_scope', {}).get(feed)
    try:
        control = require_selected_source(db, feed, provider)
        db.info.setdefault('feed_writer_scope', {})[feed] = (db.get_transaction(), provider)
        yield control
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        if previous is None:
            db.info.get('feed_writer_scope', {}).pop(feed, None)
        else:
            db.info.setdefault('feed_writer_scope', {})[feed] = previous


def select_feed_source(db, *, feed, provider, publish_since, expected_generation, reason):
    """Caller commits atomically; stale operations cannot undo a newer switch.

    Pausing is the safe rollback after direct publication. Returning to the
    legacy writer requires separate cross-provider reconciliation, so this
    interface deliberately cannot re-enable FMP after activation.
    """
    if feed not in DIRECT_PROVIDERS or provider not in {DIRECT_PROVIDERS[feed], 'paused'} or not reason.strip():
        raise ValueError('Select the matching direct provider/paused and record the operational reason')
    if not isinstance(publish_since, date) or isinstance(publish_since, datetime):
        raise ValueError('An explicit filing-date boundary is required')
    lock_feed(db, feed)
    row = db.get(FeedSourceControl, feed, populate_existing=True)
    generation = row.generation if row else 0
    if generation != expected_generation:
        raise ValueError('Source generation changed; inspect current state')
    if row and row.publish_since and row.publish_since != publish_since:
        raise ValueError('Publication boundary is immutable; reconciliation required')
    if row is None:
        row = FeedSourceControl(feed=feed)
        db.add(row)
    row.provider, row.publish_since = provider, publish_since
    row.generation, row.reason = generation + 1, reason.strip()
    row.updated_at = datetime.now(timezone.utc)
    db.info.pop('feed_source_checks', None)
    db.flush()
    return row
