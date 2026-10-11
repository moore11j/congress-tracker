from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest
from app.jobs import research_preflight as preflight

NOW = datetime(2026, 10, 11, 19, tzinfo=timezone.utc)


@pytest.fixture
def db(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE research_campaigns(id TEXT PRIMARY KEY, active BOOLEAN);
        CREATE TABLE research_campaign_items(campaign_id TEXT, status TEXT, generate_at TEXT);
        CREATE TABLE research_brief_drafts(status TEXT, scheduled_at TEXT);
        CREATE TABLE research_gsc_connection(id INTEGER, last_attempt_at TEXT);
        CREATE TABLE research_seo_settings(id INTEGER, config_json TEXT, owner_id INTEGER);
        CREATE TABLE research_seo_runs(day TEXT PRIMARY KEY, status TEXT);
    """)
    class Cursor:
        def execute(self, sql, params=()):
            self.result = conn.execute(sql.replace("%s", "?"), params)
        def fetchone(self):
            return self.result.fetchone()
    @contextmanager
    def cursor():
        yield Cursor()
    monkeypatch.setattr(preflight, "_cursor", cursor)
    yield conn
    conn.close()


def settings(db, **kwargs):
    config = {"enabled": True, "draft_time": "07:00", "timezone": "America/Los_Angeles", **kwargs}
    db.execute("INSERT INTO research_seo_settings VALUES (1,?,1)", (json.dumps(config),))


@pytest.mark.parametrize("seconds,idle", [(-1, False), (0, False), (1, True)])
def test_generation_due_boundary(db, seconds, idle):
    db.execute("INSERT INTO research_campaigns VALUES ('campaign',TRUE)")
    db.execute("INSERT INTO research_campaign_items VALUES ('campaign','pending',?)",
               ((NOW+timedelta(seconds=seconds)).isoformat(),))
    assert preflight.campaign_idle(generation=True, publication=False, now=NOW) is idle


@pytest.mark.parametrize("seconds,idle", [(-1, False), (0, False), (1, True)])
def test_publication_due_boundary(db, seconds, idle):
    db.execute("INSERT INTO research_brief_drafts VALUES ('approved_scheduled',?)",
               ((NOW+timedelta(seconds=seconds)).isoformat(),))
    assert preflight.campaign_idle(generation=False, publication=True, now=NOW) is idle


def test_inactive_or_unapproved_work_does_not_run(db):
    db.execute("INSERT INTO research_campaigns VALUES ('campaign',FALSE)")
    db.execute("INSERT INTO research_campaign_items VALUES ('campaign','pending',?)", (NOW.isoformat(),))
    db.execute("INSERT INTO research_brief_drafts VALUES ('scheduled_review',?)", (NOW.isoformat(),))
    assert preflight.campaign_idle(now=NOW)
    db.execute("UPDATE research_campaigns SET active=TRUE")
    assert not preflight.campaign_idle(now=NOW)
    assert preflight.campaign_idle(generation=False, now=NOW)
    db.execute("UPDATE research_campaign_items SET status='generated'")
    assert preflight.campaign_idle(now=NOW)
    db.execute("UPDATE research_brief_drafts SET status='approved_scheduled'")
    assert not preflight.campaign_idle(now=NOW)
    assert preflight.campaign_idle(publication=False, now=NOW)


@pytest.mark.parametrize("status", ["planning", "completed", "failed", "skipped"])
def test_daily_claims_skip_until_manual_requeue(db, status):
    settings(db)
    db.execute("INSERT INTO research_seo_runs VALUES ('2026-10-11',?)", (status,))
    assert preflight.seo_idle(now=NOW)
    db.execute("UPDATE research_seo_runs SET status='queued'")
    assert not preflight.seo_idle(now=NOW)


@pytest.mark.parametrize("enabled", [False, True])
def test_manual_queue_ignores_disabled_setting_and_draft_hour(db, enabled):
    settings(db, enabled=enabled, draft_time="23:59")
    assert preflight.seo_idle(now=NOW)
    db.execute("INSERT INTO research_seo_runs VALUES ('2026-10-11','queued')")
    assert not preflight.seo_idle(now=NOW)


@pytest.mark.parametrize("hour,idle", [(13, True), (14, False), (15, False)])
def test_local_draft_boundary(db, hour, idle):
    settings(db)
    assert preflight.seo_idle(now=NOW.replace(hour=hour)) is idle


@pytest.mark.parametrize("last,idle", [(None,False), ('2026-10-11T06:59:59+00:00',False),
    ('2026-10-11T07:00:00+00:00',True), ('2026-10-11T08:00:00+00:00',True)])
def test_search_console_utc_cutoff_preserved(db, last, idle):
    db.execute("INSERT INTO research_gsc_connection VALUES (1,?)", (last,))
    assert preflight.seo_idle(now=NOW) is idle


def test_calendar_midnight_and_dst(db):
    settings(db)
    db.execute("INSERT INTO research_seo_runs VALUES ('2026-10-31','completed')")
    # November 1 midnight Pacific is 07:00 UTC, before the 02:00 fallback.
    db.execute("INSERT INTO research_gsc_connection VALUES (1,'2026-11-01T06:59:59+00:00')")
    assert preflight.seo_idle(now=datetime(2026,11,1,6,59,59,tzinfo=timezone.utc))
    assert not preflight.seo_idle(now=datetime(2026,11,1,7,tzinfo=timezone.utc))


def test_custom_planner_timezone_and_missing_owner(db):
    settings(db, timezone="Asia/Tokyo")
    db.execute("INSERT INTO research_seo_runs VALUES ('2026-10-11','completed')")
    assert not preflight.seo_idle(now=NOW.replace(hour=23))  # Tokyo October 12 after 07:00
    db.execute("UPDATE research_seo_settings SET owner_id=0")
    assert preflight.seo_idle(now=NOW.replace(hour=23))


@pytest.mark.parametrize("malformed", ['not json', '{"timezone":"Bad/Timezone"}', '[]'])
def test_malformed_state_runs_existing_worker(db, malformed):
    db.execute("INSERT INTO research_seo_settings VALUES (1,?,1)", (malformed,))
    assert not preflight.seo_idle(now=NOW)


def test_missing_schema_and_connection_errors_do_not_drop_work(db, monkeypatch):
    db.execute("DROP TABLE research_seo_runs")
    settings(db)
    assert not preflight.seo_idle(now=NOW)
    @contextmanager
    def fail():
        raise RuntimeError("private connection details")
        yield
    monkeypatch.setattr(preflight,"_cursor",fail)
    assert not preflight.seo_idle(now=NOW)
    assert not preflight.campaign_idle(now=NOW)


def test_connection_is_bounded_readonly_and_closed(monkeypatch):
    import psycopg
    seen = {}
    class Cursor:
        def __enter__(self): return self
        def __exit__(self,*args): seen["cursor_closed"] = True
    class Connection:
        def __enter__(self): return self
        def __exit__(self,*args): seen["closed"] = True
        def cursor(self): return Cursor()
    def connect(dsn, **kwargs):
        seen.update(dsn=dsn, **kwargs)
        return Connection()
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://localhost/test")
    monkeypatch.setattr(psycopg,"connect",connect)
    with preflight._cursor(): pass
    assert seen["dsn"] == "postgresql://localhost/test"
    assert seen["closed"] and seen["cursor_closed"]
    assert seen["connect_timeout"] == 2
    assert "default_transaction_read_only=on" in seen["options"]
    assert "statement_timeout=1500" in seen["options"]
    assert "lock_timeout=500" in seen["options"]


@pytest.mark.parametrize("module,method", [("run_research_seo","seo_idle"),
    ("run_research_campaigns","campaign_idle")])
def test_idle_cli_does_not_import_application(module, method):
    code = f"""import runpy,sys,importlib.abc
from app.jobs import research_preflight
research_preflight.{method}=lambda **kwargs:True
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self,name,*args):
  if name in ('app.models','app.db','sqlalchemy'):raise AssertionError('Heavy idle import')
sys.meta_path.insert(0,Block())
runpy.run_module('app.jobs.{module}',run_name='__main__')
"""
    result = subprocess.run([sys.executable,"-c",code], cwd=Path(__file__).resolve().parents[1],
        env=os.environ.copy(), capture_output=True,text=True,timeout=15)
    assert result.returncode == 0, result.stderr
    assert "idle preflight=true" in result.stderr


@pytest.mark.parametrize("args,expected", [([], ["generation", "publication"]),
    (["--generation-only"], ["generation"]), (["--publish-only"], ["publication"])])
def test_due_campaign_cli_retains_existing_dispatch(monkeypatch, args, expected):
    import types
    from app.jobs import run_research_campaigns as job
    calls = []
    class Session:
        def close(self): calls.append("closed")
    monkeypatch.setattr(job, "campaign_idle", lambda **kwargs: False)
    monkeypatch.setattr(sys, "argv", ["campaign", "--limit", "7", *args])
    monkeypatch.setitem(sys.modules,"app.db",types.SimpleNamespace(SessionLocal=Session))
    def operation(name):
        def run(db, *, limit):
            assert isinstance(db, Session) and limit == 7
            calls.append(name)
            return {"checked":1}
        return run
    monkeypatch.setitem(sys.modules,"app.services.research_briefs",types.SimpleNamespace(
        run_due_research_campaign_generation=operation("generation"),
        run_due_scheduled_research_publications=operation("publication")))
    assert job.main() == 0
    assert calls == [*expected,"closed"]


def test_due_seo_retains_sync_then_plan_and_failure_exit(monkeypatch):
    import types
    from app.jobs import run_research_seo as job
    calls=[]
    class Session:
        def __enter__(self): return self
        def __exit__(self,*args): calls.append("closed")
    def sync(db): calls.append("sync"); return {"status":"ok"}
    def plan(db): calls.append("plan"); return {"status":"failed"}
    monkeypatch.setattr(job,"seo_idle",lambda:False)
    monkeypatch.setitem(sys.modules,"app.db",types.SimpleNamespace(SessionLocal=Session))
    monkeypatch.setitem(sys.modules,"app.services.search_console",types.SimpleNamespace(sync=sync))
    monkeypatch.setitem(sys.modules,"app.services.research_seo",types.SimpleNamespace(run_daily_plan=plan))
    assert job.main() == 1
    assert calls == ["sync","plan","closed"]
