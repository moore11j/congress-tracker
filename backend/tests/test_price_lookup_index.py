import json
from pathlib import Path
import sys

import pytest
from sqlalchemy import create_engine, text
from app.jobs import migrate_price_lookup_index as migration


def test_price_index_preserves_mixed_case_history_and_removes_scan():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE price_cache(symbol TEXT,date TEXT,close REAL,PRIMARY KEY(symbol,date))"))
        rows = [{"symbol": "OTHER" + str(n), "date": "2026-10-01", "close": 1} for n in range(500)]
        rows += [{"symbol": s, "date": d, "close": v} for s,d,v in [
            ("aApL","2026-10-01",10), ("AAPL","2026-10-02",11), ("aapl","2026-10-03",12),
            ("BRK.B","2026-10-01",20), ("brk.b","2026-10-03",22)]]
        conn.execute(text("INSERT INTO price_cache VALUES(:symbol,:date,:close)"), rows)
        query = "SELECT symbol,date,close FROM price_cache WHERE upper(symbol)=:symbol ORDER BY date"
        before = {s: conn.execute(text(query), {"symbol": s}).all() for s in ("AAPL","BRK.B","ABSENT")}
        conn.execute(text("CREATE INDEX ix_price_cache_upper_symbol_date ON price_cache(upper(symbol),date)"))
        after = {s: conn.execute(text(query), {"symbol": s}).all() for s in before}
        assert before == after
        assert len(after["AAPL"]) == 3 and len(after["BRK.B"]) == 2 and after["ABSENT"] == []
        plan = str(conn.execute(text("EXPLAIN QUERY PLAN " + query), {"symbol":"AAPL"}).all())
        assert "ix_price_cache_upper_symbol_date" in plan and "SCAN price_cache" not in plan
        newest = conn.execute(text(query + " DESC LIMIT 1"), {"symbol":"AAPL"}).one()
        assert newest == ("aapl","2026-10-03",12)


@pytest.mark.parametrize("change", [{"valid":False},{"ready":False},{"definition":"different index"}])
def test_existing_invalid_or_conflicting_index_is_not_rebuilt(monkeypatch, change):
    existing = {"valid":True,"ready":True,"definition":migration.DEFINITION,"bytes":1,**change}
    monkeypatch.setattr(migration,"inspect_index",lambda conn:existing)
    with pytest.raises(ValueError,match="inspect before retrying"):
        migration.create_index(object())


def test_completed_build_repeats_without_ddl(monkeypatch):
    existing = {"valid":True,"ready":True,"definition":migration.DEFINITION,"bytes":1}
    monkeypatch.setattr(migration,"inspect_index",lambda conn:existing)
    assert migration.create_index(object()) == {"created":False,"index":existing}


def test_manual_maintenance_shares_data_lane_and_is_not_scheduled(tmp_path):
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"scripts"))
    import scheduled_jobs as jobs
    path=tmp_path/"manifest.json"
    path.write_text(json.dumps({"import":{"lane":"data"}}))
    manifest=jobs.load_manifest(path)
    key="maintenance-price-lookup-index-v1"
    cron,_=jobs.compile_schedule("* * * * * import-command",Path("worker"),path,tmp_path/"queue")
    assert key not in cron and "migrate_price_lookup_index" not in cron
    with jobs.database(tmp_path/"queue") as db:
        assert jobs.claim(db,"data",manifest) is None
        jobs.enqueue(db,"import",manifest["import"],now=1)
        jobs.enqueue(db,key,manifest[key],now=2)
        running=jobs.claim(db,"data",manifest,now=3)
        assert running["key"] == "import"
        assert jobs.claim(db,"data",manifest) is None
        jobs.finish(db,running,0)
        assert jobs.claim(db,"data",manifest)["key"] == key


def test_apply_requires_serialized_worker(monkeypatch):
    monkeypatch.setattr(sys,"argv",["migration","--apply"])
    monkeypatch.setattr(migration,"engine",type("Engine",(),{"dialect":type("Dialect",(),{"name":"postgresql"})()})())
    monkeypatch.delenv("WALNUT_JOB_TOKEN",raising=False)
    with pytest.raises(ValueError,match="serialized"):
        migration.main()
