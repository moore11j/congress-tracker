"""Measure trusted model source in a fresh process with an isolated database URL.

Run separately for each revision. This imports declarations only, then records
full PostgreSQL DDL and configured ORM signatures; it never runs product jobs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import types


def main():
    backend = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=backend / "app/models.py")
    args = parser.parse_args()
    source = args.source.read_text(encoding="utf-8")
    os.environ["DATABASE_URL"] = "sqlite://"
    sys.path.insert(0, str(backend))
    if "app.models" in sys.modules or "app.db" in sys.modules:
        raise RuntimeError("Use a fresh process for model measurements")
    module = types.ModuleType("app.models")
    module.__file__ = str(backend / "app/models.py")
    sys.modules["app.models"] = module
    started = time.process_time()
    wall = time.monotonic()
    exec(compile(source, module.__file__, "exec", dont_inherit=True), module.__dict__)
    cpu = time.process_time() - started
    elapsed = time.monotonic() - wall
    module.Base.registry.configure()
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateIndex, CreateTable
    statements = []
    for table in module.Base.metadata.sorted_tables:
        statements.append(str(CreateTable(table).compile(dialect=postgresql.dialect())))
        statements.extend(sorted(str(CreateIndex(index).compile(dialect=postgresql.dialect()))
                                 for index in table.indexes))
    mappings = sorted((mapper.class_.__name__, [(prop.key,
        [(column.name, str(column.type), column.nullable, column.primary_key) for column in prop.columns])
        for prop in mapper.column_attrs]) for mapper in module.Base.registry.mappers)
    print(json.dumps({"python": sys.version, "cpu_seconds": cpu, "wall_seconds": elapsed,
        "source_sha256": hashlib.sha256(source.encode()).hexdigest(),
        "tables": len(module.Base.metadata.tables), "mappers": len(mappings),
        "ddl_sha256": hashlib.sha256("\n".join(statements).encode()).hexdigest(),
        "orm_sha256": hashlib.sha256(json.dumps(mappings, sort_keys=True).encode()).hexdigest()}))


if __name__ == "__main__":
    main()
