# Model startup cost, October 11, 2026

The full FMP migration remains gated on sustained reliability. PR577's idle research saving is verified; the four frequent SEC jobs still rebuild the full model registry separately. A proposal to batch those jobs was not implemented, because a narrower change preserves existing staggered schedules, timeouts and process isolation.

SQLAlchemy currently receives postponed string annotations for every mapped model field. Native annotations avoid repeated string evaluation. Explicit datetime.date qualification prevents a column named date from shadowing the Python date type, including Python3.14 deferred-annotation behavior. No mapped field, database type, nullability, index, key, default, event hook or business rule is intentionally changed. No schema migration, flag, job schedule, resource change or source activation is included.

## Evidence

An isolated candidate benchmark on production Python3.12.15 reads model source but forces an in-memory SQLite URL; no product jobs, providers or production database are used. Two interleaved baseline imports consume0.90645/0.84286 CPU seconds, versus0.72417/0.74019 for native annotations. Mean0.87466 to0.73218 is a16.3% reduction in this import component, not total job or total API CPU. All134 PostgreSQL table definitions and indexes match exactly. Local Python3.14 measurements are variable, and one exact-file comparison during other tests is slower; no universal speedup claim.

Exact current-main versus candidate comparison also configures all134 ORM mappers. DDL SHA256 is2f67ad038fc64719d9654b898961cf8fbcc14532b48c0e39d27e717bcb9ef15e; ORM attribute/column/type/nullability/key SHA256 is893413ce98a790f506c3835eeee9b804011cb2d4dd238b617200e941bde7dba4. Both signatures match. The tracked measure_model_startup.py script makes the same check reproducible in fresh isolated processes.

All214 selected tests pass on Python3.14.2: SEC research/metadata/earnings/fundamental preparation, direct-feed alert parity, email digests, watchlist delivery, strategy model charts and confirmation bundles. This is not a full-suite or real recipient delivery claim. Initial benchmark harness escaping was corrected; the first native candidate exposed date name shadowing, which the qualified date annotations resolve before the successful comparisons.

A separate24-hour read-only capacity sample finds API mean0.1548CPU/p950.2310 and database mean0.1200/p950.4781. It includes prior diagnostics/deployments and cannot establish ordinary weekday demand. Recent database idle samples do not justify a downgrade; no resource swap or spending change is proposed/applied. Fly documentation confirms shared2 permits0.125CPU sustained total, with burst credit above that allowance: https://docs.fly.io/machines/cpu-performance .

Deployment and ordinary exact-release operation remain pending. Local artifacts: probe_model_annotations_oct11.json, model-annotations-linux-oct11.json and compute-fit-1791748776.json in the primary direct-feeds evidence folder; artifacts/model-startup-oct11/exact-model-parity.json in the managed checkout. Next: verify this release and sustained capacity, then institutional growth/capacity and remaining core/transcript/consumer/final Massive/global FMP/billing gates. FMP remains active.
