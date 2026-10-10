# Cold ticker build diagnosis, October 10

After PR539, public AAPL context generation took6.35s while unchanged-generation cached reads took0.357/0.275s. NVDA new generation took2.03s. This is an unresolved cold-path sample, not proof of database contention or a transcript regression.

The existing main.py builder already measures profile, quote, signals, source context, confirmation and build-slot wait, but emits the entire breakdown at INFO. The combined server starts uvicorn with access logging and no app logger/root INFO configuration. Reading the most recent100 Fly entries yields no retained timing rows. Initial log reader attempts assumed line-delimited/one-object JSON; corrected decoding handles concatenated pretty-printed objects without dumping unrelated logs.

Emit that existing breakdown at WARNING only when total build time is at least2000ms; fast builds remain INFO. No global logging-level change, user IDs, source text, private thesis content or credentials. Response payloads, cache freshness, score/price semantics and provider selections are unchanged. Twelve existing canonical entitlement-projection/public-cache checks pass on Python3.14.2; no full-suite claim. This release also carries completed transcript deployment receipts into tracked reports.

Deploy and capture actual slow-stage evidence before selecting a repair. Do not infer a database resize or mask missing/stale data. FMP remains enabled; Massive last.
