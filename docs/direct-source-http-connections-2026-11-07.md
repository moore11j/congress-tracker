# Direct source downloads and database connections

Congress and SEC document collection previously kept a database transaction and
pool connection open while downloading each source. Issuer transcript collection
already committed its attempt before downloading. The shared collector now commits
one attempt before every document download, releasing its connection while waiting
for the network. The separately detached advisory lock still serializes collectors.

Congress and SEC documents retain their prior status until parsing succeeds or the
attempt fails. Failure handling records the failed refresh without incrementing the
already persisted attempt again. Whole originals, revisions, quarantine rules,
canonical events, publication receipts and source schedules are unchanged.

The regression test uses a real one-slot SQLAlchemy connection pool. The download
callback checks that no transaction or connection is held and successfully acquires
the only slot through another connection. It exercises a complete 13F original,
denial, malformed source, persistence failure, repeated collection, exactly one
attempt per request, retained source evidence and no canonical writes.

All 210 focused local checks pass, including Congress scan quarantine, Senate
collection, issuer fairness, Form 4 recollection, source workers and alert identity.
The four new cases fail against deployed baseline `3e51cf1e` because the download
callback finds an active transaction; all four pass with the repair. Linux release
validation, deployment and installed verification remain pending.

This repair addresses a demonstrated
connection retention path; it does not establish the cause of the earlier
production QueuePool timeout or prove sustained capacity.
