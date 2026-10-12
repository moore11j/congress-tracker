# Senate backup evidence preparation, October 12, 2026

## Scope and implementation

The official Senate discovery outage still requires either restored access or a reliable independent replacement before FMP shutdown. This change implements the evidence-conversion boundary for the free [community snapshot](https://huggingface.co/datasets/austin-starks/congressional-stock-trades). It does not activate a fallback collector or publisher.

`backend/app/services/senate_backup_evidence.py` verifies complete partition byte counts and SHA256 checksums, validates snapshot identity/schema/audit/freshness, and prepares complete individual filings without transport or database writes. The default maximum snapshot age is 24 hours, compared with the publisher's 20-hour refresh target. This is a freshness gate, not a delivery guarantee.

Preparation retains the full secondary filing and row records. It preserves printed and separately resolved tickers, owner, partial-sale status, action, instrument, amount ranges, transaction/disclosure dates, original-file hash claims and snapshot/observation clocks. It does not infer stock classification from a ticker when the source omits the asset type. A separately resolved ticker remains an unverified candidate until independent resolution evidence exists. Snapshot end-of-day availability is retained as a reported value, while Walnut's observation is separately labeled.

Secondary row IDs have their own namespace and do not become official printed row IDs or canonical economic-event IDs. `source_line_ref` remains absent. An optional original-binding path verifies the exact original bytes, reparses them through Walnut's existing official parser, compares complete economic multisets and produces an explicit crosswalk. Identical lots with ambiguous row assignment remain held. Original matching is stronger evidence for available cases; the later secondary publication design must explicitly retain secondary provenance for cases without original bytes, rather than falsely presenting them as official extraction.

No preparation result is publishable yet. Independent member/term checks and canonical event reconciliation remain unimplemented for this secondary source. Paper reports, amendments, unresolved symbols and instruments remain explicit holds. The existing official source and canonical publication path are unchanged. There is no new scheduler entry, paid provider, database schema or dependency installation.

## Actual source validation

The saved snapshot was generated and source-checked at `2026-10-11T11:32:37.357Z`. Both complete 2026 partitions were verified before decoding:

| Partition | Bytes | SHA256 |
|---|---:|---|
| Filings | 74,222 | `5ed7cb5418449ddf7c669aa63ba5373f4feb147191d735cc634fc4f86eac06d7` |
| Reported trades | 233,755 | `f540da56262647cd718fa697330521454c309c3c7c8a4d61e14de6fa82445b37` |

The final audit at 03:43:09 UTC prepares **135 Senate filings and all 2,176 reported rows**, with zero conversion errors and identical results when input row order is reversed. These are preparation counts, not a claim of complete official discovery or accuracy of every extraction. Receipt: `senate-preparation-1791776589.json`, output SHA256 `cbcdb97e627e0c89985e3f20690a859550f0f02a5cdb9c0d5c63c58ca84c2d2a`.

Explicit row holds include 209 secondary ticker resolutions needing independent evidence, 418 stock rows without an accepted printed ticker, 566 instrument reviews, two action reviews and one malformed printed ticker. Categories overlap. The malformed value is `-- AMCR` on a BERY/AMCR exchange description; it is not silently rewritten into a stock purchase/sale. The 17 amendments and nine paper reports retain their separate holds. All rows remain present.

Both actual October originals match the dataset's exact original hash claims:

- Fetterman, October 7: `702112b68bdcd99afae857320e303730607ddc84bf34954af08b7d5b71b44c76`, two dependent-child corporate-bond purchases.
- Whitehouse, October 1: `6d2b4767bb94d26fe7f7db1d1eec0629e35fde4508b4f58ac35a01517bfb3dfa`, five stock partial sales.

All seven secondary rows map to the existing official normalized hashes without guessed row offsets. The secondary order differs from the official printed order, which validates the need for explicit bindings. Repeating the complete preparation and bindings is identical. No network, production writes or emails occur.

The tracked fixture contains the seven published secondary records and base64-encoded exact original bytes, so Git newline conversion cannot invalidate the source hashes. Initially, a test used the repository's reduced HTML fixtures; the hash guard correctly refused them. Tests were corrected to use actual saved originals, and the assumed ticker order was corrected from the source data. No guard was weakened. A first copy command had an incorrect workdir-relative destination and ran no tests; corrected absolute destinations were used.

## Tests and remaining integration

**104 tests pass on Python 3.14.2**, covering the new boundary, Senate parsing, canonical Congress publication and reconciliation. Checks include complete/tampered/duplicate partitions, parent-row disagreement, missing/duplicate rows, stale/future/timezone-less snapshots, printed versus resolved symbols, non-equity preservation, immutable preparation inputs, original tampering, incompatible economics, ambiguous identical lots and repeat stability. This is focused coverage, not a full-suite pass or a production fallback receipt.

Next implement the bounded download/decoding and staging path, independently resolve member terms, reconcile secondary identities against existing official/FMP events with preserved multiplicity, and verify recovery, amendment behavior and no-send consumers before activation. The public snapshot does not itself expose original HTML/PDF bytes. Its freshness and completeness need continued observation; original-source matching of two filings is not blanket validation of all 135. Quiver is not used.

The separate institutional replay `87489` has completed its first pass: 574 documents, 137 published current comparisons and 142 events. Its full-state repeat and consumer checks remain live and unverified. Earlier source/provider gates remain in force; FMP is still active. Production remains `d2fa4296` from PR599, with no deployment or source change for this preparation work.


## Review and current official-source checkpoint

Draft [PR600](https://github.com/moore11j/congress-tracker/pull/600) is open at `637a602357eaed1e137b4fb79bb52d70828bf4db`; CI is pending, no release claimed. A fresh read-only production audit at03:44:28 (`senate-runtime-1791776668.json`) finds32 failed official discoveries sinceOctober10,20:17. Latest run202 at03:17 still returns403 on the notice page. Last complete official discovery remains run85, October10,16:22. Official Senate generation2 and both saved originals are unchanged. No manual source retry or fallback activation was performed.

## Bounded transport and isolated staging, October 12, 03:56 UTC

Draft PR600 now includes `senate_backup_collection.py`. It validates the manifest before download, pins partition URLs to the manifest response's repository revision, permits only observed Hugging Face HTTPS hosts, limits redirects and streaming bytes, and requires the complete checksum-bound partition set. Limits are two explicitly requested years, eight files, 20 MB total compressed and decoded evidence, a 180-second download deadline, 100,000 rows per partition and 2,000 Senate filings. Crossing any limit holds the collection rather than claiming a complete truncated result.

Parquet decoding uses pinned DuckDB 1.5.6, imported only by the collection function. It uses one thread, a 128 MB engine memory limit, no spill, no automatic extensions, explicit temporary-file access and locked configuration with external access disabled. A 30-second interrupt bounds each decoder session. These are engine limits, not a claim of total Python process peak memory. [Official DuckDB configuration](https://duckdb.org/docs/current/configuration/overview) documents the settings. No downloaded source package is executed.

Complete conversion precedes a savepoint transaction in existing isolated direct-feed tables. Exact manifest and partition bytes are retained once per hash; each filing retains its full secondary records, original-hash claim, snapshot link and explicit secondary provenance. Identical evidence retains its first observation and revision. Public Event and EmailDelivery tables are unchanged. Staging rechecks freshness at write time. Independent member terms, canonical matching, publisher/job integration and consumer acceptance are still required. No production collection schedule or publication was added.

Actual public download at 2026-10-12T03:55:52Z used dataset revision `e51749cbfbed3eda5fd6de4c6acc03c27f706c24`. The manifest remains `5769a981fd6eb48cea17133e3554644357a438e9530d3511dae8306f63a56f9a`. Both partitions total307,977 bytes;135filings/2,176rows stage into138documents/revisions in isolated SQLite. Full repeat is identical with zero new revisions, public events or emails. The combined download, three decodes and two staging passes use6.156CPU/7.328wall seconds locally on Python3.14.2; this is not production capacity proof. Saved receipt: `senate-download-1791777354/receipt.json`.

126 focused checks pass, including checksum and schema rejection, missing/extra files, path escape and redirect rejection, missing trades, duplicate filings, orphan rows, wrong disclosure year, stale-at-write rejection, row bounds, restricted decoder access, atomic rollback and identical repeated staging. Initial runs exposed Windows temp access, an incorrect fixture API option and DuckDB setting-order mistakes; all were corrected before the final pass. The first real download rejected the new `us.aws.cdn.hf.co` hostname. The public Hugging Face redirect was inspected and that exact host added; no wildcard or arbitrary-host fallback was introduced. Initial sandbox pip access failed; an approved scoped install succeeded. No production dependency install or deployment yet.

The institutional replay87489 also finished with exit0. Its574-source/647-reference cohort produces137current comparisons and142events,64verified continuity checks, identical full-database repeat and identical12monitoring/8signal/142watchlist no-send previews. Duplicate alert creation is refused and EmailDelivery remains0. Receipt `institutional-full-current-listings-oct12.json`, state SHA256 `0250dacce8bd03e88861220a99226ef9ccc10158ce7262d4f0a1d051b2d87140`. The49held and226waiting sources remain explicit; this does not establish full source coverage or bounded production writer capacity. FMP and existing source selections remain unchanged.
