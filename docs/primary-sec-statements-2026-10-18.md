# Primary SEC financial statements, October 18, 2026

## Scope and current state

Shared source repair prepared on `codex/primary-statement-sources`, based on PR722 (`4fd433abe16db3cb1d8d36ea12f0512d99ff9f18`). Production activation is not yet verified. Core fundamentals remain FMP. This changes generic statement discovery and normalization, without a ticker allowlist, more source requests, new schedules or hosting.

## Source defects and repair

The reader previously required a linked exhibit even when the SEC primary document itself contained the complete statements. Financial table-of-contents links to that same primary were also treated as invalid external exhibits. The repair admits the exact directory-listed primary only with matching complete source/cover bytes, preserves accession/CIK/date identity and original hashes, and ignores self-document contents links in both discovery paths. Linked exhibits still require an actual link and same-accession source identity. Equal-rank competing sources and newer unqualified statements remain held.

Explicit US accounting wording now includes United States generally accepted accounting principles and the explicitly named US GAAP basis of directly comparable financial measures. Bare GAAP and negated declarations stay held; non-GAAP tables remain excluded. Summary before a consolidated statement heading is treated as a heading adjective without erasing a company name. Trailing numeric note references are removed only for metric matching; original row labels remain in evidence. Semantic parentheticals such as adjusted, USD or per share remain meaningful.

The primary uses the existing20MB statement bound; nonfinancial covers retain2MB. Existing three-cover/six-unique-original limits remain. Both new complete issuer chains use three originals, with no duplicate fetches.

## Complete-source results

Ten compressed complete originals and a URL/length/SHA256 manifest are retained in `backend/tests/fixtures/sec_primary_statements`. TK primary hash `fbe774d01c9f581b35695d2cb2822c6399932b2e943d0ff0e9055bb6ab4d8777` provides16 native observations, two semiannual revenue rows463333000/664935000USD and current ratio1524874000/147174000. TNK primary hash `b0a31bd668b6f1727cf97f3083bd092724a29a71a16fd74ff3997ca2fccb5678` provides22 native observations, two quarterly revenue rows232866000/379508000USD and ratio1470123000/147219000. Both current periods end June30 and primary filings are July31. Periods come from statements, not filenames. Unsupported parent income/EPS/free cash flow/trailing-year values stay null; partial coverage is explicit.

## Verification before release

Initial Linux suite:191 passing tests covering discovery, complete-source extraction, identity, accounting/currency, preparation/cache reuse, and source-to-score/watchlist recovery, crossing, repeat and rollback. After adding two negation cases, final affected suite:102 passing.193 distinct tests across the two runs, not293 unique tests. Local verifier rehearsal:20 source hashes/10 originals, Python3.14.2, no network or production records, isolated SQLite, exact repeated prepared panels and one current-ratio metric per issuer. Linux tests use the existing Python3.12 runtime. Production installed verification remains required.

Full49-scope before/after source replay and VTEX follow-up are recorded under `artifacts/primary-statement-oct18/` and `artifacts/primary-vtex-followup-oct18/`. Final comparison must be reviewed before release. The first pass retains46 exact results, recovers TK/TNK, and exposes a VTEX missing-original capture gap. Two exact already-discovered VTEX URLs were subsequently fetched once each, HTTP200, under existing byte/time bounds; no retries or recurring collector were added.

Read-only pre-release receipt `primary-statement-scheduled-1792300909.json` shows no prepared public-history panels. Separate core refresh deadlines are October18 19:31UTC forTK and19:36UTC forTNK. No cache eviction or forced refresh. Ordinary public preparation, exact source proof, cache repeat, protected deployment preflight and installed hashes remain release acceptance tasks.

## Remaining migration gates

This is shared parser coverage progress, not whole-universe financial compatibility or authorization for global FMP shutdown. Institutional publication, remaining source/financial/transcript gaps, all monitoring/watchlist/daily/weekly/ranking acceptance, sustained heavy-load capacity, final Massive activation, zero-FMP egress and billing/cancellation verification remain open. Previous hosting clarification was no migration progress; this work provides tested source normalization and concrete consumer evidence. No customer emails, paid features, videos or infrastructure changes.

## Final cohort comparison, October18 05:24UTC

Final49-scope replay completes with46 byte-equivalent result objects (public/core/errors/request order), TK/TNK partial financial recoveries, and VTEX progressing past internal anchors to an explicitly unavailable panel and held core. VTEX has zero financial fields admitted; it is not a third recovery. Three source fetches for each changed chain remain within the unchanged six-original bound. Complete response projections, errors and request lists are retained in primary-statement-oct18/cohort-comparison.json; CPU67.875seconds, zero provider requests/production writes. Final affected102Linux checks pass after the accounting negation guard.
