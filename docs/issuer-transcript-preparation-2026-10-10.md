# Prepared issuer transcripts

October 10 UTC, 2026. Free research-feed continuation. This is selective issuer coverage, not a broad consensus/transcript subscription replacement.

The official [Microsoft FY2026 Q4 event page](https://www.microsoft.com/en-us/investor/events/fy-2026/earnings-fy-2026-q4) provides its July 29 call transcript and Q&A. A bounded allowlisted request captured 273,560 bytes at 06:50:32UTC, original SHA256 `2d71e5babc839cc1d1b94c6e95a1c64f906c43d06b847e5d435c64031bbc63f1`. Reparse yields 58,238 normalized characters, text SHA256 `ab02ecf6a4b1f253f37f6f0d5be90661088a140e07944477e16c0090d8c9622e`.

The adapter reads saved source revisions only. It validates reviewed company/URL, fiscal year and quarter, publication-date evidence, source and parsed-text hashes, actual observation time and size bounds. It requires the latest reviewed period rather than silently serving an older configured period. Unsupported companies are explicitly unavailable. Date-only publication evidence remains a date; it does not become an invented midnight timestamp.

Canonical research preparation locks the security and checks existing transcript records across providers for that fiscal period. An exact existing source is reused without changing its provider, publication time or saved history. Conflicting or multiple records are held. A required publication boundary prevents historical backlog publication; changing an already-recorded boundary requires reconciliation. Original source provenance remains in the staged receipt.

`TRANSCRIPT_PROVIDER=issuer` opts the existing research worker into prepared issuer material. `ISSUER_TRANSCRIPT_PUBLISH_SINCE` must be an explicit date. FMP remains the default; no production selector or boundary has been changed. Selected issuer processing works with FMP blocked and has separate `issuer_transcript` coverage so legacy failures cannot relabel it. Existing extraction budgets and matching/delivery behavior remain in effect. This package itself does not send email or purchase services.

Validation on Python3.14.2:37 adapter/operational/SEC research checks pass. The actual Microsoft source replays into isolated SQLite with one canonical document, zero research events, no provider/model/email request and identical whole-database repeat. Fiscal/source tampering, future evidence, missing cutoff, existing-provider conflicts and duplicate records are covered. The real replay checks preparation, not model extraction quality or broad company coverage.

## Separate proposed collection schedule

Automatic approval review rejected adding persistent recurring collection because that schedule was not specifically authorized, even though the proposed flag defaulted off. No cron or job change was applied.

The concrete proposal is an hourly issuer-only preparation job at minute13, capped at two reviewed pages per run, with each page rechecked no more than once per24hours. It would use the existing shared collector lock, database-pressure guard and exact approved-host registry. It would prepare source evidence only. Research publication would remain separately controlled by the provider selector, explicit date boundary and cross-provider reconciliation. Approval of this schedule would not authorize autonomous code changes, deployments, other recurring tasks or customer emails.

Until that approval, validation can use the existing one-time collector command and saved-source replays. Institutional full-cohort validation and the wider FMP retirement remain separate; Massive prices and paid subscription decisions stay last.


## Resumed bounded schedule, October 10

The owner resumed and approved infrastructure consolidation. The issuer follow-up is rebased onto the current two-service release. Hourly minute13 collection joins the serialized data lane, at most two reviewed pages per run and no page fetched more than once per24hours, including failed-page retries. Removed registry entries cannot be fetched from stale staging. ISSUER_TRANSCRIPT_WARMING_ENABLED is enabled in the reviewed deployment configuration; transcript publication/provider selection remains separately gated.

When publisher HTML changes without transcript changes, preserve the original canonical publication receipt and validate its original source revision, hash and observation time. Changed transcript identity/boundary remains held. No duplicate research document, new historical availability time or automatic customer email is introduced.

Python3.14.2 validation:35 focused issuer/schedule/runtime checks pass, four Linux-only runtime checks skipped (the unchanged runtime passed all nine Linux checks in PR531). Shared direct-source tests initially85pass/two fail because old calendar doubles return two values while the deployed contract returns three; the doubles are corrected and all14 actual consumer checks pass. Across the focused groups122 checks now pass, with four platform skips; this is not a full-suite claim. Captured Microsoft FY2026Q4 source replay retains58,238 text characters, one canonical document, identical repeat state and unchanged original receipt after a harmless HTML revision. Source/model/email/production writes allzero during this isolated replay. Fly config validates. Current registry is Microsoft only, not broad-market transcript parity.

Deployment, runtime flag/hash checks and the first scheduled collection receipt remain pending at this checkpoint. FMP remains enabled; Massive prices last.


## Deployed and first scheduled source receipt

PR533/fa7654f9 merged as7250a59391c60e6174ff12f5557b2afe0cc6eaec; workflow38082627973 succeeds. Eight source/schedule/runtime hashes match the sole combined host. At20:13:09UTC run86 completes both reviewed Microsoft pages with zero errors, zero public writes and one revision per document. Q4 live reader is prepared with Q&A, original expected text SHAab02ecf6a4b1f253f37f6f0d5be90661088a140e07944477e16c0090d8c9622e; current HTML SHA differs from the earlier capture, reinforcing the need to preserve text identity independently. Scheduler records exit0. Transcript publication remains FMP and registry coverage remains Microsoft only. Daily recheck/retry bounds are tested; a second ordinary hourly production run is not yet observed.

First broad provider-usage query included legacy-labeled cache hits, so it is not a network-call inventory. Refined20:12UTC audit excludes cache records/content-write counts and confirms recorded FMP provider authorizations for quotes, operational transcripts, institutional analytics, sector snapshots, valuation targets/DCF/statements and float. Such authorization telemetry is not proof of a completed HTTP response, and a one-hour window is not a full endpoint inventory. SEC filing cache activity is not falsely treated as an FMP request. News/analyst selectors remain Finnhub; official House/Senate/Form4/SEC press owners remain selected.


## Next reviewed source candidate

October10 web verification finds NVIDIA hosts a sixteen-page corrected Q2FY2027 call transcript dated August26,2026 at https://investor.nvidia.com/files/content_files/TRANSCRIPT_-NVIDIA-Corp-NVDA-US-Q2-2027-Earnings-Call-26-August-2026-5_00-PM-ET.pdf . It includes management discussion and analyst questions and identifies FactSet CallStreet as transcript publisher. Current issuer parser accepts HTML only. Next implementation needs bounded PDF extraction, exact fiscal/date/company verification and preserved publisher provenance before registry inclusion; no coverage or activation claim. Alphabet's current Q2 event URL was inaccessible to the web reader, so no transcript content was verified there.

Post-release20:14:33 check retains exactly one API and one production DB, exact hashes and a drained queue. Minimal/full feed0.591/0.549s, NVDA profile0.614s; newly generated AAPL/NVDA contexts3.1/1.818s. This distinguishes uncached generation from earlier faster cached samples; all six requests return200.


## NVIDIA PDF implementation and exact source replay

October10,20:18UTC capture verifies the official NVIDIA Q2FY2027 PDF,16pages/289,667bytes. The reviewed original URL redirects to exactly https://s201.q4cdn.com/141608511/files/content_files/TRANSCRIPT_-NVIDIA-Corp-NVDA-US-Q2-2027-Earnings-Call-26-August-2026-5_00-PM-ET.pdf . Initial transport correctly refused that unreviewed destination. The adapter now accepts explicitly reviewed full CDN destinations only after an issuer redirect, not arbitrary paths on that CDN or direct CDN entry.

PDF input is capped at2MB,40pages,5MB decoded content and2M extracted characters; encrypted, malformed, unreviewed or identity-mismatched material is held. Company/date/fiscal period and FactSet CallStreet publisher evidence are checked. HTML parsing retains its existing text contract. The registry adds NVIDIA alongside Microsoft without increasing the global two-page/hourly or daily page-recheck limits. Public transcript selection stays gated.

Actual captured PDF SHAa8be2bac4fd08ca18fc4bccbb165cd48237166ac5b4e211e779fc9f89ee35e16;62,532 normalized characters, text SHA8f0482ec48d776b1149b9e75a41dcd53fa3b6b947f7ea74b2c1abd74b2659e1d. Isolated canonical replay preserves one document, original source receipt and publisher attribution across identical-text PDF revisions; source/model/email/production writes allzero. Publisher changes are held. Across focused groups157 checks pass on Python3.14.2:67 issuer/direct-source,53 worker/monitoring/digest and37 research checks. Initial publisher-mutation fixture exercised an earlier parser mismatch; it was refined to a real new PDF revision to verify the publication guard. No full-suite claim.

Deployment and live NVIDIA collection/readers remain pending at this checkpoint. Institutional session92282 continues independently; no institutional publication or FMP shutdown.
