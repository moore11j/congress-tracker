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
