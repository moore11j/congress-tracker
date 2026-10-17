# Shared independent financial calculations, October 17, 2026

## Problem and implementation

The SEC projector and inline acquisition parser restricted revenue-independent recovery to ANIX and BCDA. Other verified issuers lost current ratio, return on equity and free cash flow when trailing revenue was unavailable. Missing or conflicted revenue still does not justify suppressing separately supported fields.

The shared path now accepts any verified issuer with current USD US GAAP inputs. It retains exact latest completed domestic filing accession/date matching, 180-day period freshness, coherent trailing periods, positive equity/liability denominators and nonnegative reported capital expenditure. Missing capital expenditure is not zero. Current revenue, margins and growth remain unavailable when unsupported. Newer transition or foreign annual filings prevent fallback to an older domestic filing; their period/accounting rules remain a separate work category. Reconciled parent-income/equity and continuity evidence are retained. No new per-ticker application rule is introduced.

Existing complete-revenue calculations keep their prior path. Existing normal TTL/queue limits control refresh; the universe cache version is not bumped. The two legacy per-symbol preparation version entries remain for compatibility and refresh when their implementation version changes; they are no longer an eligibility allowlist. Public core fundamentals remain FMP. No scheduler, quote source, resource size, delivery preference or alert metric registry changes.

## Entire held-cohort evidence

Read-only production metadata receipt `artifacts/sec-coverage-shared-cohort-1792202683.json` at 02:04:43 UTC identifies all 239 scopes currently carrying unsupported financial coverage, including expired entries. This differs from the earlier 169 fresh holds; neither count is the entire 1,774-symbol universe.

Complete public companyfacts and submissions for all 239 were captured with URL/hash manifests. `artifacts/shared-financial-cohort-oct17/results.json` is terminal, and `comparison.json` compares identical source inputs against deployed baseline `2fdacf17445f9c211002e40c3a9640497e121b04` and the candidate:

- 84 previously held issuers recover 165 independent fields: 63 current ratios, 64 ROE values and 38 free-cash-flow values.
- 155 remain held with more specific reasons: 124 reporting-form/period policy cases, 17 without a supported completed filing, 12 without independently supported metrics, one stale filing and one incomplete filing identity.
- The 124 form cases comprise 105 20-F, six 20-F/A, 11 40-F, one 40-F/A and one 10-KT. This identifies shared foreign reporting support as the largest next category, not 124 independent ticker adapters.

This is a projection comparison over complete public facts/submissions, not proof of live acquisition for all 84 or full field equivalence. An attempted ARCC original acquisition exceeds the current 20 MB parser/download-test bound, so its live path remains explicitly unverified. No source bound was relaxed. No private production financial values or customer records were exported.

## Consumer and release checks

Complete public facts, submissions and original current reports for ALOY and BCAX are retained as six compressed fixtures with source hashes. They exercise actual acquisition, prepared cache, exact repeated cache reads, score context and supported watchlist current-ratio/ROE alert lifecycles. Missing-to-available and rollback rebaseline without false alerts; a subsequent synthetic genuine threshold crossing triggers once and repeated evaluation does not duplicate it. BCAX free cash flow is checked through calculation/preparation/score inputs; it is not an existing custom-alert metric and the registry is unchanged.

Adjacent regression tests cover full revenue calculations, reporting identity, stale/future/ambiguous filings, missing/conflicted inputs, tax-series separation, currencies, continuity, parent income/equity and inline nil/precision rules. Expanded run passes 315 cases with one extra test incorrectly requesting an unsupported free-cash-flow custom alert; that invalid test parameter is removed. Final targeted rerun passes all31 tests on Linux Python3.12.3 in9.62seconds. There are315 distinct passing cases overall, not346. Earlier initial test failures exposed the second inline-parser allowlist and it is repaired in shared code.

The 14-file installed-runtime verifier is prepared and passes locally over the complete originals, actual acquisition/preparation and exact SQLite repeats with network forbidden. This is not a production receipt. Review, merge, backend deployment and ordinary refresh remain pending. Core FMP remains selected. Full financial coverage, institutional publication, transcripts/analysts/valuation, all scheduled consumers, sustained capacity, final Massive activation, zero FMP traffic and billing closure remain open.
