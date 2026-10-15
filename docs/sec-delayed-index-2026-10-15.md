# Delayed SEC daily-index entries, October 15, 2026

## Source evidence and failure

Production collector run610 reports the same error for Form4 and13F discovery: SEC daily index contains an unexpected filing date, index date October14. The exact public October14 daily index was fetched once through the existing identified source client. Its header says Last Data Received: Oct14,2026. It contains543 retained-form accessions:542 datedOctober14 and one13F-HR datedOctober13. The collector previously rolled back discovery for the whole day when it reached that earlier row.

The earlier row is StoryOne LLC accession0002079652-26-000005. Its original SEC submission header independently confirms accession, form13F-HR and filed-as-of20261013. The preceding October13 daily index does not contain that accession. This is directly verified delayed dissemination, not permission to rewrite filing dates or accept any wrong-day response.

Public originals:
- https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/master.20261014.idx SHA256 74a2830b81ee2ad818321981b68c15539c86eb6e268f57580d94894ea6e14a02,289582bytes.
- https://www.sec.gov/Archives/edgar/data/2079652/0002079652-26-000005.txt SHA256 7a874f3130226946baaf2399a1e6869c0e6cff3b68d56f7e13c17f49567b6731.
- https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/master.20261013.idx SHA256 a17ef25c4d1e8417352d89336cdfc889d1d15bc9b51c5afb4fd958adaf1468a1.

Artifacts: sec-index-error-1792104342.json; sec-index-oct14/analysis.json,delayed-analysis.json,replay.json and exact public originals. The web reader could not open the .idx URL; the existing identified source client succeeded without changing headers or transport policy. No source or customer production payload exported.

## Repair and checks

The index parser accepts an optional expected index date from discovery. An explicit Last Data Received header must match that date. Earlier filing dates are allowed only when that matching header proves the index date; missing/malformed/wrong headers cannot authorize them. Future filing dates remain rejected. Same-day fixtures without a header preserve their existing behavior. Filings retain their original dates, identities, URLs and first-observed clocks. Existing duplicate and complete-original validation remains mandatory downstream.

105 focused LinuxPython3.12.3 checks pass, covering delayed dates, wrong/missing/malformed headers, future dates, repeated discovery, closures, source parsing and publication. Complete-original offline replay on WindowsPython3.14.2 stages all543 accessions (298Form4,24513F), with every parsed field equal to the existing parser output and exact IDs/source metadata/first-observation clocks unchanged on repeat. Zero events and zero requests during replay. This is staging, not proof of complete parsed sources or publication.

Local collector repair at this checkpoint; reviewed release, installed runtime and ordinary source collection remain required. PR691 publisher fairness is separately deployed5a3bc04d with installed synthetic proof; its first ordinary backlog progression remains pending. FMP stays enabled, institutions remain separately gated, and all financial/transcript/consumer/capacity/final-provider/zero-FMP/billing gates remain.

## Deployed and complete-original installed replay verified, October15 22:54UTC

PR692 https://github.com/moore11j/congress-tracker/pull/692 testedf5a9edb397dc515266f806bc6028040cf6705c48, merged252899af70bb7fc029650e367a02e725f2c40aff. Exact merged tree equals tested tree; both dependency audits, secret scan and Vercel preview succeed. Guarded workflow38820028975/job116714089476 succeeds; readiness/privacy checks pass22:54:03UTC. One transient diagnostics502 was followed on the same job using status metadata, without redispatch. Existing submodule cleanup warning is nonfatal; attachment cap100 leaves the normal PR link available.

Installed Python3.12.15 proof matches three LF-normalized module hashes and replays the complete public October14 index in isolated SQLite with production autoflush disabled. All543 rows/fields remain intact; staging298Form4/24513F repeats with identical IDs, metadata and first-discovery clocks. Wrong header, missing date proof and future filing dates are rejected. Network sockets/requests are blocked; zero production record reads/writes/provider requests/customer emails. Receipt artifacts/sec-index-runtime-1792104892.json. The 77KB public-index proof is transmitted on SSH stdin to stay within Windows command-line limits. Local rehearsal also passes.

Actual scheduled collection after this release remains unverified; the next ordinary collection is required before claiming live discovery recovery. Form4 publisher PR691 is separately deployed and installed-verified, with first ordinary backlog observation ongoing. Latest public spot check at22:51:28 (after publisher, before this collector release) returns200/no rendered errors: Feed0.493s,Insights0.185s,AAPL0.256s,NVDA0.300s. This is not sustained or signed-in data completeness proof.

FMP remains enabled. Both changes make concrete migration progress without weakening reconciliation, buying a plan, changing schedules, or activating institutional publication. Full coverage, all consumers, sustained capacity, final Massive/paid validation, actual zero-FMP traffic and billing retirement remain open.


October15,22:58UTC: The separate Form4 publisher normal run now verifies200attempts/96published filings/243events, with source-identity checks and352remaining unattempted filings. See form4-publication-fairness-2026-10-15.md. This does not establish the collector's next ordinary source run, which remains pending.
