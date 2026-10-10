# Institutional security continuity

October 10 UTC, 2026. Local continuation after deployed reference mapping PR526/69213790. Institutional publication remains disabled.

XOM and OKE changed CUSIPs during one-for-one reorganizations. Comparing those identifiers literally produces a false exit and new position. The adapter now matches only the reviewed old/new pair across the effective quarter boundary, preserving the original SEC positions, shares, values, identifiers and dates. Mixed identities, unknown transitions, future evidence and other periods remain held.

The original source bytes and checksum-bound manifest are packaged with the backend. Git attributes preserve those bytes. The evidence was reviewed at 2026-10-10T06:21:26.999641+00:00; this is not backdated to the legal action date or filing date. Published filing and qualifying-event provenance retain the separate review time, source URLs and hashes.

Reviewed sources:

- [ONEOK SEC Form 25](https://www.sec.gov/Archives/edgar/data/876661/000087666126000770/0000876661-26-000770.txt): 682680103 to 30609A109, September 10 business reorganization, one share for each share, ticker OKE. The later trading date is not used to claim an earlier observation.
- [ExxonMobil Form 25](https://investor.exxonmobil.com/sec-filings/all-sec-filings/content/0000876661-26-000593/ruleprovisionnotice.htm) and [8-K](https://investor.exxonmobil.com/sec-filings/all-sec-filings/content/0001193125-26-291986/d70995d8k.htm): 30231G102 to 30233Q108, July 1 reorganization, one-for-one ownership, ticker XOM. Retained HTML was decoded from the separately preserved gzip transport; manifest hashes identify decoded HTML.

The shared comparison is applied to publication, the symbol-batch path and the holder-page fallback. A generic recalculation cannot overwrite a published direct filing's event availability or provenance; reset requests require explicit reconciliation. This does not rewrite existing published pairs or silently enrich their supporting history.

The MBGL spin-off remains held separately. A distribution is not an interchangeable one-for-one security identity, and quarter-end ownership alone does not establish entitlement through the due-bill period.

## Validation

Python 3.14.2, isolated SQLite, HTTP forbidden and no customer delivery:

- 88 focused reference, publication, worker, batch and continuity checks pass on the final implementation.
- 334 institutional view, source snapshot, signal, watchlist, digest and replicated-portfolio checks pass. One previously reproduced unrelated locked-profile name assertion is explicitly deselected; this is not a complete suite pass.
- Original six-source/three-pair replay uses the unchanged captured baseline hash `87bf23c06546589ff97509ba50dff8e63894c3dfd6d5246831ee3732e76c312e`, 402 independent peer documents and eleven saved reference responses. Two current pairs publish in isolation, one MBGL pair remains held, 557 canonical positions retain source facts, and the whole database repeats identically. No qualifying feed events arise in this sample, so it does not establish a real-event digest receipt. A qualifying synthetic reduction verifies event provenance while the broader consumer tests exercise delivery rules.

The additional original-share replay also passes: OKE7,202 to7,241 shares is an increase of39; XOM21,766 to21,527 is a decrease of239; another XOM position remains14,185 shares across its identifier change. All three comparisons match the original SEC source rows, and the database repeats identically. All three committed evidence-file hashes match the manifest. The older full 286-document replay began before later reference/consumer/distribution safeguards and is not an exact-source test of this package. Full current-cohort replay, ordinary scheduled publication, PostgreSQL validation of these changes and broader coverage remain activation gates. No institutional production publication, paid subscription, customer test email or FMP cancellation.
