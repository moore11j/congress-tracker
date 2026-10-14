# Reviewed parent income, October 14, 2026

## Problem and repair

The 90-scope missing-input diagnosis identified FLEX, FOX and JEF with positive beginning/ending equity but no usable current parent income. Fresh SEC companyfacts, company identities and six annual/interim originals were captured and reviewed. Seven exact reporting periods now support a guarded source reconciliation. Existing reported parent values, including conflicts, retain priority. In the actual replay, six periods are added and FLEX's existing prior-period parent value is retained.

FLEX's IncomeLossAttributableToParent is explicitly before tax, not a NetIncomeLoss replacement. Its interim statement independently reconciles the reported after-tax net amount to parent pretax income less tax. FOX's original statement reports parent shareholder income after total NCI, including the separately tagged redeemable component; subtracting that component again would be wrong. JEF's original common-shareholder income excludes preferred dividends: adding the explicitly reported preferred dividends reconciles consolidated income less minority allocations to the parent amount. These reviews do not generalize across other companies, periods or future filings.

Each accepted period requires exact CIK, period, filing date, accession, currency, reported concept and amount. Optional inline URL, hash and context must match the original. Changed/newly reported minority allocations invalidate the review even if not part of the arithmetic. Missing allocations are never assumed zero. Original statements, input observations, arithmetic and provenance remain attached. Public/core provider and leverage calculations are unchanged. Only the three preparation versions change; existing refresh cadence is retained.

## Original-source replay and consumers

Against current deployed/main fcb36a2d, complete source replay adds net margin and ROE for each company while preserving every existing public financial value, all non-income source observations, equity and the complete leverage panel. Current parent TTM income is FLEX $973 million, FOX $1.685 billion and JEF $936.650 million. Prepared ROE is respectively18.3775616205%,14.2857142857%,8.8559215827%. These are the adapter's explicit parent-income/average-equity calculations, not a claim of FMP methodology equivalence. JEF's existing revenue basis and financial-sector scoring suitability remain separate semantic gates.

All20 retained original-source controls match the prior complete output exactly, including holds. Three actual isolated preparation/cache runs preserve complete projector evidence and repeat without transport or canonical writes. Actual captured ROE changes run through the SQLite watchlist evaluator with zero transition alerts, zero repeat alerts and zero rollback alerts. Synthetic later threshold crossings each create one monitoring record and no duplicates; they are not issuer observations or real email-delivery proof. Full rankings, daily/weekly delivery and universe-wide rebaseline remain separate acceptance gates.

Final affected Linux Python3.12.3 suite:269pass, including parent/consolidated/source review, projector, preparation, inline facts, financial consumer selection and alert-basis tests. Actual original replay also passes on Windows3.14.2; cache/control/watchlist runs use Linux3.12.3. Initial baseline extraction hit Git ownership protection; command-scoped safe.directory resolves that diagnostic without global configuration changes. No full-suite or clean dependency-audit claim.

Local receipts under artifacts/parent-gaps-oct14: manifest.json, inventory.json, replay.json, controls-cache-replay.json and alert-replay.json. The latter runs are offline with zero production writes or email. Original captures are public SEC reads. The tracked review JSON preserves source URLs, hashes, contexts, values, identities and accounting basis.

## Release state

At PR preparation, implementation and local validation were complete; deployment was pending. Current release evidence follows below. Core financials remain FMP; institutional publication remains disabled. Future statement reviews, broad coverage/definitions, all consumer/scheduled operation, sustained capacity, institutions/transcripts, final Massive and global zero-FMP/billing gates remain. No selector, schedule, host cost, purchase, customer email, video or landing change.

## Captured source identities

- FLEX-company.json: [https://data.sec.gov/submissions/CIK0000866374.json](https://data.sec.gov/submissions/CIK0000866374.json), SHA256 `9e0e7f40c7bd09a77ed0aa9d30f6a458e91ab65547dced3c7e258f11a39245b9`.
- FLEX-facts.json: [https://data.sec.gov/api/xbrl/companyfacts/CIK0000866374.json](https://data.sec.gov/api/xbrl/companyfacts/CIK0000866374.json), SHA256 `5d922c26a276de702c49be1ee681f27a4d46cc67e78204ca92c53508b0242f83`.
- FLEX-annual.html: [https://www.sec.gov/Archives/edgar/data/866374/000086637426000012/flex-20260331.htm](https://www.sec.gov/Archives/edgar/data/866374/000086637426000012/flex-20260331.htm), SHA256 `1c2764f144e916eeb390bbd65d81d7e230770f88f2d4a2e976a7916c61fca4ab`.
- FLEX-interim.html: [https://www.sec.gov/Archives/edgar/data/866374/000086637426000030/flex-20260626.htm](https://www.sec.gov/Archives/edgar/data/866374/000086637426000030/flex-20260626.htm), SHA256 `783b8052cfdcad7257ea501cbc65ca68dc9701c8180d78b17edc7ed816972166`.
- FOX-company.json: [https://data.sec.gov/submissions/CIK0001754301.json](https://data.sec.gov/submissions/CIK0001754301.json), SHA256 `601b49b334f1a9746df8441d5882b2557d96cee934e2c682d566902fdfd6f553`.
- FOX-facts.json: [https://data.sec.gov/api/xbrl/companyfacts/CIK0001754301.json](https://data.sec.gov/api/xbrl/companyfacts/CIK0001754301.json), SHA256 `4a80ac8514d37da0a0ca8b01a090cf90c3eff2c3095b9b3602ac357627bf6589`.
- FOX-annual.html: [https://www.sec.gov/Archives/edgar/data/1754301/000162828026053960/fox-20260630.htm](https://www.sec.gov/Archives/edgar/data/1754301/000162828026053960/fox-20260630.htm), SHA256 `7ecb9dca18462f9c2346a348638f160d8196ab4b1c2d96a4cb713cf9939b92e7`.
- FOX-interim.html: [https://www.sec.gov/Archives/edgar/data/1754301/000162828026033172/fox-20260331.htm](https://www.sec.gov/Archives/edgar/data/1754301/000162828026033172/fox-20260331.htm), SHA256 `f159c2e9e41def5e43e55e3f711d2edc5a29d5663ad5188eb66c86570319ea70`.
- JEF-company.json: [https://data.sec.gov/submissions/CIK0000096223.json](https://data.sec.gov/submissions/CIK0000096223.json), SHA256 `eee31a5259f6e0c8d5a93d53bd5f0dcdafbe9d036bd8932d4f266d3caabe5844`.
- JEF-facts.json: [https://data.sec.gov/api/xbrl/companyfacts/CIK0000096223.json](https://data.sec.gov/api/xbrl/companyfacts/CIK0000096223.json), SHA256 `b70dcc619f178bbd465da5037bc9de9827edc50a4de1629f21459e90697762a0`.
- JEF-annual.html: [https://www.sec.gov/Archives/edgar/data/96223/000009622326000009/jef-20251130.htm](https://www.sec.gov/Archives/edgar/data/96223/000009622326000009/jef-20251130.htm), SHA256 `0d489f6f36087d7d88dd987d9bc7a5147cf090146f7b0c34c4f5ccf5e068cc41`.
- JEF-interim.html: [https://www.sec.gov/Archives/edgar/data/96223/000009622326000033/jef-20260831.htm](https://www.sec.gov/Archives/edgar/data/96223/000009622326000033/jef-20260831.htm), SHA256 `6ead5c847ecc5035350a072c403abb5bf6715ad0d58a96d1d4d2a77e726bb1bd`.

## Deployed runtime verification,21:43 UTC

PR646 source c0d5935b0697a7d7c5de15f5b06df2e012bc2e18 merged/deployed a57e5ab9f90a55170075f6bd431ad39198fc39a7. Workflow38641989108/job116119532029 is terminalSUCCESS. Fourteen normalized runtime hashes match and all seven reviewed-period fixtures, immutable repeats and unreviewed-filing refusals pass on productionPython3.12.15. Receipt artifacts/parent-gaps-oct14/runtime-1792014140.json. CoreFMP and institutional publicationfalse remain unchanged; no production writes or provider calls in that verifier.

Seven public reads all return200: Feed0.534s, Insights0.538s with research present, news0.443s/12items, research0.536s/44items, AAPL2.622s and NVDA2.357s. These are postrelease spot checks. At21:39, API credits14,353/current throttle0 have recovered from21:19, but the two-hour window still includes exhaustion/deployments; sustained capacity remains open. CPU receipt cpu-quota-1792013950.json and public receipt insights-incident-1792014140.json under artifacts/direct-feeds/free-replacements-2026-10-09.

Read-only receipt artifacts/preload-receipts-1792014154.json verifies API8d1099c53ed378 shared2/2048MB, database81721da9770131 performance1/2048MB, and the exact release image. News23/23 with42fresh company caches, five metadata/earnings scopes succeed. Institutional950-scope preparation retains459pending/491deferred, truncated universe and publicationfalse; financial research16partial/4unavailable remains explicit.

At21:42, the three parent-income preparation rows are still old versions, awaiting the ordinary job. No forced refresh or selector change. JNJ's ordinary21:06 preparation is now verified equal to its earlier original-statement reconciliation and EBITDA33.959billion; HCA still has its older19:31 panel. Receipt artifacts/parent-gaps-oct14/ordinary-1792014153.json. Future statement and full-consumer coverage remain open.

CI secret scan succeeds. The existing frontend4high/1critical production dependency findings and Pillow10.4.0/33listed advisories still fail; manifests are unchanged. Required CI diagnostics were inspected. One deploy-log request returned502; the same run independently reachesSUCCESS through run-level diagnostics, with no repeated deployment. Attachment fails at the existing100-item limit; PR https://github.com/moore11j/congress-tracker/pull/646 remains available.

## Specific next market-ratio gate

JEF's reviewed statements also prove common-shareholder trailing income852.597million and preferred dividends84.053million, versus parent income936.650million. Parent income with total parent equity is the present ROE basis. The currently unselected SEC/Massive price-ratio path still divides common market capitalization by parent income. Before selecting that market path, implement and validate the common-income numerator (or a truthful missing-value hold) for preferred-share issuers. Do not call the parent-based multiple conventional common-equity P/E. This does not change today's selected FMP public ratios and is a concrete follow-up within the original final-Massive/financial-consumer acceptance gate.

## Ordinary rotation checked,21:48 UTC

The21:46 run completed ten of13 due SEC scopes, seven partial and three known symbols absent from the SEC directory. It selected older DBOEY through DEER.TO scopes; FLEX/FOX/JEF were not attempted. Exit1 reports partial coverage, not an execution crash. The three reviewed versions remain pending in normal age-ordered bounded rotation; no forced refresh or repeated job was issued. Read-only receipts ordinary-1792014394.json and warmer-1792014432.json under artifacts/parent-gaps-oct14 preserve the exact results. Earlier next-job expectations did not establish propagation. All handles are terminal. Continue independent common-equity valuation work while the ordinary rotation proceeds.
