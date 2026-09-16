# Free options-activity pilot: fixed protocol

Approved September 14, 2026. No paid subscription, production data writes, live confirmation changes, or retrospective changes to the public ledger are authorized by this experiment.

## Initial cohort and timing

First verify the collection and evaluation pipeline on eight prespecified liquid stocks: TSM, AAPL, NVDA, MSFT, AMZN, JPM, XOM, WMT. This convenience pilot includes technology, finance, energy, and retail; it is not representative of the full ledger and is not a model-promotion sample. Use each ticker's earliest frozen public confirmation. Choose the latest SPY session strictly before its original calculation's New York calendar date, conservatively withholding same-day EOD options data even for after-close calculations. Use the underlying closing price from that session for contract selection. Never use the outcome or later stock price to choose a contract.

Retrieve reference metadata as of that cutoff, including both expired and nonexpired records. Collect every page within a +/-10% strike range and expiration 1–90 calendar days after cutoff, then deduplicate by contract ticker. For each of three expiry buckets (1–7, 8–29, 30–90 days), select one call/put pair at the same strike and expiry: nearest expiry to 7, 21, or 60 days, then nearest strike to the cutoff stock price, with deterministic ties. Require standard 100-share deliverables and no additional underlyings. This is a six-contract sample, not total chain activity.

Retrieve unadjusted daily bars from cutoff minus 90 calendar days through cutoff. Retain only timestamps at/before cutoff. Free EOD data is available no earlier than the following session. Cached payloads contain public data and sanitized metadata, never credentials or credential-bearing pagination URLs. Requests are serialized at >=13-second intervals; stop on authorization or rate-limit errors, cap requests per invocation, and resume from cache. No request is made by a public page view.

## Fixed features and comparisons

For each matched pair, use the last five benchmark sessions versus the preceding 20 sessions. A contract must have an observed bar on/before the start of that 25-session window before absent bars inside it can count as zero qualifying volume. Otherwise baseline coverage is unknown. Failed/truncated responses are unknown, not inactivity. Require volume and VWAP for positive-volume observations used in premium turnover.

Features are call/put volumes, gross premium turnover (VWAP times contracts times 100), call shares of volume/premium, changes in those shares versus baseline, and recent daily activity relative to baseline daily activity. No buyer/seller intent, opening/closing status, sweeps, or net inflows are inferred. Shorter expiry buckets may lack sufficient history; do not force a baseline for newly listed weeklies.

The first evaluation uses two **fixed, untrained** candidate bullish gates on the 30–90-day matched pair:

1. Options gate: recent gross premium activity >=2x baseline daily rate, recent call premium share >=0.60, and call premium share rising at least 0.10 versus baseline.
2. Combined gate: options gate plus stock and SPY each above their own 20-session moving average at cutoff.

Keep original bearish calls. Missing options data retains the original decision. A nonqualifying bullish call is an abstention in the hypothetical comparison, not a retroactive relabeling. Report identical-event original versus candidate results, number retained, unknown coverage, raw positive frequency, signed returns, and SPY-relative returns for 30D first and 7D separately. Distinguish the fully covered subset from the fallback-inclusive result. Never present an accuracy rise obtained by dropping most calls as demonstrated general improvement.

Before joining any outcomes, add a price-only control requiring stock and SPY above their 20-session moving averages. Compare this with the combined gate on the same options-covered cohort, so an improvement caused by the price/market filter cannot be attributed to options. This control does not change either options threshold.

Eight tickers and a handful of opening dates cannot support fitting weights or a held-out accuracy claim. Do not fit a classifier to this pilot. Broader chronological training would require more dated cohorts with labels matured before the next evaluation period, a 30D maturity gap, and an independent later period. This pilot's findings determine data feasibility, not a production score change.

## Invariants

Verify the frozen ledger SHA-256 before and after: `de0885ffc1c1b026541b0cb364efd7c0b5dcf9feb62108d7d08473a49303613b`. Research labels use the existing public raw-return OR SPY-relative correctness definition and the already stored returns. Record input/configuration hashes, endpoint coverage, all failures, and sample membership. Do not fit rules after seeing pilot outcomes.
