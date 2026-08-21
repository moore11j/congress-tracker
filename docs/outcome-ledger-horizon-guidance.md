# Outcome Ledger Horizon Guidance

Walnut's 30-day Confirmation Score should not be justified as the average investor's holding period. The stronger rationale is that 30 days is a useful common denominator across Walnut's fast and slow evidence sources.

The current 30D score sits near the intersection of:

- Fast evidence: price/volume, RSI, MACD, momentum, options flow, news, near-term catalysts.
- Medium evidence: analyst revisions, insider trades, Congress trades, institutional activity.
- Slower evidence: government contracts, fundamentals, industry trends, balance-sheet quality, multi-quarter institutional behavior.

A 7D score would naturally overweight technicals, options, and catalysts. A 365D score should care far more about fundamentals, valuation, margins, balance sheet, industry structure, capital allocation, and durable institutional trends. A short-term MACD crossover should barely affect a one-year thesis.

## Product Position

Keep the ticker headline simple:

- Confirmation Score
- Direction
- 30-day outlook

Do not clutter ticker pages with separate 7D, 30D, 90D, 180D, and 365D numeric scores yet. Multiple horizon scores may eventually be useful, but they should be built from Outcome Ledger evidence, not guessed.

## Outcome Ledger Role

The Outcome Ledger should measure every directional event at:

- 7D
- 30D
- 90D
- 180D
- 365D

This lets Walnut discover where the current score actually works best. The working hypothesis is that cross-source confirmation should have predictive value around 30D to 90D, but the ledger should prove or disprove that.

## Public Grading Rule

The main public accuracy statistic should grade directional calls against SPY, not just against zero return:

- Bullish is correct when the ticker outperforms SPY over the horizon.
- Bearish is correct when the ticker underperforms SPY over the horizon.
- Raw up/down correctness remains a diagnostic field, but it is not the headline public score.
- Mixed and neutral are watch states. They do not open, close, or grade directional outcome events.
- A direct bullish-to-bearish or bearish-to-bullish flip closes the old directional event.
- Missing entry, target, or benchmark prices are excluded from scored accuracy.

## Future Horizon Models

Once enough events mature, Walnut can build empirically derived horizon models:

- Short-Term, 7D: technicals, momentum, options, catalysts.
- Confirmation, 30D: broad cross-source evidence.
- Position, 90D: fundamentals, analyst revisions, institutional accumulation, insiders, contracts, industry trends.
- Long-Term Conviction, 1Y: growth, ROIC/ROE, margins, balance sheet, valuation, competitive position, institutional trends, capital allocation, multi-quarter insider behavior.

Each model should have source weights derived from observed outcomes. The Outcome Ledger should answer:

- Which horizon performs best?
- Which source predicts which horizon?
- Do insiders predict 90D better than 7D?
- Does options flow add anything beyond 30D?
- Do institutional signals become more useful at 90D to 180D?
- Does technical confirmation decay after 30D?
- Does combining technical and fundamental evidence extend the useful horizon?
- Are bullish and bearish signals different?

## Current V2 Constraint

Confirmation-v2 should improve the existing 30D confirmation methodology while preserving the ticker-page surface. It should not introduce five public score numbers or an arbitrary timeframe dropdown before the Outcome Ledger has enough samples to justify horizon-specific models.
