# Research credit pricing revision

November 24, 2026. Owner accepted the layout but rejected the original one-cent top-up economics because they undermined the subscriptions. Owner directed roughly two cents per purchased credit, retained Pro at $39.95/4,000 credits, and requested higher task charges. This revision is implemented in the parallel preview only. The task prices below are the proposed starting prices, not measured willingness to pay or live billing.

## Current preview prices

| Purchase | Price | Credits | Effective cents per included credit |
|---|---:|---:|---:|
| One-time | $12.50 example | 625 | 2.00 |
| Premium monthly | $24.95 | 1,500/month | 1.663 |
| Pro monthly | $39.95 | 4,000/month | 0.999 |
| Premium annual | $249.50/year | 1,500/month | 1.386 |
| Pro annual | $399.95/year | 4,000/month | 0.833 |

At one-time prices, Premium's monthly credits would cost $30 and Pro's would cost $80. Monthly subscription savings on fully used credits are 16.83% and 50.06%, respectively, with the existing paid features included. Annual equivalents save 30.69% and 58.34%, assuming every monthly allocation is used. This comparison does not imply that unused monthly credits roll over: monthly grants expire; purchased credits carry forward. Free still proposes 100 monthly credits. Credits never unlock inaccessible Premium/Pro data.

| Task | Previous credits | Revised credits | One-time price equivalent |
|---|---:|---:|---:|
| Company lookup, charts and navigation | 0 | 0 | $0 |
| Short sourced answer | 10 | 25 | $0.50 |
| Research analysis or comparison | 50 | 100 | $2.00 |
| Focused research report | 200 | 400 | $8.00 |

If used on just one category, Premium covers 60 short answers, 15 analyses, or three reports with 300 credits remaining. Pro covers 160 short answers, 40 analyses, or ten reports. These are alternative uses, not additive allowances. Free covers four short answers or one analysis. Task prices are shared across plans; plan value comes from included credits and feature access. Scope must be quoted before execution. A follow-up can be a short answer even after a report.

One-time packages are $5/250 credits, $10/500, $20/1,000 and $50/2,500. Custom amounts remain available from $5 to the current $1,000 sandbox limit. Whole credits require two-cent increments; amounts such as $10.01 are rejected explicitly rather than rounded or charged for a fractional credit. $12.50 buys exactly 625 credits.

## Implementation and accounting

The backend now separates paid cents from granted credits. Checkout quantity is `amount_cents / 2`, Stripe unit amount is two cents, the paid subtotal must equal quantity times two, and fulfillment grants quantity rather than cents. Duplicate fulfillment and concurrent spend protection remain. The catalog version is `walnut_research_v2`; old one-cent prices and old-version line items cannot fulfill as v2 purchases. No real Stripe objects or payments had been created in the earlier preview, so no subscriber/order migration was performed. Do not silently repoint a configured older sandbox with outstanding orders; reconcile those before changing versions.

Both backend task pricing and frontend cost displays use 25/100/400. Subscription prices/allowances and paid-data gates are unchanged. The preview now shows the $30/$80 one-time equivalents and per-task one-time costs, with a full-use comparison caveat. No model calls, account configuration, production pricing changes or live charges are included.

## Economics

This recalculation retains the earlier November 24 scenario assumptions, not new API telemetry or a fresh Stripe account fee audit: maximum variable budget $0.002 per consumed credit; domestic one-time card fees modeled at 2.9%+$0.30; subscriptions additionally modeled with a 0.7% Billing fee. The earlier workload budgets remain $0.020/$0.09125/$0.39750 per answer/analysis/report. Higher customer prices do not justify changing quality requirements or unbounded model work.

| One-time purchase | Credits | Payment fee | Full-use variable budget | Contribution before other costs |
|---|---:|---:|---:|---:|
| $5 | 250 | $0.445 | $0.50 | $4.055 (81.1%) |
| $10 | 500 | $0.590 | $1.00 | $8.410 (84.1%) |
| $20 | 1,000 | $0.880 | $2.00 | $17.120 (85.6%) |
| $50 | 2,500 | $1.750 | $5.00 | $43.250 (86.5%) |

The subscription ceilings stay $3/month for Premium and $8/month for Pro; modeled monthly contributions therefore remain $20.75/$30.21 before shared infrastructure, data licensing, support, acquisition, refunds/disputes and taxes. These are contribution scenarios, not net-profit promises. Cost/quality validation, real test payments, subscription lifecycle and production release gates remain open. Offline arithmetic is reproducible in `artifacts/walnut-credit-economics-v2-nov24.py` and its JSON output in the owner workspace; the original calculator remains historical.

Local validation: 40 credit/sandbox cases and five frontend credit cases pass; these include exact $5/$12.50/$1,000 quantity-to-payment-to-grant checks, duplicate delivery, wrong unit price, old catalog, wrong quantity and odd-cent rejection. TypeScript and exact remote checks are recorded in the latest task checkpoint when complete.
