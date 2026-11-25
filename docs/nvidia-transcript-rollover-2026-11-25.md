# NVIDIA fiscal transcript rollover

November 25, 2026. Isolated candidate based on deployed851ba270. Not deployed or selected as the public transcript provider.

The configured NVIDIA transcript was fixed at Q2 fiscal2027. Its official [quarterly index](https://investor.nvidia.com/financial-info/quarterly-results/default.aspx) now links a Q3 fiscal2027 transcript published November17,2026. The issuer's public Q4 widget identifies the financial-report service and query contract used to discover these links.

Two details require explicit handling. NVIDIA's index uses calendar-shaped fiscal labels, such as September30,2027 for Q3FY2027, even though that call occurred in November2026. Those labels cannot be interpreted as publication dates or actual period ends. Its newest transcript also changed from FactSet CallStreet to Bloomberg, with a different first-page layout. Replacing a fixed URL alone would leave the next rollover unresolved.

The candidate adds a scoped NVIDIA discovery adapter. It reads two fiscal indexes within the existing shared two-request discovery budget, validates the issuer tenant, fiscal labels, document category and exact period-bound filename formats, and independently validates each complete PDF. FactSet requires an anchored company/ticker/quarter/year/publisher heading and call date matching its source filename. Bloomberg requires the unique fiscal title and company/ticker/date/publisher footer on the bounded first page. Both require an actual Q&A section and a publication date within the reviewed issuer fiscal window and no later than observation. Index labels remain labeled metadata and never become publication dates or invented period ends.

## Source proof and consumer acceptance

Two complete fiscal indexes and seven complete PDFs were retained. Seven independent parser replays pass, including metadata/text/parsed-output repeat equality; Q3FY2027 resolves to November17,2026 and46,206normalized characters. The existing Q2 static source metadata and full parsed output are bound to the production baseline hash5bee6d0038fc88649bbcbeaaad36f188dee75a66022a5baefc93ab43dcc8dea4. Discovery excludes that existing static key, preserving its current source URL and receipt.

The CI manifest contains source URLs, lengths, source hashes and complete output hashes. Original PDFs and transcript text are not committed. A bounded fetch step obtains nine exact public inputs, with no redirects or retries and2MBper input. Any changed source hash fails for independent review.

Tests cover incorrect source paths, labels, fiscal periods, company/ticker identity, publishers, Q&A, future call dates, source-filename date conflict and calendar rollover. The complete source collector/research/history/repeat suite and full existing issuer regression suite must pass before release review. Normal discovery requires eight requests across four existing two-request batches: two indexes plus six PDFs, excluding the static Q2 key.

## Release and migration boundaries

This adds NVIDIA preparation to the existing issuer collector. It does not change `TRANSCRIPT_PROVIDER`, core financial/scoring methods, subscriptions, feature gates, customer deliveries or institutional publication. Public transcript activation remains separate. The proposed release must coordinate collector11632 between requests, preserve all reference hashes and original bounds, and verify installed source hashes and unchanged effective provider flags. The current Moderna ordinary-recovery observer16180 is bound to851ba270: do not let a later release silently invalidate that observation; finish its check or explicitly qualify a replacement observation against the new exact release.

After deployment, ordinary new-index collection and Q3 research-reader recovery must be demonstrated before counting NVIDIA as current coverage. The new issuer indexes, two publisher layouts and publication windows are a bounded supported contract, not a guarantee for arbitrary future layouts. Unilever source access, broader transcript coverage, financial consumer acceptance, institutional first/repeat/consumer acceptance and final zero-FMP verification remain open.

Local evidence: nvidia-rollover-nov25/manifest.json; nvidia-widget-nov25/manifest.json; nvidia-report-indexes-nov25/manifest.json; nvidia-historical-originals-nov25/manifest.json; nvidia-seven-originals-nov25.json; nvidia-static-baseline-nov25.json.
