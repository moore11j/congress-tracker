# Insights thumbnails and free Treasury data

October 9 Pacific / October 10 UTC, 2026. The owner reports missing Insights news thumbnails. The Finnhub normalizer discarded every source image even though both article-card layouts support images. It now retains validated feed image URLs and preserves signed query strings.

The existing scheduled news warmer additionally prepares publisher Open Graph, Twitter image and image_src metadata for missing images in the first fifty stories of each of its three market feeds. It fetches at most eight pages per run within a twenty-second batch budget, caches positive results for seven days and negative results for twelve hours, and releases its database transaction before HTTP. Every redirect revalidates public DNS addresses and pins the connection to the checked IP while TLS verifies the publisher hostname. Only bounded HTML is read; no JavaScript, cookies, provider credentials, article-body republication or image downloading/proxying is introduced.

Image enrichment preserves article identity, publication and first-observation times, raw source refresh timestamps and saved editorial takes. A thumbnail revision refreshes the headline projection even when the underlying news timestamp is unchanged. Both client layouts can display an image discovered after their initial render and suppress a failed image URL. Articles without a usable image retain the text layout.

The same Insights audit found an unnecessary FMP Treasury call before the existing FRED fallback. The reader now uses its prepared FRED Treasury series directly. A read-only production receipt at 04:27UTC finds all five Treasury series cached, latest observations October7, with no missing/stale/error series under the existing cache policy. All five public Treasury values, dates and basis-point changes already equal those FRED projections; this removes an unused request without changing those displayed facts. The broader price/sector replacements remain separate.

## Checks

Seventy-five focused Python3.14.2 checks pass across news normalization, safe metadata extraction, persistent cache/repeat, actual news warming, headline projection and Insights snapshots. TypeScript passes. Ten frontend source checks pass; one unrelated ticker pricing-link expectation reproduces identically on unchanged released source. No full-suite claim.

A bounded live probe of six currently displayed article URLs retrieves four publisher thumbnails from CNBC. Two Google News redirect-wrapper links expose no usable metadata and remain without images. This is four successful current-page extractions, not a promise of complete publisher coverage. Receipt: artifacts/direct-feeds/free-replacements-2026-10-09/thumbnail-live-probe.json. The probe makes no database writes or emails.

Release, actual scheduled preparation, API/browser image rendering and post-release Treasury request observation remain next. No purchase, customer test email, price switch or FMP shutdown.
