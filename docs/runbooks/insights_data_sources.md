# Insights Data Sources

This note captures the launch data-source decision and the upgrade path after Walnut moves beyond the FMP Builder plan.

## Launch Mode

Default Insights mode is `builder_safe`.

- US Macro reads from Walnut's local FRED macro cache.
- Treasury reads from FRED cache, or a future Treasury.gov cache if added.
- US Indexes read EOD ETF proxies from the existing FMP Builder-safe price cache.
- US Sectors read EOD sector ETF proxies from the existing FMP Builder-safe price cache.
- World Indexes use global ETF proxies, or can stay disabled if proxy coverage is not acceptable.
- Currencies stay disabled unless a licensed forex provider is added.
- Crypto stays disabled unless a licensed crypto provider is added.
- Commodities use ETF proxies, or can stay disabled if proxy coverage is not acceptable.

Do not use FMP macro, index, forex, crypto, or commodity add-on endpoints while Walnut is on the FMP Builder plan.

## FRED Guardrail

US Macro must remain FRED-backed. Keep macro observations stored locally, refreshed on a schedule, and read from the local cache during page render. Do not fetch FRED synchronously from Insights page render paths.

Admin diagnostics should keep showing the last FRED refresh timestamp, stale series, and missing series so stale or unavailable macro data is visible before it reaches users.

## After FMP Upgrade

When Walnut upgrades from the FMP Builder plan to a plan that includes the relevant licensed endpoints, switch the non-macro Insights blocks back to FMP-backed cached data where appropriate:

- US Indexes can move from ETF proxies to licensed FMP index data.
- US Sectors can move from ETF proxies to licensed FMP sector or market-breadth data if available.
- World Indexes can move from ETF proxies to licensed FMP global index data.
- Currencies can be enabled only after a licensed forex provider is available.
- Crypto can be enabled only after a licensed crypto provider is available.
- Commodities can move from ETF proxies to licensed commodity data if available.

Keep US Macro on FRED after the FMP upgrade. Treasury should remain FRED or Treasury.gov-backed unless a deliberate provider decision changes that.

Any FMP-backed upgrade path should still write to Walnut's local cache and serve Insights from cached snapshots, not live provider calls during render.
