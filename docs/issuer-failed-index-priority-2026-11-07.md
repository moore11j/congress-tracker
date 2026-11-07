# Current issuer index retry priority

At 05:17 UTC on November 7, the ordinary transcript collector had spent its two
05:13 request slots rechecking Moderna Q1/Q2 2025 transcripts. Reddit's current
2026 index still carried the prior parser failure from November 6. Its repaired
parser was already deployed in PR779, but six older transcript checks preceded
the now-eligible current index. This delays source recovery without adding data.

Within the existing eligible queue, retry a failed current index before routine
historical document rechecks. Existing new and 36-hour-old index priority stays
first. Current identity comes from the configured issuer's current index key;
failed archive indexes keep their old order. The daily or longer configured
recheck limit, two-request shared budget, static-source reservation, source URL
review holds and stop-on-cooldown behavior remain unchanged. No new schedule,
extra requests, parser permissions or publication activation.

The complete Reddit index/PDF regression recovers its current transcript while
leaving the historical PDF untouched, retaining one canonical document and the
existing index revision. A read-only queue projection moves Reddit from seventh
to first among eight eligible entries while preserving every other entry's
relative order. This is a projection, not evidence of an actual source fetch.

130 local checks pass, covering complete originals, discovery, reservation and
freshness guards. Exact baseline failure, Linux release, installed validation
and subsequent ordinary collection are separate acceptance steps.
