# Financial warming work-slot reuse

## Measured delay

After currency guard PR714, the ordinary04:04UTC run planned20 scopes with13 requested cache misses, but ten results only reused older valid caches. It refreshed ten demanded sources and left three (BOSC/NAMI/TSEM) waiting. Receipts: currency-schedule-capacity-1792209926.json, currency-guard-ordinary-1792209901.json and currency-guard-public-1792209892.json. Four of the seven PR713 source panels (AER/ASC/SLGL/VLN) now exactly match originals under the new currency policy; three still await normal refresh. NAMI's old ambiguous rows are withheld, but its final unavailable cache has not yet been observed.

## Shared scheduling repair

Preserve the existing ten-slot demand reservation and give due watched/default work its existing priority. When those watched caches are already valid, place remaining missing requests ahead of cached no-op work. The batch stays capped at20; trigger times, shared collection locks, job serialization, source budgets, opt-in/delivery behavior and host sizing are unchanged. This can use more of the already authorized20-source budget; it is not a claim of unchanged work in every individual run or of sustained capacity.

Validate the union of the existing bounded watched/default and requested scopes using one streamed query with100-row batches. Retain only freshness decisions rather than full financial payloads. The public cache validator remains authoritative, including currency policy, identity, freshness and unavailable-cache handling. No new ticker allowlist or private scope export is added.

Regression cases reproduce13pending requests plus21fresh watched caches: all13requests precede cached no-op work. With21expired watched caches, the existing ten-demand/ten-watched allocation remains. Existing repeat, held-source rotation, lease, source403, public cache, currency and schedule tests remain in the affected suite. All32 affected checks pass on LinuxPython3.12 after the final bounded-streaming change.

The repair is local on codex/sec-financial-warming-capacity based on deployedc857d990. No new deployment or forced refresh yet. CoreFMP and broad coverage/all-consumer/capacity/finalMassive/global-shutdown gates remain open.
