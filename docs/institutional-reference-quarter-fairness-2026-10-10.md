# Institutional reference quarter fairness

The identifier warmer previously sorted quarter first, so every current-quarter request preceded every prior-quarter request. Direct 13F comparisons require both quarters. This can hold otherwise useful comparisons while lower-frequency current-quarter identities are fetched.

The existing two-request batch now interleaves the current and prior quarters, preserving filing-frequency priority inside each quarter. When one quarter has no eligible work, the other may use both slots. Provider quota/cooldown, daily retry limits, source provenance, publication flags and the serialized host remain unchanged. No additional recurring job or request budget is introduced.

On October10 at20:35:11UTC, a bounded read-only comparison of the actual saved universe finds4,962 pending scopes. The old plan selects two current-quarter identities each needed by one filing. The new plan keeps the first and gives the second slot to prior-quarter CUSIP03073E105 needed by35 filings. Other plan fields are identical; zero DB writes/provider requests/public writes. These are pending-scope planning counts, not completed filing counts.

Python3.14.2:27 reference/warming checks pass, including a100-identity current backlog that cannot starve the prior quarter, duplicate-free subsequent batches, daily deferral and fallback to two current-quarter slots when prior work finishes. Deployment and first ordinary scheduled observation are pending.

The separate full286-source institutional replay remains active in session92282 after its first pass. New saved reference capture contains87 documents:72 verified,11 unsupported/incomplete-security holds and4 missing matches. A source-only diagnostic reduces remaining distinct unmapped CUSIPs to188 across63 waiting comparisons and identifies one comparison with no remaining mapping failures. This diagnostic is not publication or consumer validation. Institutions remain FMP with direct publication disabled; no subscription, customer test email or FMP shutdown.
