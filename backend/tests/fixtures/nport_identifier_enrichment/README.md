# Complete SEC identifier original

`0002066578-26-003341.source.gz` contains the complete, unmodified SEC submission retrieved November26,2026. Compression is deterministic gzip; the decompressed8,148,718bytes have SHA256`fd403420fb788d29d8effdece5b309f2ea94cf017c9dd0ef148ccff2c1c6816b`.

Source: https://www.sec.gov/Archives/edgar/data/92500/0002066578-26-003341.txt

The existing production parser verifies the header, accession, CIK, URL and XML identity and extracts823eligible equity identifiers, filed September28 for the July31 reporting period. Tests combine this real source with explicitly synthetic holder filings. They prove the enrichment behavior and date boundary, not full production recovery.

`gtm-reference-september.json` is a reformatted copy of an already retained Massive canonical parsed response for CUSIP98980F104 atSeptember30. It is not original HTTP wire bytes. Its original response checksum, URL, query and observation timestamp remain unchanged and are validated by the existing reference reader. It is used with the complete N-PORT source to test preservation of both proofs in the presence of conflicting legacy ZI/ZOM candidates.

These fixtures do not add a production identifier source or relax the FIGI acceptance policy.
