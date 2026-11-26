# Complete reviewed SEC identifier corpus

The manifest retains all twelve deployed source URLs and checksums in their existing order and appends the complete N-PORT original 0002066578-26-003341. Each `.source.gz` is deterministic gzip of the complete original, with no source trimming or reconstructed records. The thirteen decompressed originals total 52,115,168 bytes; the largest is 20,994,977 bytes.

The original twelve files were already retained and verified by the institutional replay. The additional SEC submission was retrieved November 26, 2026, is filed September 28, and reports the July 31 period. Its complete original is also present in the smaller enrichment fixture. The new source supplies 823 accepted identifiers only from its disclosure date. Every source URL and full decompressed SHA256 is in `manifest.json` and the candidate operator manifest.

`identifier_outputs` records complete canonical parser-output SHA256 values from the independently verified originals. The offline resource fixture checks all source and output hashes, exact configuration membership, both file and staged readers, immutable staging repeat, unchanged old source records and no external transport. Two retained originals produce empty eligible identifier lists under the existing parser; they remain in the corpus rather than being silently discarded.

These files qualify the candidate corpus. They do not modify the active frozen replay or establish full-cohort publication recovery. Production staging of the additional original remains part of a reviewed release operation.
