# Index Membership Sources

Walnut's `index_memberships` table stores broad-universe membership data for background jobs and Market Pressure capabilities.

## Wikipedia Component Tables

The active production-safe source is the MediaWiki Action API for Wikipedia's current component tables:

- `List_of_S%26P_500_companies`
- `Nasdaq-100`

This data must be described as Wikipedia-derived membership data, not official index-administrator or licensed index-provider data. Store source metadata including page, resolved title, revision ID, retrieval date, source kind, and parser version.

Owner review required: Wikipedia content reuse is generally governed by CC BY-SA terms, including attribution and possible share-alike obligations for covered reused content. Walnut should confirm that the intended commercial use, attribution, and downstream presentation comply before relying on this permanently.
