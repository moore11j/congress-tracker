# AI Outreach Provider Credentials

AI Outreach provider credentials are server environment variables only. Do not store OpenAI or Reddit provider values in the admin settings table.

Configure these on Fly as secrets or environment variables:

- `OPENAI_API_KEY`
- `AI_MARKETING_MODEL`
- `REDDIT_CLIENT_ID`
- `REDDIT_CLIENT_SECRET`
- `REDDIT_USER_AGENT`

The admin panel reports configured/missing/default status and can test OpenAI or Reddit connectivity, but it does not accept or return raw provider values.

Deprecated DB rows for these keys are ignored. To remove old rows after confirming Fly secrets are set, run a maintenance SQL command against production Postgres:

```sql
DELETE FROM ai_marketing_settings
WHERE key IN (
  'OPENAI_API_KEY',
  'AI_MARKETING_MODEL',
  'REDDIT_CLIENT_ID',
  'REDDIT_CLIENT_SECRET',
  'REDDIT_USER_AGENT'
);
```

Do not paste credential values into logs, docs, migration files, or admin settings.
