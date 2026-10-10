# Article-feed shutdown readiness, October 9

The authorized FMP exit requires article-reactive marketing jobs to respect the global provider off switch. This scoped release adds the missing request guard and usage accounting, makes scheduled/manual article runs return provider_disabled before source/model/email work, and displays that state in the admin interface. Saved campaigns, cadence, drafts and history remain intact; other growth schedulers retain their behavior. The flag remains off in production during this preparation release.

Exact-release checks on Python 3.14.2: all 65 marketing and retirement tests pass. The first broader run exposed three HTTP fixtures missing status_code after response accounting was added; the fixtures now implement the real response contract. Ten frontend checks and TypeScript pass. Isolated tests cover repeated forced/manual calls, no database/provider work in disabled scheduler dispatch, unchanged campaign/history counts, and other scheduler dispatch. No customer email or campaign execution was performed.

This closes an unguarded caller for the eventual shutdown; it is not a substitute full-article news service or a claim that global FMP retirement is ready. Existing dependency audit failures remain separate. Deployment/runtime verification is pending at this checkpoint.
