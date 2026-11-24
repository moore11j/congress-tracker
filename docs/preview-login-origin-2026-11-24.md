# Preview login origin

November 24, 2026. Owner reported a page-not-found screen after opening the My Stocks preview. The screenshot is on app.walnutmarkets.com, not the Vercel preview. PR846 is still a draft and the new route is absent from the production release.

The code explains the route: backend `_google_redirect_uri` deliberately selects the production app callback. The preview sends a relative `/my-stocks` return path in signed OAuth state. The production callback then navigates to that relative path on production. Origin-scoped preview cookies cannot be supplied by the production callback. This is a strong code-supported diagnosis consistent with the screenshot, not a captured trace of the owner's phone login.

The login panel now checks the supplied authorization URL's callback before leaving the page or storing a return hint. An incompatible callback keeps the user on the preview with a clear explanation and a Discover link. Production app login and the existing canonical marketing-to-app flow remain supported. The callback URL, backend OAuth configuration, cookies and account records are unchanged. No wildcard callback or token bridge is introduced.

Five new cases cover the origin matrix, malformed authorization responses and the actual login-button handler for blocked preview and allowed production navigation. Existing workspace and auth checks also run. This fix prevents the dead end; it does not enable Google login on preview domains. Existing password sign-in remains available through the same-origin API proxy; a populated owner session still requires browser acceptance. Google-only accounts need a deliberately configured preview authentication environment before that acceptance can pass.

Independent scheduled observation at 02:10:01 UTC returned HTTP200 and three guest rankings with generated_at and universe_generated_at both 2026-11-24T02:00:04.195771Z. This confirms the scheduled snapshot advanced; it does not prove all underlying data is fresh. Receipt is retained in root artifacts/scheduled-ranking-observation-nov24.json. No manual refresh was triggered.

No production deployment, feature activation, provider request or customer send is part of this change.
