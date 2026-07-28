# Walnut Reddit Ads Assistant

Internal Manifest V3 Chrome extension for approved Walnut Reddit ad drafts.

## Local Loading

1. Start the Walnut backend and frontend locally.
2. Open Walnut Admin, go to `AI Growth` -> `Reddit Ads Assistant`.
3. Create and approve a draft.
4. Click `Open in extension` to create a short-lived extension token.
5. Open `chrome://extensions`, enable Developer mode, and choose `Load unpacked`.
6. Select `chrome-extension/reddit-ads-assistant`.
7. Paste the token into the popup and refresh approved drafts.

## Security Notes

- The extension never stores the OpenAI API key, Reddit passwords, payment details, or Walnut session cookies.
- Approved drafts are fetched with a short-lived Walnut extension token.
- Draft generation happens only through the authenticated Walnut backend.
- The content script never submits forms, launches campaigns, clicks payment controls, or changes budget fields.
- Field filling requires an explicit `Fill approved Walnut draft` click inside Reddit Ads Manager.
- If Reddit Ads Manager fields are not recognized, the extension fails closed and the admin must use copy-to-clipboard.
- The included logo is copied directly from `backend/app/assets/walnut-markets-logo-lockup.png`.

## Known Reddit Ads Manager Selector Limitations

Selectors are versioned as `reddit_ads_manager_2026_07_v1` in `src/content-script.js`.

Reddit may change label text, field names, or React-managed input behavior. When that happens, update only `FIELD_SELECTORS`, run the extension tests, and manually verify that:

- only approved fields populate,
- undo restores original values,
- no submit, launch, payment, or budget controls are clicked,
- the admin can inspect every populated field before manual submission.

## Validation

Run extension tests:

```powershell
node --test chrome-extension/reddit-ads-assistant/tests/*.test.mjs
```

No build step is required because the extension is plain MV3 HTML, CSS, and JavaScript.
