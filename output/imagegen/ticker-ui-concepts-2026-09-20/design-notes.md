# Walnut ticker UI concepts

Design mockups only. No application code, calculation, deployment, or production data was changed.

Generated with the built-in image-generation tool using the supplied screenshots as references. This tool exposes no model-version selector; the requested “Image 2.5” version could not be independently selected or verified. Generated raster text/charts are conceptual and are not authoritative financial data or a pixel-perfect specification.

## Recommended direction

### Overview — `overview-compact.png`

Combine the existing **30-day** confirmation (the screenshot's actual window), cross-source divergence, and trend chart into a single compact summary band. Keep their meanings separate: confirmation score, source alignment, and historical trend are not interchangeable measures.

Replace large bullish/bearish subcards with wrapping source labels and explicit counts. Do not allocate half a row to an empty bearish box. If bearish evidence exists, give it equal readability rather than hiding it. Keep neutral/inactive state distinctions and existing methodology available.

Use borderless analytical sections with thin dividers. Stack Risks directly above What to Watch Next so a short risk list does not create a tall empty card. Keep all current findings, source dates, and links. The concept explores moving What Changed below Catalysts; final ordering can retain today's priority without changing the overall design.

Keep historical comparisons and Research Memory entry points. Use labeled, keyboard-accessible disclosures for detailed comparisons and methodology. The image's compact Options disclosure is optional: the implementation can keep the current sidebar unchanged while applying only the central layout redesign.

### Research — `research-editorial.png`

Display related Walnut briefs as editorial rows with titles, descriptions, and links. Remove redundant outer and inner card borders.

Combine coverage into one compact strip with separate source statuses and counts. Per-source attempt/success dates, limitations, and error states remain in Coverage details; don't collapse partial/failed/not-checked into a green check.

Use one evidence list, with category filters for Catalysts, Risks, Opportunities, and Watch next. A finding with multiple classifications can have multiple tags rather than repeat its full content. Filtering must preserve all records and category membership; the default is All findings.

Keep source type, date, headline, and meaningful materiality/confidence visible. Expand a row to show its complete interpretation, original excerpt, source link (only if available), and any explicit future milestone. Never truncate away uncertainty or substitute a date-only label for a full company development.

## No-information-loss checklist for a later implementation

- Score, direction, strength, update date, trend inspection, calibration dates and explanatory text.
- Bullish/bearish source names and counts; weighted-divergence explanation and methodology version. A count bar must remain labeled as source counts, not weighted contribution.
- Every change, catalyst, risk, and watch item; timestamps and available provenance.
- Historical sample sizes, current profile, comparison rationale, scores, pending outcomes, methodology and links.
- Private Research Memory status and creation/view actions.
- Both research briefs' complete descriptions and links.
- All source coverage states, counts, timestamps and source limitations.
- Every evidence record, full summary, excerpt, confidence, materiality, category, source link, and watch milestone.
- Existing right-side source modules and all their metrics/charts.

Reduce default visual weight, not font size. Details should be reachable by an explicit click/tap and keyboard—not hover-only. Mobile should stack naturally without horizontal table scrolling. Validate an expanded state as well as the compact default before claiming no information loss.

## Prompt set / generation direction

**Overview:** High-fidelity, implementable Walnut dark-navy desktop UI using the supplied MU Overview as its content reference. Combine confirmation, alignment, and trend into one shallow band; replace empty bearish space with inline source counts and labels; use flowing, compact analytical lists; keep score methodology, calibration, comparisons, existing source modules, and private-research actions accessible. Preserve supplied data, use readable type, and avoid nested cards. A follow-up edit removed generated options metrics in favor of an existing-details disclosure and corrected supplied dates/labels. Use the final image, not the initial generated draft.

**Research:** High-fidelity Walnut Research tab using the supplied annotated screenshot as a reference, excluding the red annotations and browser chrome. Related briefs become borderless editorial rows, coverage becomes one source-specific strip, and Company Developments becomes a single categorized evidence list. Preserve full finding text, source dates, confidence, materiality, source access, quotations, and future milestones via clearly labeled details. Keep the current narrow metrics sidebar, avoid fabricated facts or thesis-health states, and show that more findings continue below the viewport.

These are static concept images. Clickable filters/disclosures in the images are proposed interactions, not working prototypes. Any implementation still needs field-by-field preservation tests and desktop/mobile accessibility QA.
