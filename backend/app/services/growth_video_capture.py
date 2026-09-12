"""Allowlisted public UI capture. No model-authored selectors or browser code."""
from __future__ import annotations

import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit

from app.services.growth_video_domain import allowed_route, now

TARGETS = {
    "research_header": '[data-growth-capture="research-header"]',
    "research_summary": '[data-growth-capture="research-summary"]',
}
# Existing published research has stable semantic markup. These explicit fallbacks
# allow the worker to operate during a rolling frontend deployment.
LEGACY_TARGETS = {"research_header": "main h1", "research_summary": "main h1 + p"}
VIEWPORT = {"width": 1060, "height": 1440}


def validate_command(scene):
    allowed_route(scene["walnut_url"])
    if scene["capture_target"] not in TARGETS or scene["capture_action"] not in {"screenshot", "record"}:
        raise ValueError("Capture command is not allowlisted.")
    return TARGETS[scene["capture_target"]]


def capture_scene(scene, *, expected_title):
    selector = validate_command(scene)
    from playwright.sync_api import sync_playwright
    # Fresh contexts omit user cookies and admin state by construction.
    last_error = None
    for attempt in range(2):
        try:
            with tempfile.TemporaryDirectory(prefix="walnut-capture-") as directory, sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                context = browser.new_context(viewport=VIEWPORT, device_scale_factor=1,
                    color_scheme="dark", reduced_motion="reduce", locale="en-US", timezone_id="UTC",
                    record_video_dir=directory if scene["capture_action"] == "record" else None,
                    record_video_size=VIEWPORT if scene["capture_action"] == "record" else None)
                page = context.new_page()
                # Top-level redirects cannot escape the explicit capture host/route.
                def route_guard(route):
                    request = route.request
                    if request.is_navigation_request() and request.frame == page.main_frame:
                        try:
                            allowed_route(request.url)
                        except ValueError:
                            return route.abort()
                    route.continue_()
                page.route("**/*", route_guard)
                started = time.monotonic()
                response = page.goto(scene["walnut_url"], wait_until="domcontentloaded", timeout=45000)
                if response is None or response.status != 200 or page.url != scene["walnut_url"]:
                    raise ValueError("Canonical research page is unavailable or redirected.")
                page.locator("main h1").wait_for(state="visible", timeout=20000)
                title = page.locator("h1").inner_text()
                if " ".join(title.split()) != " ".join(expected_title.split()):
                    raise ValueError("Captured research title does not match its approved source.")
                page.evaluate("document.fonts.ready")
                page.add_style_tag(content="*,*::before,*::after{animation:none!important;transition:none!important;caret-color:transparent!important}")
                target = page.locator(selector)
                if target.count() == 0:
                    selector = LEGACY_TARGETS[scene["capture_target"]]
                    target = page.locator(selector)
                if target.count() != 1:
                    raise ValueError("Capture target is missing or ambiguous.")
                target.scroll_into_view_if_needed()
                page.wait_for_timeout(800)
                consent = page.get_by_role("button", name="Reject optional", exact=True)
                if consent.count() == 1 and consent.is_visible():
                    consent.click(timeout=3000)
                    page.wait_for_timeout(200)
                screenshot = target.screenshot(animations="disabled")
                trim_start = time.monotonic() - started
                if scene["capture_action"] == "record":
                    page.wait_for_timeout(5000)
                    video = page.video
                    context.close()
                    content = Path(video.path()).read_bytes()
                    mime = "video/webm"
                else:
                    content, mime = screenshot, "image/png"
                    context.close()
                browser.close()
                return content, mime, {"page_url": scene["walnut_url"], "captured_at": now(),
                    "component": scene["capture_target"], "selector": selector, "viewport": VIEWPORT,
                    "evidence_ids": scene["evidence_ids"], "attempts": attempt + 1,
                    "trim_start": trim_start, "page_title": title}, screenshot
        except Exception as exc:
            last_error = exc
    raise ValueError("Real Walnut capture failed after two attempts. Check the published page and capture targets.") from last_error
