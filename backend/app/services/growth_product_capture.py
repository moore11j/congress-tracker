"""Real product footage using an ephemeral session for the approving admin owner."""
import tempfile
import time
import re
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

from app.services.growth_product_ad import TICKER_URL, OUTCOMES_URL
from app.services.growth_video_domain import now, digest

VIEWPORT = {"width": 1440, "height": 1100}


def capture_product_shot(shot, *, owner_id=None, session_token=None):
    if shot not in {"chart", "score", "risks", "outcomes"}:
        raise ValueError("Unsupported product shot.")
    from playwright.sync_api import sync_playwright
    url = OUTCOMES_URL if shot == "outcomes" else TICKER_URL
    with tempfile.TemporaryDirectory(prefix="walnut-product-") as directory, sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(viewport=VIEWPORT, device_scale_factor=1, color_scheme="dark",
            locale="en-US", timezone_id="UTC")
        # Use Walnut's real opt-out preference in this disposable browser context.
        # This also keeps recording visits out of acquisition analytics.
        context.add_cookies([{"name": "walnut_privacy_consent", "value": "v1.a0.m0",
            "domain": "app.walnutmarkets.com", "path": "/", "secure": True, "sameSite": "Lax"}])
        if owner_id is not None:
            # An authorized local recovery worker can supply an ephemeral session
            # through memory; production workers sign their own session in memory.
            token = session_token
            if token is None:
                from app.auth import sign_session_payload
                token = sign_session_payload({"uid": owner_id, "exp": int(time.time()) + 900})
            context.add_cookies([{"name": "ct_session", "value": token, "domain": host,
                "path": "/", "secure": True, "httpOnly": True, "sameSite": "None", "expires": time.time()+900}
                for host in ["app.walnutmarkets.com", "api.walnutmarkets.com", "congress-tracker-api.fly.dev"]])
            context.add_cookies([{"name":"ct_auth_hint", "value":"1", "domain":"app.walnutmarkets.com", "path":"/", "secure":True}])
        page = context.new_page()
        def guard(route):
            parsed = urlsplit(route.request.url)
            if parsed.scheme in {"http", "https"}:
                if parsed.hostname not in {"app.walnutmarkets.com", "api.walnutmarkets.com", "congress-tracker-api.fly.dev", "fonts.googleapis.com", "fonts.gstatic.com"}:
                    return route.abort()
                if route.request.method not in {"GET", "HEAD", "OPTIONS"}:
                    return route.abort()
                if parsed.path.startswith("/api/") and not (
                    parsed.path in {"/api/auth/me", "/api/entitlements"} or
                    parsed.path.startswith(("/api/tickers/NVDA", "/api/tickers/SPY", "/api/outcomes/"))):
                    return route.abort()
            if route.request.is_navigation_request() and route.request.frame == page.main_frame and route.request.url != url:
                return route.abort()
            route.continue_()
        page.route("**/*", guard)
        response = page.goto(url, wait_until="domcontentloaded", timeout=60000)
        if not response or response.status != 200 or page.url != url:
            raise ValueError("Public product page unavailable.")
        page.locator("main").first.wait_for(timeout=45000)
        if shot != "outcomes":
            page.get_by_role("heading", level=1).filter(has_text="NVIDIA").wait_for(timeout=45000)
        if shot == "chart":
            target = page.get_by_role("heading", name="NVDA vs S&P 500 (SPY)", exact=True).locator("xpath=ancestor::section[1]")
            target.evaluate("node => node.scrollIntoView({block: 'center', inline: 'nearest'})")
            target.locator("canvas").first.wait_for(state="visible", timeout=45000)
        elif shot in {"score", "risks"}:
            target = page.get_by_role("button", name="Overview", exact=True).locator("xpath=ancestor::section[1]")
            target.get_by_text(re.compile("30.day confirmation", re.I)).first.wait_for(timeout=45000)
        else:
            target = page.get_by_role("heading", name="Confirmation Events", exact=True).locator("xpath=ancestor::section[1]")
            target.wait_for(state="visible", timeout=45000)
        page.evaluate("Promise.race([document.fonts.ready, new Promise(resolve => setTimeout(resolve, 5000))])")
        target.evaluate("node => node.scrollIntoView({block: 'center', inline: 'nearest'})")
        page.wait_for_timeout(2000)
        if shot in {"score", "risks"}:
            # Wait for the owner's entitlements and hydrated live context.
            target.get_by_role("button", name="Unlock with Premium", exact=True).first.wait_for(state="hidden", timeout=45000)
        if shot == "risks":
            target.get_by_role("heading", name="RISKS", exact=True).evaluate("node => node.scrollIntoView({block: 'center', inline: 'nearest'})")
        target_text = target.inner_text()
        if len(target_text) < 80 or re.search(r"Sign in to unlock|Unlock with Premium|freshness setup are available with Premium", target_text, re.I):
            raise ValueError("Product capture is empty or gated; no substitute screen will be generated.")
        if shot == "risks":
            target = target.get_by_role("heading", name="RISKS", exact=True).locator("xpath=ancestor::section[1]/..")
            target.evaluate("node => node.scrollIntoView({block: 'center', inline: 'nearest'})")
        box = target.bounding_box()
        if not box or box["width"] < 300:
            raise ValueError("Product component is not ready to capture.")
        crop = {"x": max(0, int(box["x"])), "y": max(0, int(box["y"])),
                "width": min(int(box["width"]), VIEWPORT["width"]-max(0,int(box["x"]))),
                "height": min(int(box["height"]), VIEWPORT["height"]-max(0,int(box["y"])))}
        if shot == "score":
            crop["height"] = min(crop["height"], 730)
        thumbnail = page.screenshot(clip=crop, animations="disabled", timeout=90000)
        # Capture only the prepared product action. Recording during initial page
        # hydration starves the small shared worker and creates long useless clips.
        # Every frame is the real browser; no screen content is synthesized.
        x, y = crop["x"] + crop["width"]*.5, crop["y"] + crop["height"]*.55
        page.mouse.move(x, y)
        root = Path(directory)
        recording_deadline = time.monotonic() + 240
        for frame in range(60):
            if time.monotonic() >= recording_deadline:
                raise ValueError("Product browser recording exceeded its time budget.")
            if shot in {"score", "risks", "outcomes"}:
                page.mouse.wheel(0, 3 if shot == "score" else -2 if shot == "risks" else 2)
            else:
                page.mouse.move(crop["x"] + crop["width"]*(.25+.5*frame/60), y)
            page.screenshot(path=str(root/f"frame-{frame:03d}.png"), animations="disabled", timeout=min(30000, max(1000, int((recording_deadline-time.monotonic())*1000))))
        context.close()
        browser.close()
        import imageio_ffmpeg
        output = root / "capture.mp4"
        subprocess.run([imageio_ffmpeg.get_ffmpeg_exe(), "-y", "-v", "error",
            "-framerate", "12", "-i", str(root/"frame-%03d.png"), "-c:v", "libx264",
            "-preset", "veryfast", "-crf", "16", "-threads", "1", "-pix_fmt", "yuv420p",
            "-movflags", "+faststart", str(output)], capture_output=True, check=True, timeout=180)
        content = output.read_bytes()
        return content, thumbnail, {"page_url": url, "captured_at": now(), "page_title": "NVIDIA" if shot != "outcomes" else "Outcomes",
            "component": shot, "viewport": VIEWPORT, "crop": crop, "trim_start": 0, "media_type": "video/mp4", "capture_method": "real_browser_frames", "frame_rate": 12,
            "clip_duration": 5, "source_text": target_text[:16000], "source_hash": digest(target_text), "public_context": owner_id is None, "authorized_product_demo": owner_id is not None}
