"""Opt-in local browser test. Real React UI + authenticated API; paid providers mocked.

Start frontend on localhost:3119, set GROWTH_VIDEO_BROWSER_TEST=true and install
the Playwright Chromium runtime. Never points at a production admin account.
"""
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base, get_db
from app.models import UserAccount
from app.routers import growth_video as api
from app.services.ai_marketing import public_settings_payload
from test_growth_video import prepared, render_to_review


@pytest.mark.skipif(os.getenv("GROWTH_VIDEO_BROWSER_TEST") != "true", reason="Opt-in local browser integration test")
def test_video_admin_browser_flow(monkeypatch):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    engine=create_engine("sqlite://",connect_args={"check_same_thread":False},poolclass=StaticPool)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        api.store.ensure_schema(db)
        user=UserAccount(email="browser-fixture@example.test",role="admin")
        db.add(user);db.commit()
        item,opp,_,_=prepared(db,monkeypatch)
        item,_,_,_=render_to_review(db,item)
        app=FastAPI();app.include_router(api.router,prefix="/api")
        app.dependency_overrides[get_db]=lambda:db
        app.dependency_overrides[api.rate_limit_admin_mutation]=lambda:None
        client=TestClient(app)
        session=sign_session_payload({"uid":user.id,"email":user.email})
        browser_errors=[]
        def respond(route):
            request=route.request;parts=urlsplit(request.url);path=parts.path
            if path.startswith("/api/admin/ai-growth/video"):
                response=client.request(request.method,path+("?"+parts.query if parts.query else ""),content=request.post_data,
                    headers={"cookie":f"{SESSION_COOKIE_NAME}={session}","content-type":"application/json"})
                route.fulfill(status=response.status_code,content_type="application/json",body=response.content)
                return
            if path=="/api/auth/me":
                result={"user":{"id":user.id,"email":user.email,"role":"admin","is_admin":True,"entitlement_tier":"admin"}}
            elif path=="/api/admin/settings":
                result={"plan_config":{"plan_limits":[],"plan_prices":[],"features":[]},"feature_gates":[]}
            elif path=="/api/admin/ai-marketing/settings":
                result=public_settings_payload(db)
            else:
                result={"items":[],"config":{},"count":0}
            route.fulfill(status=200,content_type="application/json",body=json.dumps(result))
        with sync_playwright() as pw:
            browser=pw.chromium.launch(headless=True)
            page=browser.new_page(viewport={"width":1440,"height":1100})
            page.on("pageerror",lambda error:browser_errors.append(error.stack))
            page.context.add_cookies([{"name":SESSION_COOKIE_NAME,"value":session,"url":"http://localhost:3119"},{"name":"ct_auth_hint","value":"1","url":"http://localhost:3119"}])
            page.route("**/api/**",respond)
            page.goto("http://localhost:3119/admin/ai-marketing",wait_until="domcontentloaded",timeout=120000)
            page.get_by_role("button",name="Reject optional",exact=True).click(timeout=30000)
            try:
                page.get_by_role("button",name="Content Opportunities",exact=True).click(timeout=30000)
            except Exception:
                print({"url":page.url,"errors":browser_errors,"body":page.locator("body").inner_text()[:3500]})
                page.screenshot(path="artifacts/growth-video-ui-failure.png",full_page=True)
                raise
            page.get_by_role("button",name="Why this opportunity").click()
            page.get_by_text("Source research",exact=True).first.wait_for()
            assert page.get_by_text("Investigating NVDA",exact=True).count()>=1
            page.get_by_role("button",name="Growth Brief",exact=True).click()
            page.get_by_label("brand voice",exact=True).fill("Specific, dated, investigative.")
            page.get_by_role("button",name="Save Growth Brief",exact=True).click()
            page.get_by_text("Growth Brief version saved.",exact=True).wait_for()
            page.get_by_role("button",name="Draft Queue",exact=True).click()
            page.get_by_role("button",name="Approve",exact=True).click()
            page.get_by_role("button",name="Download approved MP4",exact=True).wait_for()
            page.get_by_label("Review feedback",exact=True).fill("Use a more specific opening next time.")
            page.get_by_role("button",name="Reject with feedback",exact=True).click()
            page.get_by_role("button",name="Growth Memory",exact=True).click()
            page.get_by_role("button",name="Save lesson",exact=True).first.wait_for()
            output=Path("artifacts/growth-video-ui.png")
            page.screenshot(path=str(output),full_page=True)
            assert not browser_errors,browser_errors
            browser.close()
    engine.dispose()
