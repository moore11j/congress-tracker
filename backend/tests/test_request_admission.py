import ast
import asyncio
import logging
import threading
import time
from enum import Enum
from pathlib import Path
from types import SimpleNamespace

from starlette.requests import Request
from starlette.responses import JSONResponse
from app.request_admission import acquire_without_blocking


class Priority(Enum):
    NORMAL = "normal"
    HEAVY = "heavy"


def middleware():
    # Execute the actual middleware without initializing production services.
    tree = ast.parse(Path("app/main.py").read_text())
    fn = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "log_slow_requests")
    fn.decorator_list = []
    gate = threading.BoundedSemaphore(2)
    heavy = threading.BoundedSemaphore(1)
    ns = dict(Request=Request, perf_counter=time.perf_counter, os=SimpleNamespace(getenv=lambda *a: None),
        is_production=lambda: True, logger=logging.getLogger("test"), RoutePriority=Priority,
        classify_request=lambda path, q: Priority.HEAVY if path == "/heavy" else Priority.NORMAL,
        _request_attribution_fields=lambda r, **k: dict(route_family="insider", request_source=r.headers.get("source", "client"),
          auth_state=r.headers.get("auth", "logged_out"), user_agent_class="unknown", panel="test"),
        set_request_context=lambda _: None, reset_request_context=lambda _: None,
        _should_bypass_heavy_route_slot=lambda *a: False,
        _is_secondary_analytics_path=lambda p: p.startswith("/api/insiders/"),
        _PUBLIC_PROFILE_SSR_SEMAPHORE=gate, _HEAVY_ROUTE_SEMAPHORE=heavy,
        _HEAVY_ROUTE_WAIT_SECONDS=.12, acquire_without_blocking=acquire_without_blocking,
        _ANALYTICS_TEMPORARILY_UNAVAILABLE="unavailable", JSONResponse=JSONResponse,
        retry_after_for_priority=lambda _: 2, _log_request_attribution=lambda *a, **k: None,
        _analytics_panel_name=lambda _: "summary", _should_log_request_attribution=lambda **k: False)
    exec(compile(ast.Module(body=[fn], type_ignores=[]), "actual_request_middleware", "exec"), ns)
    return ns["log_slow_requests"], gate, heavy


def request(path, source="client", auth="logged_out"):
    return Request(dict(type="http", method="GET", path=path, query_string=b"",
        headers=[(b"source", source.encode()), (b"auth", auth.encode())]))


def test_saturated_profiles_preserve_interactive_capacity_and_release_on_cancel():
    async def scenario():
        handler, gate, _ = middleware()
        entered = 0
        blocked = asyncio.Event()
        async def slow(r):
            nonlocal entered
            entered += 1
            await blocked.wait()
            return JSONResponse({"complete": True})
        tasks = [asyncio.create_task(handler(request("/api/insiders/1/summary", "ssr"), slow)) for _ in range(2)]
        await asyncio.sleep(.01)
        assert entered == 2
        for _ in range(24):
            response = await handler(request("/api/insiders/2/trades", "ssr"), slow)
            assert response.status_code == 503
            assert response.headers["cache-control"] == "no-store"
            assert response.headers["retry-after"] == "10"
        async def ready(r): return JSONResponse({"complete": True})
        for req in [request("/ready"), request("/api/events"), request("/api/insiders/3/summary"), request("/api/insiders/4/summary", "ssr", "logged_in")]:
            assert (await handler(req, ready)).status_code == 200
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        assert gate.acquire(blocking=False) and gate.acquire(blocking=False)
        assert not gate.acquire(blocking=False)
    asyncio.run(scenario())


def test_heavy_saturation_does_not_block_readiness_event_loop():
    async def scenario():
        handler, _, gate = middleware()
        gate.acquire()
        async def ready(r): return JSONResponse({"complete": True})
        begin = time.monotonic()
        heavy = asyncio.create_task(handler(request("/heavy"), ready))
        await asyncio.sleep(.01)
        assert (await handler(request("/ready"), ready)).status_code == 200
        assert time.monotonic() - begin < .08
        assert (await heavy).status_code == 503
        gate.release()
    asyncio.run(scenario())


def test_cancelled_waiter_never_consumes_a_later_permit():
    async def scenario():
        gate = threading.BoundedSemaphore(1)
        gate.acquire()
        waiter = asyncio.create_task(acquire_without_blocking(gate, 10))
        await asyncio.sleep(.01)
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        gate.release()
        await asyncio.sleep(.02)
        assert gate.acquire(blocking=False)
        gate.release()
        assert await acquire_without_blocking(gate, 0)
        assert not await acquire_without_blocking(gate, 0)
    asyncio.run(scenario())


def test_profile_exception_releases_admission_slot():
    async def scenario():
        handler, gate, _ = middleware()
        async def failure(r): raise ValueError("upstream")
        try: await handler(request("/api/insiders/1/summary", "ssr"), failure)
        except ValueError: pass
        else: raise AssertionError("exception lost")
        assert gate.acquire(blocking=False) and gate.acquire(blocking=False)
    asyncio.run(scenario())
