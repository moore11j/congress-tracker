"""Cooperative acquisition of gates shared with synchronous request handlers."""
import asyncio
import time


async def acquire_without_blocking(semaphore, timeout: float) -> bool:
    deadline = time.monotonic() + max(timeout, 0)
    while True:
        if semaphore.acquire(blocking=False):
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        # No executor waiter remains to acquire a permit after cancellation.
        await asyncio.sleep(min(0.01, remaining))
