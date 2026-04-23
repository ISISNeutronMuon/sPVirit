"""demo_source_async.py — async def Python source methods.

All `Source` hooks may be `async def` coroutines — the adapter detects
awaitables at runtime and schedules them on the shared Tokio runtime.
This lets Python providers cleanly mix network calls, asyncio locks,
and other coroutines without blocking the server.

Run, then::

    spget ASYNC:DOUBLE?q=21    # -> 42
    spget ASYNC:DOUBLE?q=3.5   # -> 7.0
"""

import asyncio
import time

import spvirit

PREFIX = "ASYNC:DOUBLE"


class AsyncDoubler:
    """Parse ``ASYNC:DOUBLE?q=<number>`` and return 2*q with simulated latency."""

    async def claim(self, name: str):
        if not name.startswith(PREFIX):
            return None
        # Simulate an async metadata lookup.
        await asyncio.sleep(0.01)
        return spvirit.PvInfo.nt_scalar("double")

    async def get(self, name: str):
        if not name.startswith(PREFIX):
            return None
        q = _parse_q(name)
        # Simulate an async compute.
        await asyncio.sleep(0.02)
        return spvirit.NtScalar(2.0 * q)

    async def put(self, name, value):
        raise RuntimeError("doubler is read-only")

    async def names(self):
        return [PREFIX + "?q=0"]


def _parse_q(name: str) -> float:
    if "?q=" in name:
        tail = name.split("?q=", 1)[1]
        try:
            return float(tail)
        except ValueError:
            return 0.0
    return 0.0


def main():
    server = (
        spvirit.ServerBuilder()
        .add_source("async_doubler", 10, AsyncDoubler())
        .build()
    )
    server.start_background()

    print("Async source server on port 5075.")
    print(f"  Try:  spget {PREFIX}?q=21     # returns 42")
    print(f"        spget {PREFIX}?q=3.5    # returns 7.0")

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\nbye.")


if __name__ == "__main__":
    main()
