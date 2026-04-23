"""Async Channel demo — the same operations as demo_channel but via asyncio.

Every Channel method has an ``_async`` twin that returns an awaitable
integrated with Python's asyncio loop (via ``pyo3-async-runtimes``).

Usage::

    python3 demo_channel_async.py PV_NAME [SERVER_ADDR]
"""
from __future__ import annotations

import asyncio
import sys

from spvirit.lowlevel import Channel


async def run(pv_name: str, server_addr: str) -> None:
    ch = await Channel.connect_async(pv_name, server_addr, timeout=3.0)
    print(f"[async] connected: sid={ch.sid}  addr={ch.server_addr}")

    desc = await ch.introspect_async()
    print(f"[async] introspect: struct_id={desc.struct_id!r}  fields={len(desc)}")

    result = await ch.get_async()
    print(f"[async] get() -> pv_name={result.pv_name}")

    # Run a few gets concurrently over the same channel.  They still go
    # through the Channel's internal mutex, so they serialise on the
    # wire — but awaiting many at once is the idiomatic async pattern.
    async def one(i: int) -> None:
        r = await ch.get_async(fields=["value"])
        print(f"[async] parallel get #{i}: {r.value!r}")

    await asyncio.gather(*(one(i) for i in range(3)))

    ch.close()


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    pv_name = sys.argv[1]
    server_addr = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1:5075"
    asyncio.run(run(pv_name, server_addr))


if __name__ == "__main__":
    main()
