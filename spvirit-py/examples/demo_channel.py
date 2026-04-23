"""Persistent Channel demo — repeated get/put/introspect on one TCP connection.

Exercises ``spvirit.lowlevel.Channel``:
  - classmethod ``connect(pv_name, server_addr, timeout)``
  - ``get(fields=None)`` / ``put(value, fields=None)``
  - ``introspect()``
  - properties: ``pv_name``, ``server_addr``, ``sid``, ``is_open``
  - ``close()`` / context-manager usage

Usage::

    python3 demo_channel.py PV_NAME [SERVER_ADDR]

``SERVER_ADDR`` defaults to ``127.0.0.1:5075``.  If omitted and a UDP search
is needed, use :mod:`demo_discovery` to obtain an address first.
"""
from __future__ import annotations

import sys

from spvirit import codec
from spvirit.lowlevel import Channel


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    pv_name = sys.argv[1]
    server_addr = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1:5075"

    # ── Single connection, many operations ──────────────────────────────
    ch = Channel.connect(pv_name, server_addr, timeout=3.0)
    print(f"connected: {ch!r}")
    print(f"  pv_name     = {ch.pv_name}")
    print(f"  server_addr = {ch.server_addr}")
    print(f"  sid         = {ch.sid}")
    print(f"  is_open     = {ch.is_open}")

    # Introspect once, then reuse the description as needed.
    desc = ch.introspect()
    print(f"\nintrospection: struct_id={desc.struct_id!r}  fields={len(desc)}")
    for f in desc.fields:
        suffix = "[]" if f.is_array else ""
        print(f"  .{f.name}: {f.field_type}{suffix}")

    # Full get (all fields).
    result = ch.get()
    print(f"\nget() -> {codec.format_value(result.value)}")
    print(f"  raw_pva bytes: {len(result.raw_pva)}  raw_pvd bytes: {len(result.raw_pvd)}")

    # Narrow get (subset of fields) — reuses the same TCP connection.
    narrow = ch.get(fields=["value"])
    print(f"get(['value']) -> {narrow.value}")

    # Attempt a write — server ACLs may deny it; we tolerate that.
    try:
        ch.put(0.0)
        print("put(0.0) succeeded")
    except Exception as e:  # noqa: BLE001 — demo code
        print(f"put denied (ok for read-only PVs): {type(e).__name__}: {e}")

    ch.close()
    print(f"\nafter close: is_open={ch.is_open}")

    # ── Context-manager variant ─────────────────────────────────────────
    with Channel.connect(pv_name, server_addr, timeout=3.0) as ch2:
        v = ch2.get().value
        print(f"\n[ctxmgr] {pv_name} = {codec.extract_nt_value(v)}")
    print(f"[ctxmgr] is_open after with-block: {ch2.is_open}")


if __name__ == "__main__":
    main()
