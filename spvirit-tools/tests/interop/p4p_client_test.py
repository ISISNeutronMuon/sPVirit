#!/usr/bin/env python3
"""p4p client test against a spvirit server.

Exercises p4p (pvxs) client operations against a running spvirit server.
Expects the server to already be running with the test DB (SIM:* PVs).

Environment variables:
    SPVIRIT_TEST_TCP_PORT  – TCP port of the spvirit server (required)
    SPVIRIT_TEST_UDP_PORT  – UDP port of the spvirit server (required)

Exit code 0 on success, 1 on any failure.
"""
import os
import sys
import time

from p4p.client.thread import Context


def main():
    tcp_port = os.environ.get("SPVIRIT_TEST_TCP_PORT")
    udp_port = os.environ.get("SPVIRIT_TEST_UDP_PORT")
    if not tcp_port or not udp_port:
        print("ERROR: SPVIRIT_TEST_TCP_PORT and SPVIRIT_TEST_UDP_PORT must be set",
              file=sys.stderr)
        sys.exit(1)

    # Configure p4p to connect only to our test server on loopback
    conf = {
        "EPICS_PVA_ADDR_LIST": "127.0.0.1",
        "EPICS_PVA_AUTO_ADDR_LIST": "NO",
        "EPICS_PVA_SERVER_PORT": tcp_port,
        "EPICS_PVA_BROADCAST_PORT": udp_port,
    }
    ctx = Context("pva", conf=conf, useenv=False)

    failures = []
    passed = 0

    def check(name, condition, msg=""):
        nonlocal passed
        if condition:
            passed += 1
            print(f"  PASS: {name}")
        else:
            failures.append(f"{name}: {msg}")
            print(f"  FAIL: {name} — {msg}")

    # ── GET scalar double ─────────────────────────────────────────────────
    print("GET scalar tests:")
    try:
        val = ctx.get("SIM:AI", timeout=10.0)
        check("GET SIM:AI returns value",
              val is not None, "got None")
        check("GET SIM:AI value is ~1.23",
              abs(float(val) - 1.23) < 0.01,
              f"got {val}")
    except Exception as e:
        check("GET SIM:AI", False, str(e))

    try:
        val = ctx.get("SIM:AO", timeout=10.0)
        check("GET SIM:AO returns value",
              val is not None, "got None")
        check("GET SIM:AO value is ~2.34",
              abs(float(val) - 2.34) < 0.01,
              f"got {val}")
    except Exception as e:
        check("GET SIM:AO", False, str(e))

    # ── GET string ────────────────────────────────────────────────────────
    try:
        val = ctx.get("SIM:STR", timeout=10.0)
        check("GET SIM:STR returns value",
              val is not None, "got None")
        check("GET SIM:STR value is 'hello'",
              str(val) == "hello",
              f"got '{val}'")
    except Exception as e:
        check("GET SIM:STR", False, str(e))

    # ── GET binary (boolean) ──────────────────────────────────────────────
    try:
        val = ctx.get("SIM:BI", timeout=10.0)
        check("GET SIM:BI returns value",
              val is not None, "got None")
    except Exception as e:
        check("GET SIM:BI", False, str(e))

    # ── PUT + readback ────────────────────────────────────────────────────
    print("\nPUT + readback tests:")
    try:
        ctx.put("SIM:AO", 99.5, timeout=10.0)
        time.sleep(0.2)
        val = ctx.get("SIM:AO", timeout=10.0)
        check("PUT SIM:AO 99.5 + readback",
              abs(float(val) - 99.5) < 0.01,
              f"got {val}")
        # Restore
        ctx.put("SIM:AO", 2.34, timeout=10.0)
    except Exception as e:
        check("PUT SIM:AO", False, str(e))

    try:
        ctx.put("SIM:STR", "world", timeout=10.0)
        time.sleep(0.2)
        val = ctx.get("SIM:STR", timeout=10.0)
        check("PUT SIM:STR 'world' + readback",
              str(val) == "world",
              f"got '{val}'")
        # Restore
        ctx.put("SIM:STR", "hello", timeout=10.0)
    except Exception as e:
        check("PUT SIM:STR", False, str(e))

    # ── MONITOR ───────────────────────────────────────────────────────────
    print("\nMONITOR test:")
    try:
        updates = []

        def cb(val):
            updates.append(val)

        sub = ctx.monitor("SIM:AI", cb, notify_disconnect=True)
        time.sleep(1.0)
        sub.close()
        check("MONITOR SIM:AI got initial update",
              len(updates) >= 1,
              f"got {len(updates)} updates")
    except Exception as e:
        check("MONITOR SIM:AI", False, str(e))

    # ── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Results: {passed} passed, {len(failures)} failed")
    if failures:
        for f in failures:
            print(f"  FAIL: {f}")
        sys.exit(1)
    else:
        print("All tests passed.")
        sys.exit(0)


if __name__ == "__main__":
    main()
