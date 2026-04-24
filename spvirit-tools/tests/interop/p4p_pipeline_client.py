#!/usr/bin/env python3
"""p4p pipelined-monitor interop client.

Connects to a spvirit server on loopback, subscribes to the given PV
using a pipelined pvRequest (``record[pipeline=true,queueSize=N]``),
drives updates from a background thread, and exits 0 iff at least the
target number of monitor callbacks fired.

Environment variables:
    SPVIRIT_TEST_TCP_PORT  - TCP port of the spvirit server (required)
    SPVIRIT_TEST_UDP_PORT  - UDP port of the spvirit server (required)
    SPVIRIT_TEST_PV        - PV to monitor and drive (required)
    SPVIRIT_PIPELINE_QSIZE - pipeline queue size (default: 4)
    SPVIRIT_PIPELINE_TARGET- minimum callback count required (default: 6)
    SPVIRIT_PIPELINE_TIMEOUT - seconds to wait (default: 10)

Exit code 0 on success, 1 on failure.
"""
import os
import sys
import threading
import time

from p4p.client.thread import Context


def main():
    tcp_port = os.environ.get("SPVIRIT_TEST_TCP_PORT")
    udp_port = os.environ.get("SPVIRIT_TEST_UDP_PORT")
    pv = os.environ.get("SPVIRIT_TEST_PV")
    if not (tcp_port and udp_port and pv):
        print("ERROR: SPVIRIT_TEST_TCP_PORT, SPVIRIT_TEST_UDP_PORT and "
              "SPVIRIT_TEST_PV are required", file=sys.stderr)
        return 1

    qsize = int(os.environ.get("SPVIRIT_PIPELINE_QSIZE", "4"))
    target = int(os.environ.get("SPVIRIT_PIPELINE_TARGET", "6"))
    timeout = float(os.environ.get("SPVIRIT_PIPELINE_TIMEOUT", "10"))

    conf = {
        "EPICS_PVA_ADDR_LIST": "127.0.0.1",
        "EPICS_PVA_AUTO_ADDR_LIST": "NO",
        "EPICS_PVA_SERVER_PORT": tcp_port,
        "EPICS_PVA_BROADCAST_PORT": udp_port,
    }
    ctx = Context("pva", conf=conf, useenv=False)

    count = 0
    lock = threading.Lock()
    done = threading.Event()

    def cb(value):
        nonlocal count
        with lock:
            count += 1
            if count >= target:
                done.set()

    # pvxs/p4p pvRequest syntax for pipelining.
    req = "field() record[pipeline=true,queueSize=%d]" % qsize
    sub = ctx.monitor(pv, cb, request=req)

    # Drive updates from a background thread so the server has something
    # to send once the monitor is up.
    def driver():
        # small warm-up so the subscription is established first
        time.sleep(0.3)
        for i in range(target * 3):
            if done.is_set():
                return
            try:
                ctx.put(pv, 10.0 + i, timeout=5.0)
            except Exception as e:
                print(f"  put warn: {e}", file=sys.stderr)
            time.sleep(0.05)

    drv = threading.Thread(target=driver, daemon=True)
    drv.start()

    ok = done.wait(timeout=timeout)
    sub.close()

    with lock:
        final = count

    if ok and final >= target:
        print(f"PASS: received {final} pipelined frames (target {target}, "
              f"queueSize {qsize})")
        return 0

    print(f"FAIL: only received {final} pipelined frames in {timeout}s "
          f"(target {target}, queueSize {qsize})", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
