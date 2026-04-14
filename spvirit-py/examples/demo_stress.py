"""Stress-test for the spvirit Python PVAccess bindings.

Starts an embedded server with PVs of every type, then hammers it with
rapid get/put/monitor operations from multiple threads.  Prints throughput
and latency statistics at the end.

Usage:
    python examples/demo_stress.py
    python examples/demo_stress.py --duration 30 --workers 8
"""

import argparse
import math
import random
import statistics
import threading
import time

import spvirit

# ─── CLI args ─────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser(description="spvirit stress test")
parser.add_argument("--duration", type=float, default=10.0, help="seconds to run (default 10)")
parser.add_argument("--workers", type=int, default=4, help="parallel worker threads (default 4)")
parser.add_argument("--port", type=int, default=5175, help="TCP port (default 5175)")
args = parser.parse_args()

DURATION = args.duration
N_WORKERS = args.workers
PORT = args.port

# ─── Build a server with all record types ─────────────────────────────────────

builder = spvirit.ServerBuilder()

# Scalar types
builder.ai("STRESS:AI", 0.0)
builder.ao("STRESS:AO", 0.0)
builder.bi("STRESS:BI", False)
builder.bo("STRESS:BO", False)
builder.string_in("STRESS:SI", "hello")
builder.string_out("STRESS:SO", "world")

# Array types
builder.waveform("STRESS:WF", [0.0] * 256)
builder.aai("STRESS:AAI", [0] * 128)
builder.aao("STRESS:AAO", [1.0, 2.0, 3.0])
builder.sub_array("STRESS:SUB", list(range(64)), indx=0, nelm=16)

# Table
builder.nt_table("STRESS:TABLE", {
    "x": [float(i) for i in range(10)],
    "y": [float(i * i) for i in range(10)],
})

# NdArray (8x8 image)
builder.nt_ndarray("STRESS:IMAGE", [0.0] * 64, [(8, 8), (8, 8)])

# Enum-style
builder.ao("STRESS:ENUM", 0)

builder.port(PORT)
builder.udp_port(PORT + 1)
builder.beacon_period(5)

server = builder.build()
store = server.start_background()

pv_names = store.pv_names()
print(f"Server started on port {PORT} with {len(pv_names)} PVs")
print(f"Running stress test for {DURATION}s with {N_WORKERS} workers ...\n")

# Give the server a moment to bind
time.sleep(0.5)

# ─── Shared counters ─────────────────────────────────────────────────────────

lock = threading.Lock()
stats = {
    "get_count": 0,
    "put_scalar_count": 0,
    "put_array_count": 0,
    "put_nt_count": 0,
    "get_nt_count": 0,
    "errors": 0,
    "get_latencies": [],
    "put_latencies": [],
}


def record_stat(key, count=1):
    with lock:
        stats[key] += count


def record_latency(key, dt):
    with lock:
        stats[key].append(dt)


# ─── Worker functions ─────────────────────────────────────────────────────────

stop_event = threading.Event()


def worker_get_put(worker_id: int):
    """Rapidly get and put scalar values via the Store handle."""
    scalar_pvs = ["STRESS:AI", "STRESS:AO", "STRESS:BI", "STRESS:BO",
                   "STRESS:SI", "STRESS:SO", "STRESS:ENUM"]
    array_pvs = ["STRESS:WF", "STRESS:AAI", "STRESS:AAO", "STRESS:SUB"]

    while not stop_event.is_set():
        # GET scalar
        pv = random.choice(scalar_pvs)
        t0 = time.perf_counter()
        try:
            store.get_value(pv)
            dt = time.perf_counter() - t0
            record_stat("get_count")
            record_latency("get_latencies", dt)
        except Exception:
            record_stat("errors")

        # PUT scalar
        pv = random.choice(["STRESS:AO", "STRESS:BO", "STRESS:SO", "STRESS:ENUM"])
        t0 = time.perf_counter()
        try:
            if pv == "STRESS:BO":
                store.set_value(pv, random.choice([True, False]))
            elif pv == "STRESS:SO":
                store.set_value(pv, f"msg-{worker_id}-{random.randint(0, 999)}")
            elif pv == "STRESS:ENUM":
                store.set_value(pv, random.randint(0, 3))
            else:
                store.set_value(pv, random.gauss(22.5, 2.0))
            dt = time.perf_counter() - t0
            record_stat("put_scalar_count")
            record_latency("put_latencies", dt)
        except Exception:
            record_stat("errors")

        # PUT array
        pv = random.choice(array_pvs)
        t0 = time.perf_counter()
        try:
            if pv in ("STRESS:AAI", "STRESS:SUB"):
                data = [random.randint(0, 255) for _ in range(128)]
            else:
                data = [random.gauss(0, 1) for _ in range(256)]
            store.set_array_value(pv, data)
            dt = time.perf_counter() - t0
            record_stat("put_array_count")
            record_latency("put_latencies", dt)
        except Exception:
            record_stat("errors")


def worker_nt(worker_id: int):
    """Rapidly read/write full NT payloads."""
    while not stop_event.is_set():
        # GET NT
        pv = random.choice(["STRESS:AI", "STRESS:WF", "STRESS:TABLE", "STRESS:IMAGE"])
        t0 = time.perf_counter()
        try:
            store.get_nt(pv)
            dt = time.perf_counter() - t0
            record_stat("get_nt_count")
            record_latency("get_latencies", dt)
        except Exception:
            record_stat("errors")

        # PUT NT (scalar)
        t0 = time.perf_counter()
        try:
            nt = spvirit.NtScalar(random.gauss(0, 10), units="degC")
            store.put_nt("STRESS:AO", nt)
            dt = time.perf_counter() - t0
            record_stat("put_nt_count")
            record_latency("put_latencies", dt)
        except Exception:
            record_stat("errors")

        # PUT NT (array)
        t0 = time.perf_counter()
        try:
            nt = spvirit.NtScalarArray([math.sin(i * 0.1) for i in range(256)])
            store.put_nt("STRESS:WF", nt)
            dt = time.perf_counter() - t0
            record_stat("put_nt_count")
            record_latency("put_latencies", dt)
        except Exception:
            record_stat("errors")


# ─── Launch workers ───────────────────────────────────────────────────────────

threads = []
for i in range(N_WORKERS):
    t = threading.Thread(target=worker_get_put, args=(i,), daemon=True)
    t.start()
    threads.append(t)
    # Half the workers also do NT operations
    if i % 2 == 0:
        t2 = threading.Thread(target=worker_nt, args=(i,), daemon=True)
        t2.start()
        threads.append(t2)

# ─── Run for the configured duration ──────────────────────────────────────────

start = time.time()
try:
    while time.time() - start < DURATION:
        time.sleep(0.25)
        elapsed = time.time() - start
        with lock:
            total = (stats["get_count"] + stats["put_scalar_count"]
                     + stats["put_array_count"] + stats["put_nt_count"]
                     + stats["get_nt_count"])
        rate = total / elapsed if elapsed > 0 else 0
        print(f"\r  [{elapsed:5.1f}s] {total:>8,} ops  ({rate:>8,.0f} ops/s)", end="", flush=True)
except KeyboardInterrupt:
    pass
stop_event.set()
for t in threads:
    t.join(timeout=2)

# ─── Report ───────────────────────────────────────────────────────────────────

elapsed = time.time() - start
total_ops = (stats["get_count"] + stats["put_scalar_count"]
             + stats["put_array_count"] + stats["put_nt_count"]
             + stats["get_nt_count"])

print(f"\n\n{'=' * 60}")
print(f"  Stress Test Results  ({elapsed:.1f}s, {N_WORKERS} workers)")
print(f"{'=' * 60}")
print(f"  Total operations   : {total_ops:>10,}")
print(f"  Throughput         : {total_ops / elapsed:>10,.0f} ops/s")
print()
print(f"  Scalar gets        : {stats['get_count']:>10,}")
print(f"  Scalar puts        : {stats['put_scalar_count']:>10,}")
print(f"  Array puts         : {stats['put_array_count']:>10,}")
print(f"  NT gets            : {stats['get_nt_count']:>10,}")
print(f"  NT puts            : {stats['put_nt_count']:>10,}")
print(f"  Errors             : {stats['errors']:>10,}")
print()

for label, key in [("GET latency", "get_latencies"), ("PUT latency", "put_latencies")]:
    lats = stats[key]
    if lats:
        lats_us = [x * 1e6 for x in lats]
        print(f"  {label}:")
        print(f"    mean   = {statistics.mean(lats_us):>8.1f} µs")
        print(f"    median = {statistics.median(lats_us):>8.1f} µs")
        print(f"    p95    = {sorted(lats_us)[int(len(lats_us) * 0.95)]:>8.1f} µs")
        print(f"    p99    = {sorted(lats_us)[int(len(lats_us) * 0.99)]:>8.1f} µs")
        print(f"    max    = {max(lats_us):>8.1f} µs")
        print()

print(f"{'=' * 60}")
