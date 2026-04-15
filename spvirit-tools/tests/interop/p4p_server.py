#!/usr/bin/env python3
"""p4p PVA server for interop testing.

Serves a variety of PV types to exercise interop surfaces between
spvirit Rust tools and a p4p (Python/C++) PVA server.

Default PV names (overridable via env vars in the test):

    p4p:rw            – writable NTScalar double
    p4p:ro            – read-only NTScalar double
    p4p:int           – writable NTScalar int32
    p4p:str           – writable NTScalar string
    p4p:float         – writable NTScalar float32
    p4p:long          – writable NTScalar int64
    p4p:arr:double    – writable NTScalar double array
    p4p:arr:int       – writable NTScalar int32 array
    p4p:arr:str       – writable NTScalar string array
    p4p:enum          – writable NTEnum

Usage (from the repo root):

    export PVA_TEST_P4P=1
    export P4P_PROVIDER_CMD="python3 spvirit-tools/tests/interop/p4p_server.py"
    cargo test --all
"""
import time

from p4p.nt import NTEnum, NTScalar
from p4p.server import Server
from p4p.server.thread import SharedPV


def writable(pv_ref):
    """Attach a generic put handler that accepts and posts any value."""

    @pv_ref.put
    def _handle(pv, op):
        pv.post(op.value())
        op.done()


# ── Scalar PVs ────────────────────────────────────────────────────────────────

pv_rw = SharedPV(nt=NTScalar("d"), initial=42.0)
writable(pv_rw)

pv_ro = SharedPV(nt=NTScalar("d"), initial=3.14)
# intentionally no put handler → read-only

pv_int = SharedPV(nt=NTScalar("i"), initial=7)
writable(pv_int)

pv_str = SharedPV(nt=NTScalar("s"), initial="hello")
writable(pv_str)

pv_float = SharedPV(nt=NTScalar("f"), initial=2.5)
writable(pv_float)

pv_long = SharedPV(nt=NTScalar("l"), initial=123456789)
writable(pv_long)

# ── Array PVs ─────────────────────────────────────────────────────────────────

pv_arr_double = SharedPV(nt=NTScalar("ad"), initial=[1.0, 2.0, 3.0])
writable(pv_arr_double)

pv_arr_int = SharedPV(nt=NTScalar("ai"), initial=[10, 20, 30])
writable(pv_arr_int)

pv_arr_str = SharedPV(nt=NTScalar("as"), initial=["alpha", "beta", "gamma"])
writable(pv_arr_str)

# ── Enum PV ───────────────────────────────────────────────────────────────────

pv_enum = SharedPV(nt=NTEnum(), initial={"choices": ["Off", "On", "Error"], "index": 1})
writable(pv_enum)

# ── Provider map ──────────────────────────────────────────────────────────────

providers = {
    "p4p:rw": pv_rw,
    "p4p:ro": pv_ro,
    "p4p:int": pv_int,
    "p4p:str": pv_str,
    "p4p:float": pv_float,
    "p4p:long": pv_long,
    "p4p:arr:double": pv_arr_double,
    "p4p:arr:int": pv_arr_int,
    "p4p:arr:str": pv_arr_str,
    "p4p:enum": pv_enum,
}

with Server(providers=[providers]):
    while True:
        time.sleep(1)
