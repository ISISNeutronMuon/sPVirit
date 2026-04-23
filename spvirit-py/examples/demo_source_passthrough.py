"""demo_source_passthrough.py — Decorator / middleware source.

Wraps another Python source and layers cross-cutting concerns on top:

  * logs every operation
  * enforces an allow-list for PUT
  * tracks a global access counter exposed as CTRL:ACCESS_COUNT

This is the decorator pattern applied to PV providers — useful for
authentication, auditing, rate-limiting, or value clamping in front
of any other source.
"""

import threading
import time

import spvirit


class SimpleStore:
    """Inner source: keeps a dict of float-valued PVs under CTRL:*."""

    PREFIX = "CTRL:"
    DEFAULTS = {"CTRL:SETPOINT": 0.0, "CTRL:READBACK": 0.0}

    def __init__(self):
        self._values = dict(self.DEFAULTS)
        self._lock = threading.Lock()

    def claim(self, name):
        if name in self._values:
            return spvirit.PvInfo.nt_scalar("double", writable=True)
        return None

    def get(self, name):
        with self._lock:
            if name in self._values:
                return spvirit.NtScalar(self._values[name])
        return None

    def put(self, name, value):
        v = _as_float(value)
        with self._lock:
            self._values[name] = v
        return spvirit.NtScalar(v)

    def names(self):
        return list(self._values.keys())


class AccessControl:
    """Decorator that wraps an inner source."""

    VIRTUAL = "CTRL:ACCESS_COUNT"

    def __init__(self, inner, put_allow_list: list[str]):
        self._inner = inner
        self._allow = set(put_allow_list)
        self._count = 0
        self._lock = threading.Lock()

    def _bump(self):
        with self._lock:
            self._count += 1

    def claim(self, name):
        if name == self.VIRTUAL:
            return spvirit.PvInfo.nt_scalar("double")  # read-only
        return self._inner.claim(name)

    def get(self, name):
        self._bump()
        print(f"[get] {name}")
        if name == self.VIRTUAL:
            with self._lock:
                return spvirit.NtScalar(float(self._count))
        return self._inner.get(name)

    def put(self, name, value):
        self._bump()
        print(f"[put] {name}={value!r}")
        if name not in self._allow:
            raise RuntimeError(f"PUT not allowed on '{name}'")
        return self._inner.put(name, value)

    def names(self):
        base = list(self._inner.names())
        base.append(self.VIRTUAL)
        return base


def _as_float(v):
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, dict):
        for key in ("value", "val"):
            if key in v:
                return _as_float(v[key])
    raise TypeError(f"cannot coerce {v!r} to float")


def main() -> None:
    inner = SimpleStore()
    decorated = AccessControl(inner, put_allow_list=["CTRL:SETPOINT"])

    server = (
        spvirit.ServerBuilder()
        .add_source("ctrl", 10, decorated)
        .build()
    )
    server.start_background()

    print("Passthrough (decorator) source server on port 5075.")
    print("  PUT allowed:  CTRL:SETPOINT")
    print("  PUT denied:   CTRL:READBACK")
    print("  Virtual PV:   CTRL:ACCESS_COUNT (read-only)")

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\nbye.")


if __name__ == "__main__":
    main()
