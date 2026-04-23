"""demo_source_aggregate.py — Computed/aggregate source.

Registers three raw temperature PVs in the built-in store and a Python
source that publishes *computed* PVs derived from them:

    AGG:MEAN   — arithmetic mean of RAW:T1/T2/T3
    AGG:MIN    — minimum
    AGG:MAX    — maximum

Because the Python source is registered at order 10 (the built-in store
is always at 0), the `RAW:*` names are resolved first by the built-in
store and the `AGG:*` names fall through to the Python source.

Run, then::

    spget RAW:T1 RAW:T2 RAW:T3 AGG:MEAN AGG:MIN AGG:MAX
"""

import math
import time

import spvirit


class AggregateSource:
    def __init__(self, store: spvirit.Store, inputs: list[str]) -> None:
        self._store = store
        self._inputs = inputs
        self._outputs = {"AGG:MEAN", "AGG:MIN", "AGG:MAX"}

    def claim(self, name: str):
        if name not in self._outputs:
            return None
        return spvirit.PvInfo.nt_scalar("double")  # read-only

    def _read_inputs(self) -> list[float]:
        out = []
        for n in self._inputs:
            v = self._store.get_value(n)
            if v is not None:
                out.append(float(v))
        return out

    def get(self, name: str):
        if name not in self._outputs:
            return None
        vals = self._read_inputs()
        if not vals:
            return spvirit.NtScalar(0.0)
        if name == "AGG:MEAN":
            v = sum(vals) / len(vals)
        elif name == "AGG:MIN":
            v = min(vals)
        else:  # AGG:MAX
            v = max(vals)
        return spvirit.NtScalar(v)

    def put(self, name, value):
        raise RuntimeError(f"'{name}' is read-only")

    def names(self):
        return list(self._outputs)


def main() -> None:
    builder = (
        spvirit.ServerBuilder()
        .ai("RAW:T1", 20.0)
        .ai("RAW:T2", 22.0)
        .ai("RAW:T3", 24.0)
    )
    server = builder.build()

    # Register the aggregate source *after* build so it can reference the store.
    store = server.store()
    server.add_source(
        "aggregates",
        10,
        AggregateSource(store, ["RAW:T1", "RAW:T2", "RAW:T3"]),
    )

    store_handle = server.start_background()

    print("Aggregate source server on port 5075.")
    print("  Raw PVs:       RAW:T1, RAW:T2, RAW:T3")
    print("  Aggregate PVs: AGG:MEAN, AGG:MIN, AGG:MAX  (computed on each GET)")

    tick = 0
    try:
        while True:
            phase = tick * 0.3
            store_handle.set_value("RAW:T1", 20.0 + 2.0 * math.sin(phase))
            store_handle.set_value("RAW:T2", 22.0 + 1.5 * math.sin(phase * 1.3))
            store_handle.set_value("RAW:T3", 24.0 + 3.0 * math.sin(phase * 0.7))
            tick += 1
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nbye.")


if __name__ == "__main__":
    main()
