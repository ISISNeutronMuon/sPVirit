"""Demo PVAccess server using spvirit Python bindings.

Serves one of every record type on port 5075.  Run this script and
then use any PVA client (pvget, pvmonitor, or the spvirit Client) to
read the values:

    pvget SIM:TEMPERATURE
    pvmonitor SIM:SINE
"""

import math
import time
import spvirit

# ─── Build the server ─────────────────────────────────────────────────────────

builder = spvirit.ServerBuilder()

# ── Analog records (ai / ao) ──────────────────────────────────────────────────

builder.ai("SIM:TEMPERATURE", 22.5)
builder.ai("SIM:SINE", 0.0)
builder.ao("SIM:SETPOINT", 25.0)

# ── Binary records (bi / bo) — 2-state enum via znam/onam ────────────────────

builder.bi("SIM:INTERLOCK", False)      # Off / On
builder.bo("SIM:ENABLE", True)          # Off / On

# ── String records (stringin / stringout) ─────────────────────────────────────

builder.string_in("SIM:STATUS", "IDLE")
builder.string_out("SIM:COMMAND", "")

# ── Waveform (float64 array, read-only style) ────────────────────────────────

builder.waveform("SIM:WAVEFORM", [0.0] * 64)

# ── Aai — analog array input (read-only array of int32) ──────────────────────

builder.aai("SIM:HISTOGRAM", [0] * 32)

# ── Aao — analog array output (writable array of float64) ────────────────────

builder.aao("SIM:SETPOINTS_ARRAY", [1.0, 2.0, 3.0, 4.0, 5.0])

# ── SubArray — view into a larger array ───────────────────────────────────────

builder.sub_array("SIM:SUBARRAY", list(range(100)), indx=10, nelm=20)

# ── NtTable — tabular data ───────────────────────────────────────────────────

builder.nt_table("SIM:TABLE", {
    "name":  ["Alice", "Bob", "Charlie"],
    "score": [95.0, 87.5, 92.0],
    "rank":  [1, 2, 3],
})

# ── NtNdArray — 2-D image-like data ──────────────────────────────────────────

width, height = 16, 16
pixels = [float(i % 256) for i in range(width * height)]
builder.nt_ndarray("SIM:IMAGE", pixels, [(width, width), (height, height)])

# ── Enum equivalent — NtScalar with display_form_choices ──────────────────────
# PVA "enum" is modelled as an integer NtScalar whose display.form.choices
# lists the allowed state names.  Clients (e.g. PyDM, CS-Studio) render
# the value as a drop-down or label from the choices list.

builder.ai("SIM:ENUM_RO", 0)    # read-only enum state
builder.ao("SIM:ENUM_RW", 0)    # writable enum state

# ── NT-level PVs — full metadata set via put_nt after build ───────────────────
# These records are created with a placeholder value; after .build() we
# overwrite them with rich NtScalar / NtScalarArray payloads that carry
# units, display limits, control limits, precision, and description.

builder.ao("SIM:PRESSURE", 0.0)         # will get full NtScalar metadata
builder.waveform("SIM:SPECTRUM", [0.0] * 128)  # will get NtScalarArray metadata

# ─── Callbacks ─────────────────────────────────────────────────────────────────

def on_setpoint(pv_name, value):
    print(f"[on_put] {pv_name} = {value}")

builder.on_put("SIM:SETPOINT", on_setpoint)

def on_command(pv_name, value):
    print(f"[command] {pv_name} = {value!r}")

builder.on_put("SIM:COMMAND", on_command)

def on_enable(pv_name, value):
    print(f"[enable] {pv_name} = {value}")

builder.on_put("SIM:ENABLE", on_enable)

def on_enum_rw(pv_name, value):
    states = ["Idle", "Running", "Paused", "Error"]
    idx = int(value) if isinstance(value, (int, float)) else 0
    label = states[idx] if 0 <= idx < len(states) else "?"
    print(f"[enum] {pv_name} = {idx} ({label})")

builder.on_put("SIM:ENUM_RW", on_enum_rw)

# ─── Periodic scans ───────────────────────────────────────────────────────────

t0 = time.time()

def sine_scan(pv_name):
    """1 Hz sine wave centred on 22.5 with amplitude 5."""
    elapsed = time.time() - t0
    return 22.5 + 5.0 * math.sin(elapsed)

builder.scan("SIM:SINE", 0.5, sine_scan)

def temperature_scan(pv_name):
    """Slow random-walk temperature."""
    import random
    return 22.5 + random.uniform(-1.0, 1.0)

builder.scan("SIM:TEMPERATURE", 2.0, temperature_scan)

# ─── Network configuration ────────────────────────────────────────────────────

builder.port(5075)
builder.udp_port(5076)
builder.beacon_period(15)
builder.compute_alarms(True)

# ─── Start ─────────────────────────────────────────────────────────────────────

server = builder.build()
store = server.start_background()

print("Server running on port 5075 — press Ctrl+C to stop")
print(f"Serving {len(store.pv_names())} PVs: {store.pv_names()}")

# ─── Set enum-style metadata via put_nt ───────────────────────────────────────
# After the server starts we enrich two records with enum choices so that
# PVA introspection returns them inside display.form.choices.

ENUM_CHOICES = ["Idle", "Running", "Paused", "Error"]

for pv in ("SIM:ENUM_RO", "SIM:ENUM_RW"):
    nt = spvirit.NtScalar(0.0, display_description="State selector")
    # NOTE: display_form_choices is set via put_nt – clients see the
    # enum_t structure {index, choices} automatically.
    store.put_nt(pv, nt)

# ── Enrich NT-level PVs with full metadata ────────────────────────────────────

# Pressure sensor — NtScalar with units, display/control range, precision
pressure_nt = spvirit.NtScalar(
    101.325,
    units="kPa",
    display_low=80.0,
    display_high=120.0,
    display_description="Vacuum chamber pressure",
    display_precision=3,
    control_low=90.0,
    control_high=110.0,
    control_min_step=0.001,
)
store.put_nt("SIM:PRESSURE", pressure_nt)

# Spectrum — NtScalarArray with display metadata
spectrum_data = [math.sin(2 * math.pi * i / 128) * 100.0 for i in range(128)]
spectrum_nt = spvirit.NtScalarArray(spectrum_data)
store.put_nt("SIM:SPECTRUM", spectrum_nt)

# ─── Periodic updates from the main thread ────────────────────────────────────

try:
    tick = 0
    while True:
        elapsed = time.time() - t0

        # Waveform — shifting sine
        wf = [math.sin(2 * math.pi * i / 64 + elapsed) for i in range(64)]
        store.set_array_value("SIM:WAVEFORM", wf)

        # Histogram — random counts
        import random
        hist = [random.randint(0, 100) for _ in range(32)]
        store.set_array_value("SIM:HISTOGRAM", hist)

        # Cycle the read-only enum through states every 2 seconds
        enum_idx = (tick // 4) % len(ENUM_CHOICES)
        store.set_value("SIM:ENUM_RO", enum_idx)

        # Pressure — slow drift around 101.325 kPa with mutating alarm & description
        pressure = 101.325 + 2.0 * math.sin(elapsed * 0.3) + random.gauss(0, 0.05)

        # Alarm cycles through: OK → MINOR (high) → MAJOR (very high) → OK
        alarm_cycle = tick % 20
        if alarm_cycle < 10:
            sev, status, msg = 0, 0, ""
            desc = f"Vacuum chamber pressure — stable (t={elapsed:.1f}s)"
        elif alarm_cycle < 15:
            sev, status, msg = 1, 3, f"Pressure drifting high: {pressure:.3f} kPa"
            desc = f"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum"
            pressure += 5.0  # push it higher to match the alarm
        else:
            sev, status, msg = 2, 3, f"Pressure CRITICAL: {pressure + 10:.3f} kPa"
            desc = f"CRITICAL: chamber over-pressure! (t={elapsed:.1f}s)"
            pressure += 10.0

        p_nt = spvirit.NtScalar(
            pressure,
            units="kPa",
            display_low=80.0,
            display_high=120.0,
            display_description=desc,
            display_precision=3,
            control_low=90.0,
            control_high=110.0,
            control_min_step=0.001,
            alarm_severity=sev,
            alarm_status=status,
            alarm_message=msg,
        )
        store.put_nt("SIM:PRESSURE", p_nt)

        # Spectrum — shifting FFT-like waveform (full NT write)
        spec = [abs(math.sin(2 * math.pi * i / 128 + elapsed * 0.5)) * 100.0
                for i in range(128)]
        store.put_nt("SIM:SPECTRUM", spvirit.NtScalarArray(spec))

        tick += 1
        time.sleep(0.5)
except KeyboardInterrupt:
    print("\nShutting down.")


