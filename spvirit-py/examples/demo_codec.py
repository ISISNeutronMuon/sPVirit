"""Standalone codec demo — encode/decode without any network IO.

Exercises the ``spvirit.codec`` submodule:
  - ``encode_pv_request`` / ``encode_put_payload``
  - ``decode_introspection`` / ``decode_value``
  - ``decode_packet``
  - ``format_value`` / ``extract_nt_value``

Run with a working ``spvirit`` extension on ``PYTHONPATH``::

    python3 demo_codec.py
"""
from __future__ import annotations

from spvirit import codec

# ─── 1. Encode a pvRequest describing which fields to fetch ──────────────────
# An empty request means "all fields"; otherwise pass a list of field paths.
print("── encode_pv_request ────────────────────────────────────────────────")
full = codec.encode_pv_request()
subset = codec.encode_pv_request(["value", "alarm.severity", "timeStamp"])
print(f"all-fields request  ({len(full)} B): {full.hex()}")
print(f"subset request      ({len(subset)} B): {subset.hex()}")

# ─── 2. Decode a captured packet ────────────────────────────────────────────
# A tiny control-message frame (SET_BYTE_ORDER) — 8-byte header only.
print("\n── decode_packet (SET_BYTE_ORDER control frame) ─────────────────────")
frame = bytes([0xCA, 0x02, 0x81, 0x00, 0x00, 0x00, 0x00, 0x00])
meta = codec.decode_packet(frame)
print(f"command      = {meta['command']} ({meta['command_name']})")
print(f"version      = {meta['version']}")
print(f"flags.raw    = {meta['flags']['raw']:#04x}")
print(f"is_control   = {meta['flags']['is_control']}")
print(f"payload len  = {meta['payload_length']}")

# ─── 3. Introspection round-trip ────────────────────────────────────────────
# A captured INIT response from a real server can be fed to
# ``decode_introspection`` to recover the StructureDesc tree without
# needing the live channel.  Here we show the StructureDesc API itself.
print("\n── StructureDesc API shape ─────────────────────────────────────────")
# Build a toy request so we have *some* bytes to introspect over.
req_bytes = codec.encode_pv_request(["value"])
# decode_introspection expects the full INIT pvStructure wire format — we
# just demonstrate the exposed Python attributes against a placeholder
# StructureDesc extracted from a live channel in the other demos.
print("StructureDesc attributes: struct_id, fields; methods: field(name),")
print("dump(), __len__, __contains__.  See demo_channel.py for a live one.")

# ─── 4. encode_put_payload (needs a real StructureDesc) ─────────────────────
# Round-tripped live in demo_channel.py; here we just show the signature.
print("\n── encode_put_payload ──────────────────────────────────────────────")
print("codec.encode_put_payload(structure_desc, value, is_be=False) -> bytes")
print("See demo_channel.py for a live call using Channel.introspect().")

# ─── 5. format_value / extract_nt_value ─────────────────────────────────────
# These operate on decoded Python dicts (from Channel.get or decode_value).
sample = {
    "value": 42.5,
    "alarm": {"severity": 0, "status": 0, "message": ""},
    "timeStamp": {"secondsPastEpoch": 1_700_000_000, "nanoseconds": 0, "userTag": 0},
    "display": {"units": "mA", "description": "sample current"},
}
print("\n── format_value / extract_nt_value ─────────────────────────────────")
print(f"format_value      -> {codec.format_value(sample)!r}")
print(f"extract_nt_value  -> {codec.extract_nt_value(sample)!r}")
