"""Example usage of the spvirit Python bindings."""

import spvirit

# ─── Discover servers ────────────────────────────────────────────────────────

servers = spvirit.py_discover_servers(timeout =10)
client = spvirit.Client()
for srv in servers:
    print(f"Found server at {srv.tcp_addr}")
    pvs = client.pvlist(srv.tcp_addr)
    print(f"  PVs: {pvs[:10]}")
