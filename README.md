# Spvirit

[![crates.io (spvirit-types)](https://img.shields.io/crates/v/spvirit-types?label=spvirit-types)](https://crates.io/crates/spvirit-types)
[![crates.io (spvirit-codec)](https://img.shields.io/crates/v/spvirit-codec?label=spvirit-codec)](https://crates.io/crates/spvirit-codec)
[![crates.io (spvirit-tools)](https://img.shields.io/crates/v/spvirit-tools?label=spvirit-tools)](https://crates.io/crates/spvirit-tools)
[![License](https://img.shields.io/crates/l/spvirit-types)](LICENSE)

*/ˈspɪrɪt/ of the Machine* 

Spvirit is a Rust library for working with EPICS PVAccess protocol, including encoding/decoding and connection state tracking. It also includes tools for monitoring and testing PVAccess connections. These are more proof of concept than production ready, but they are available for anyone to use and contribute to.

## Why Rust? 

Because why not, admittedly I just wanted to learn Rust and this seemed like a fun project with a moderately useful outcome.

## Project Structure
The project is structured as a Cargo workspace with three crates:
- `spvirit-types`: Contains shared data model types for PVAccess Normative Types (NT).
- `spvirit-codec`: Contains the PVAccess protocol encoding/decoding logic and connection state tracking.
- `spvirit-tools`: Contains command-line tools for monitoring and testing PVAccess connections.

In the future I would like to split out the tools to client and server tools and add some more IOC-like functionality to the server tools, but for now they are all in one crate for simplicity.

## Getting Started

### Install Rust 
``` bash
# Linux
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```
### clone the repo
``` bash
git clone https://github.com/ISISNeutronMuon/spvirit
```
### Build the project
``` bash
cd spvirit
cargo build --release
```

### Run the tools
``` bash
cargo run --bin spvirit_monitor my:pv:name
# or
./target/release/spvirit_monitor my:pv:name
# or if installed 
cargo install spvirit-tools

spvirit_monitor my:pv:name
```
### Using the library in your own Rust project

Add the crates you need to your `Cargo.toml`:
```toml
[dependencies]
spvirit-tools = "0.1"    # client/server library + CLI tools
spvirit-codec = "0.1"    # low-level PVA protocol encode/decode
spvirit-types = "0.1"    # shared Normative Type data model
```

#### Fetching a PV value (client)
```rust
use spvirit_tools::{format_output, pvget, PvGetOptions, RenderOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pv_name = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "MY:PV:NAME".into());

    let opts = PvGetOptions::new(pv_name);
    let result = pvget(&opts).await?;

    let render = RenderOptions::default();
    println!("{}", format_output(&result.pv_name, &result.value, &render));
    Ok(())
}
```

#### Searching for a PV server
```rust
use std::time::Duration;
use spvirit_tools::{build_auto_broadcast_targets, search_pv};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pv_name = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "MY:PV:NAME".into());

    let targets = build_auto_broadcast_targets();

    let addr = search_pv(&pv_name, 5076, Duration::from_secs(5), &targets, false).await?;
    println!("Found server at {addr}");
    Ok(())
}
```

#### Decoding a raw PVA packet (codec)
```rust
use spvirit_codec::{PvaPacket, PvaPacketCommand};

fn main() {
    let raw: &[u8] = &[
        0xCA, 0x02, 0x00, 0x01, // header: magic, version, flags (LE), command 1
        0x09, 0x00, 0x00, 0x00, // payload length = 9
        0x00, 0x40, 0x00, 0x00, // buffer_size = 16384
        0xFF, 0x7F,             // introspection_registry_size = 32767
        0x00, 0x00,             // qos = 0
        0x00,                   // authz = "" (empty string)
    ];

    let mut packet = PvaPacket::new(raw);

    println!("command: {}", packet.header.command);
    println!("endian:  {}", if packet.header.flags.is_msb { "big" } else { "little" });

    if let Some(cmd) = packet.decode_payload() {
        match cmd {
            PvaPacketCommand::ConnectionValidation(cv) => {
                println!("buffer_size: {}", cv.buffer_size);
            }
            other => println!("{other:?}"),
        }
    }
}
```

See the [`examples/`](spvirit-tools/examples) folders for runnable versions of each snippet:
```bash
cargo run --example pvget_example -p spvirit-tools       # requires a running IOC
cargo run --example search_example -p spvirit-tools      # requires a running IOC
cargo run --example decode_packet -p spvirit-codec       # self-contained, no IOC needed
```

### Tools available

| spvirit tool | EPICS Base equivalent | Description |
|---|---|---|
| `spvirit_get` | `pvget` | Fetch the current value of a PV |
| `spvirit_put` | `pvput` | Write a value to a PV |
| `spvirit_monitor` | `pvmonitor` | Subscribe to a PV and print value changes |
| `spvirit_info` | `pvinfo` | Display field/metadata information for a PV |
| `spvirit_list` | `pvlist` | List all available PVs on discovered servers |
| `spvirit_server` | `softIoc` | Not fully one-to-one - just a demo, it does parse some db file vocab |
| `spvirit_explore` |  | Interactive TUI to browse servers, select PVs, and monitor values |
| `spvirit_search` |  | TUI showing PV search network traffic for diagnostics |
| `spvirit_sine` |  | Continuously write a sine wave to a PV (demo/testing) |
| `spvirit_dodeca` |  | Server publishing a rotating 3D dodecahedron as an NTNDArray PV |

## Related Projects

- [spvirit-scry](https://crates.io/crates/spvirit-scry) — A Rust tool for capturing and analyzing pvAccess EPICS packets.

## References 

I used the following libraries and repos as refernce materials for PVAccess protocol: 

- [pvxs](https://epics-base.github.io/pvxs/)
- [pvAccess Protocol Specification](https://docs.epics-controls.org/en/latest/pv-access/protocol.html)
- [EPICS Base](https://github.com/epics-base/epics-base)
- [PVAshark](https://github.com/george-mcintyre/pvashark)
