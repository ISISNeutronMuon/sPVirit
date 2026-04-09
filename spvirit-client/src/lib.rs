//! PVAccess client library — search, connect, get, put, monitor.
//!
//! This crate provides both a **high-level API** ([`PvaClient`]) and the
//! lower-level protocol functions used to build it.
//!
//! # High-level API
//!
//! ```rust,ignore
//! use spvirit_client::PvaClient;
//! use std::ops::ControlFlow;
//!
//! let client = PvaClient::builder().build();
//!
//! // GET
//! let result = client.pvget("MY:PV").await?;
//!
//! // PUT
//! client.pvput("MY:PV", 42.0).await?;
//!
//! // MONITOR
//! client.pvmonitor("MY:PV", |val| {
//!     println!("{val:?}");
//!     ControlFlow::Continue(())
//! }).await?;
//!
//! // INFO (introspection)
//! let desc = client.pvinfo("MY:PV").await?;
//! ```
//!
//! # Key types
//!
//! - [`PvaClient`] — high-level client with `pvget`, `pvput`, `pvmonitor`, `pvinfo`
//! - [`PvOptions`] — configuration for PV operations (ports, timeout, auth)
//! - [`PvGetResult`] — decoded GET result with introspection
//! - [`ChannelConn`] — low-level established TCP channel
//! - [`SearchTarget`] — UDP/TCP search target address

pub mod auth;
pub mod client;
pub mod format;
pub mod pva_client;
pub mod put_encode;
pub mod pvlist;
pub mod search;
pub mod transport;
pub mod types;

// --- Re-exports: high-level API ---
pub use pva_client::{PvaClient, PvaClientBuilder, PvaChannel, pvput, pvmonitor, pvinfo, client_from_opts};
pub use pvlist::PvListSource;

// --- Re-exports: client core ---
pub use client::{build_client_validation, establish_channel, pvget, ChannelConn};
pub use search::{build_auto_broadcast_targets, search_pv, resolve_pv_server, SearchTarget};
pub use transport::read_packet;
pub use types::{PvGetError, PvGetOptions, PvGetResult, PvMonitorEvent, PvOptions};
pub use format::{format_output, OutputFormat, RenderOptions};
