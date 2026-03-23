//! PVAccess client/server tools for EPICS.
//!
//! This crate provides a PVA client library (`pva_client`) and server
//! library (`pva_server`), along with command-line tools: `pvget`,
//! `pvput`, `pvmonitor`, `pvlist`, `pvinfo`, `pvexplore`, `pvsine`,
//! `pva_server`, and `pva_get_compare`.
//!
//! Commonly used types are re-exported at the crate root for convenience.
//! The full module paths remain available for less common items.

pub mod pva_client;
pub mod pva_server;

// --- Re-exports: client core ---
pub use pva_client::client::{build_client_validation, pvget};
pub use pva_client::search::{search_pv, SearchTarget};
pub use pva_client::transport::read_packet;
pub use pva_client::types::{PvGetError, PvGetOptions, PvGetResult};
pub use pva_client::format::{format_output, OutputFormat, RenderOptions};
