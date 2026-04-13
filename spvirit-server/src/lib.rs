//! PVAccess server library for EPICS.
//!
//! Provides reusable server-side types, .db parsing, connection state, and the
//! [`PvStore`] trait that abstracts over a PV data source.  Consumers implement
//! `PvStore` and pass it to the protocol handler to serve PVs over PVAccess.
//!
//! # High-level API
//!
//! ```rust,ignore
//! use spvirit_server::PvaServer;
//!
//! let server = PvaServer::builder()
//!     .ai("SIM:TEMP", 22.5)
//!     .ao("SIM:SETPOINT", 25.0)
//!     .bo("SIM:ENABLE", false)
//!     .build();
//!
//! server.run().await?;
//! ```

pub mod db;
pub mod state;
pub mod types;
pub mod pvstore;
pub mod decode;
pub mod convert;
pub mod apply;
pub mod beacon;
pub mod monitor;
pub mod handler;
pub mod server;
pub mod simple_store;
pub mod pva_server;
pub mod group;

// Convenience re-exports.
pub use pvstore::PvStore;
pub use handler::PvListMode;
pub use server::{PvaServerConfig, PvaServerState, run_pva_server, run_pva_server_with_registry};
pub use types::{RecordType, RecordData, RecordInstance, DbCommonState, ScanMode, LinkExpr, OutputMode};
pub use simple_store::SimplePvStore;
pub use pva_server::{PvaServer, PvaServerBuilder};
pub use group::{GroupPvDef, GroupMember, GroupPvStore, FieldMapping, TriggerDef, parse_group_config, parse_info_group, merge_group_defs};
