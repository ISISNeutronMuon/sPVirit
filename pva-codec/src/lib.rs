//! PVAccess protocol encode/decode and connection state tracking.
//!
//! This crate provides the low-level PVA wire-format codec (encode + decode),
//! PVD (pvData) structure codec, and PVA connection state tracking.
//!
//! Commonly used types are re-exported at the crate root for convenience.
//! The full module paths remain available for less common items.

pub mod encode_common;
pub mod epics_decode;
pub mod pva_encode;
pub mod pva_state;
pub mod pvd_decode;
pub mod pvd_encode;

// --- Re-exports: PVA wire-format decode types ---
pub use epics_decode::{
    decode_string, PvaCommands, PvaHeader, PvaPacket, PvaPacketCommand, PvaStatus,
};

// --- Re-exports: PVA wire-format encode helpers ---
pub use pva_encode::{encode_control_message, encode_header, format_pva_address, ip_from_bytes, ip_to_bytes};

// --- Re-exports: connection state tracking ---
pub use pva_state::{ConnectionKey, PvaStateConfig, PvaStateStats, PvaStateTracker};

// --- Re-exports: pvData structure decode ---
pub use pvd_decode::{DecodedValue, FieldDesc, FieldType, PvdDecoder, StructureDesc, TypeCode};

// --- Re-exports: pvData structure encode ---
pub use pvd_encode::encode_structure_desc;

// --- Re-export the types crate for convenience ---
pub use pva_types;
