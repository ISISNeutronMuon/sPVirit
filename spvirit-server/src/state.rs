use std::collections::HashMap;

use spvirit_codec::spvd_decode::StructureDesc;
use spvirit_types::NtPayload;

#[derive(Debug, Default)]
pub struct ConnState {
    pub cid_to_sid: HashMap<u32, u32>,
    pub sid_to_pv: HashMap<u32, String>,
    pub ioid_to_desc: HashMap<u32, StructureDesc>,
    pub ioid_to_pv: HashMap<u32, String>,
    pub ioid_to_monitor: HashMap<u32, MonitorState>,
}

#[derive(Debug, Clone)]
pub struct MonitorSub {
    pub conn_id: u64,
    pub ioid: u32,
    pub version: u8,
    pub is_be: bool,
    pub running: bool,
    pub pipeline_enabled: bool,
    pub nfree: u32,
    /// When set, only encode these fields in monitor data responses.
    pub filtered_desc: Option<StructureDesc>,
    /// Last payload sent to this subscriber. Used to produce sparse deltas on
    /// subsequent updates (see `spvirit-server/src/monitor.rs`). `None` means
    /// the next update is the initial full snapshot.
    pub last_snapshot: Option<NtPayload>,
}

#[derive(Debug, Clone, Copy)]
pub struct MonitorState {
    pub running: bool,
    pub pipeline_enabled: bool,
    pub nfree: u32,
}
