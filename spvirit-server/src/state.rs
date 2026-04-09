use std::collections::HashMap;

use spvirit_codec::spvd_decode::StructureDesc;

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
}

#[derive(Debug, Clone, Copy)]
pub struct MonitorState {
    pub running: bool,
    pub pipeline_enabled: bool,
    pub nfree: u32,
}
