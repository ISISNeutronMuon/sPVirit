use std::net::SocketAddr;
use std::time::Duration;

use spvirit_codec::spvd_decode::{DecodedValue, StructureDesc};

#[derive(Clone, Debug)]
pub struct PvGetOptions {
    pub pv_name: String,
    pub timeout: Duration,
    pub server_addr: Option<SocketAddr>,
    pub search_addr: Option<std::net::IpAddr>,
    pub bind_addr: Option<std::net::IpAddr>,
    pub name_servers: Vec<SocketAddr>,
    pub udp_port: u16,
    pub tcp_port: u16,
    pub debug: bool,
    pub no_broadcast: bool,
    pub authnz_user: Option<String>,
    pub authnz_host: Option<String>,
}

impl PvGetOptions {
    pub fn new(pv_name: String) -> Self {
        Self {
            pv_name,
            timeout: Duration::from_secs(5),
            server_addr: None,
            search_addr: None,
            bind_addr: None,
            name_servers: Vec::new(),
            udp_port: 5076,
            tcp_port: 5075,
            debug: false,
            no_broadcast: false,
            authnz_user: None,
            authnz_host: None,
        }
    }
}

#[derive(Debug)]
pub struct PvGetResult {
    pub pv_name: String,
    pub value: DecodedValue,
    pub raw_pva: Vec<u8>,
    pub raw_pvd: Vec<u8>,
    pub introspection: StructureDesc,
}

#[derive(Debug)]
pub enum PvGetError {
    Io(std::io::Error),
    Timeout(&'static str),
    Search(&'static str),
    Protocol(String),
    Decode(String),
}

impl std::fmt::Display for PvGetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PvGetError::Io(e) => write!(f, "io error: {}", e),
            PvGetError::Timeout(ctx) => write!(f, "timeout: {}", ctx),
            PvGetError::Search(ctx) => write!(f, "search error: {}", ctx),
            PvGetError::Protocol(ctx) => write!(f, "protocol error: {}", ctx),
            PvGetError::Decode(ctx) => write!(f, "decode error: {}", ctx),
        }
    }
}

impl std::error::Error for PvGetError {}

impl From<std::io::Error> for PvGetError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
