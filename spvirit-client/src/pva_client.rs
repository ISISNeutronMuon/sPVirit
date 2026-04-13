//! High-level PVAccess client — one-liner get, put, monitor, info.
//!
//! # Example
//!
//! ```rust,ignore
//! use spvirit_client::PvaClient;
//!
//! let client = PvaClient::builder().build();
//! let result = client.pvget("MY:PV").await?;
//! client.pvput("MY:PV", 42.0).await?;
//! ```

use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::task::JoinHandle;
use tokio::time::{Instant, interval};

use spvirit_codec::epics_decode::{PvaPacket, PvaPacketCommand};
use spvirit_codec::spvd_decode::{DecodedValue, PvdDecoder, StructureDesc};
use spvirit_codec::spvd_encode::encode_pv_request;
use spvirit_codec::spvirit_encode::{
    encode_control_message, encode_get_field_request, encode_monitor_request, encode_put_request,
};

use crate::client::{ChannelConn, ensure_status_ok, establish_channel, pvget as low_level_pvget};
use crate::put_encode::encode_put_payload;
use crate::search::resolve_pv_server;
use crate::transport::{read_packet, read_until};
use crate::types::{PvGetError, PvGetResult, PvOptions};

/// PVA protocol version used in headers.
const PVA_VERSION: u8 = 2;
/// QoS / subcommand flag: INIT.
const QOS_INIT: u8 = 0x08;

static NEXT_IOID: AtomicU32 = AtomicU32::new(1);
fn alloc_ioid() -> u32 {
    NEXT_IOID.fetch_add(1, Ordering::Relaxed)
}

// ─── PvaClientBuilder ────────────────────────────────────────────────────────

/// Builder for [`PvaClient`].
///
/// ```rust,ignore
/// let client = PvaClient::builder()
///     .timeout(Duration::from_secs(10))
///     .port(5075)
///     .build();
/// ```
pub struct PvaClientBuilder {
    udp_port: u16,
    tcp_port: u16,
    timeout: Duration,
    no_broadcast: bool,
    name_servers: Vec<SocketAddr>,
    authnz_user: Option<String>,
    authnz_host: Option<String>,
    server_addr: Option<SocketAddr>,
    search_addr: Option<std::net::IpAddr>,
    bind_addr: Option<std::net::IpAddr>,
    debug: bool,
}

impl PvaClientBuilder {
    fn new() -> Self {
        Self {
            udp_port: 5076,
            tcp_port: 5075,
            timeout: Duration::from_secs(5),
            no_broadcast: false,
            name_servers: Vec::new(),
            authnz_user: None,
            authnz_host: None,
            server_addr: None,
            search_addr: None,
            bind_addr: None,
            debug: false,
        }
    }

    /// Set the TCP port (default 5075).
    pub fn port(mut self, port: u16) -> Self {
        self.tcp_port = port;
        self
    }

    /// Set the UDP search port (default 5076).
    pub fn udp_port(mut self, port: u16) -> Self {
        self.udp_port = port;
        self
    }

    /// Set the operation timeout (default 5 s).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Disable UDP broadcast search (use name servers only).
    pub fn no_broadcast(mut self) -> Self {
        self.no_broadcast = true;
        self
    }

    /// Add a PVA name-server address for TCP search.
    pub fn name_server(mut self, addr: SocketAddr) -> Self {
        self.name_servers.push(addr);
        self
    }

    /// Override the authentication user.
    pub fn authnz_user(mut self, user: impl Into<String>) -> Self {
        self.authnz_user = Some(user.into());
        self
    }

    /// Override the authentication host.
    pub fn authnz_host(mut self, host: impl Into<String>) -> Self {
        self.authnz_host = Some(host.into());
        self
    }

    /// Set an explicit server address, bypassing UDP search.
    pub fn server_addr(mut self, addr: SocketAddr) -> Self {
        self.server_addr = Some(addr);
        self
    }

    /// Set the search target IP address.
    pub fn search_addr(mut self, addr: std::net::IpAddr) -> Self {
        self.search_addr = Some(addr);
        self
    }

    /// Set the local bind IP for UDP search.
    pub fn bind_addr(mut self, addr: std::net::IpAddr) -> Self {
        self.bind_addr = Some(addr);
        self
    }

    /// Enable debug logging.
    pub fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

    /// Build the [`PvaClient`].
    pub fn build(self) -> PvaClient {
        PvaClient {
            udp_port: self.udp_port,
            tcp_port: self.tcp_port,
            timeout: self.timeout,
            no_broadcast: self.no_broadcast,
            name_servers: self.name_servers,
            authnz_user: self.authnz_user,
            authnz_host: self.authnz_host,
            server_addr: self.server_addr,
            search_addr: self.search_addr,
            bind_addr: self.bind_addr,
            debug: self.debug,
        }
    }
}

// ─── PvaClient ───────────────────────────────────────────────────────────────

/// High-level PVAccess client.
///
/// Provides `pvget`, `pvput`, `pvmonitor`, and `pvinfo` methods that hide
/// the underlying protocol handshake.
///
/// ```rust,ignore
/// let client = PvaClient::builder().build();
/// let val = client.pvget("MY:PV").await?;
/// ```
#[derive(Clone, Debug)]
pub struct PvaClient {
    udp_port: u16,
    tcp_port: u16,
    timeout: Duration,
    no_broadcast: bool,
    name_servers: Vec<SocketAddr>,
    authnz_user: Option<String>,
    authnz_host: Option<String>,
    server_addr: Option<SocketAddr>,
    search_addr: Option<std::net::IpAddr>,
    bind_addr: Option<std::net::IpAddr>,
    debug: bool,
}

impl PvaClient {
    /// Create a builder for configuring a [`PvaClient`].
    pub fn builder() -> PvaClientBuilder {
        PvaClientBuilder::new()
    }

    /// Build [`PvOptions`] for a given PV name, inheriting client-level settings.
    fn opts(&self, pv_name: &str) -> PvOptions {
        let mut o = PvOptions::new(pv_name.to_string());
        o.udp_port = self.udp_port;
        o.tcp_port = self.tcp_port;
        o.timeout = self.timeout;
        o.no_broadcast = self.no_broadcast;
        o.name_servers.clone_from(&self.name_servers);
        o.authnz_user.clone_from(&self.authnz_user);
        o.authnz_host.clone_from(&self.authnz_host);
        o.server_addr = self.server_addr;
        o.search_addr = self.search_addr;
        o.bind_addr = self.bind_addr;
        o.debug = self.debug;
        o
    }

    /// Resolve a PV server and establish a channel, returning the raw connection.
    async fn open_channel(&self, pv_name: &str) -> Result<ChannelConn, PvGetError> {
        let opts = self.opts(pv_name);
        let target = resolve_pv_server(&opts).await?;
        establish_channel(target, &opts).await
    }

    // ─── pvget ───────────────────────────────────────────────────────────

    /// Fetch the current value of a PV.
    pub async fn pvget(&self, pv_name: &str) -> Result<PvGetResult, PvGetError> {
        let opts = self.opts(pv_name);
        low_level_pvget(&opts).await
    }

    // ─── pvput ───────────────────────────────────────────────────────────

    /// Write a value to a PV.
    ///
    /// Accepts anything convertible to `serde_json::Value`:
    /// ```rust,ignore
    /// client.pvput("MY:PV", 42.0).await?;
    /// client.pvput("MY:PV", "hello").await?;
    /// client.pvput("MY:PV", serde_json::json!({"value": 1.5})).await?;
    /// ```
    pub async fn pvput(&self, pv_name: &str, value: impl Into<Value>) -> Result<(), PvGetError> {
        let json_val = value.into();
        let ChannelConn {
            mut stream,
            sid,
            version: _,
            is_be,
        } = self.open_channel(pv_name).await?;

        let ioid = alloc_ioid();

        // PUT INIT — send pvRequest for "field(value)"
        let pv_request = encode_pv_request(&["value"], is_be);
        let init = encode_put_request(sid, ioid, QOS_INIT, &pv_request, PVA_VERSION, is_be);
        stream.write_all(&init).await?;

        // Read INIT response — extract introspection
        let init_bytes = read_until(&mut stream, self.timeout, |cmd| {
            matches!(cmd, PvaPacketCommand::Op(op) if op.command == 11 && (op.subcmd & 0x08) != 0)
        })
        .await?;

        let desc = decode_init_introspection(&init_bytes, "PUT")?;

        // Encode and send the value
        let payload = encode_put_payload(&desc, &json_val, is_be)
            .map_err(|e| PvGetError::Protocol(format!("put encode: {e}")))?;
        let req = encode_put_request(sid, ioid, 0x00, &payload, PVA_VERSION, is_be);
        stream.write_all(&req).await?;

        // Read PUT response — verify status
        let resp_bytes = read_until(
            &mut stream,
            self.timeout,
            |cmd| matches!(cmd, PvaPacketCommand::Op(op) if op.command == 11 && op.subcmd == 0x00),
        )
        .await?;
        ensure_status_ok(&resp_bytes, is_be, "PUT")?;

        Ok(())
    }

    // ─── open_put_channel ────────────────────────────────────────────────

    /// Open a persistent channel for high-rate PUT streaming.
    ///
    /// Resolves the PV, establishes a channel, and completes the PUT INIT
    /// handshake. The returned [`PvaChannel`] is ready for immediate
    /// [`put`](PvaChannel::put) calls.
    pub async fn open_put_channel(&self, pv_name: &str) -> Result<PvaChannel, PvGetError> {
        let ChannelConn {
            mut stream,
            sid,
            version,
            is_be,
        } = self.open_channel(pv_name).await?;

        let ioid = alloc_ioid();

        // PUT INIT
        let pv_request = encode_pv_request(&["value"], is_be);
        let init = encode_put_request(sid, ioid, QOS_INIT, &pv_request, PVA_VERSION, is_be);
        stream.write_all(&init).await?;

        let init_bytes = read_until(&mut stream, self.timeout, |cmd| {
            matches!(cmd, PvaPacketCommand::Op(op) if op.command == 11 && (op.subcmd & 0x08) != 0)
        })
        .await?;

        let desc = decode_init_introspection(&init_bytes, "PUT")?;

        // Split stream; background reader logs PUT errors
        let (mut reader, writer) = stream.into_split();
        let reader_is_be = is_be;
        let reader_handle = tokio::spawn(async move {
            loop {
                let mut header = [0u8; 8];
                if reader.read_exact(&mut header).await.is_err() {
                    break;
                }
                let hdr = spvirit_codec::epics_decode::PvaHeader::new(&header);
                let len = if hdr.flags.is_control {
                    0usize
                } else {
                    hdr.payload_length as usize
                };
                let mut payload = vec![0u8; len];
                if len > 0 && reader.read_exact(&mut payload).await.is_err() {
                    break;
                }
                if hdr.command == 11
                    && !hdr.flags.is_control
                    && len >= 5
                    && let Some(st) =
                        spvirit_codec::epics_decode::decode_status(&payload[5..], reader_is_be).0
                    && st.code != 0
                {
                    let msg = st.message.unwrap_or_else(|| format!("code={}", st.code));
                    eprintln!("PvaChannel put error: {msg}");
                }
            }
        });

        Ok(PvaChannel {
            writer,
            sid,
            ioid,
            version,
            is_be,
            put_desc: desc,
            echo_token: 1,
            last_echo: Instant::now(),
            _reader_handle: reader_handle,
        })
    }

    // ─── pvmonitor ───────────────────────────────────────────────────────

    /// Subscribe to a PV and receive live updates via a callback.
    ///
    /// The callback returns [`ControlFlow::Continue`] to keep listening or
    /// [`ControlFlow::Break`] to stop the subscription.
    ///
    /// ```rust,ignore
    /// use std::ops::ControlFlow;
    ///
    /// client.pvmonitor("MY:PV", |value| {
    ///     println!("{value:?}");
    ///     ControlFlow::Continue(())
    /// }).await?;
    /// ```
    pub async fn pvmonitor<F>(&self, pv_name: &str, mut callback: F) -> Result<(), PvGetError>
    where
        F: FnMut(&DecodedValue) -> ControlFlow<()>,
    {
        let ChannelConn {
            mut stream,
            sid,
            version: _,
            is_be,
        } = self.open_channel(pv_name).await?;

        let ioid = alloc_ioid();
        let decoder = PvdDecoder::new(is_be);

        // MONITOR INIT — request value + alarm + timeStamp
        let pv_request = encode_pv_request(&["value", "alarm", "timeStamp"], is_be);
        let init = encode_monitor_request(sid, ioid, QOS_INIT, &pv_request, PVA_VERSION, is_be);
        stream.write_all(&init).await?;

        // Read INIT response — extract introspection
        let init_bytes = read_until(&mut stream, self.timeout, |cmd| {
            matches!(cmd, PvaPacketCommand::Op(op) if op.command == 13 && (op.subcmd & 0x08) != 0)
        })
        .await?;

        let field_desc = decode_init_introspection(&init_bytes, "MONITOR")?;

        // Start subscription (non-pipeline: START 0x04 + GET 0x40 = 0x44)
        let start = encode_monitor_request(sid, ioid, 0x44, &[], PVA_VERSION, is_be);
        stream.write_all(&start).await?;

        // Event loop — with echo keepalive and timeout resilience
        let mut echo_interval = interval(Duration::from_secs(10));
        let mut echo_token: u32 = 1;

        loop {
            tokio::select! {
                _ = echo_interval.tick() => {
                    let msg = encode_control_message(false, is_be, PVA_VERSION, 3, echo_token);
                    echo_token = echo_token.wrapping_add(1);
                    let _ = stream.write_all(&msg).await;
                }
                res = read_packet(&mut stream, self.timeout) => {
                    let bytes = match res {
                        Ok(b) => b,
                        Err(PvGetError::Timeout(_)) => continue,
                        Err(e) => return Err(e),
                    };
                    let mut pkt = PvaPacket::new(&bytes);
                    if let Some(PvaPacketCommand::Op(op)) = pkt.decode_payload()
                        && op.command == 13 && op.ioid == ioid && op.subcmd == 0x00 {
                            let payload = &bytes[8..]; // skip header
                            let pos = 5; // skip ioid(4) + subcmd(1)
                            if let Some((decoded, _)) =
                                decoder.decode_structure_with_bitset(&payload[pos..], &field_desc)
                                && callback(&decoded).is_break() {
                                    return Ok(());
                                }
                        }
                }
            }
        }
    }

    // ─── pvinfo ──────────────────────────────────────────────────────────

    /// Retrieve the field/structure description (introspection) for a PV.
    pub async fn pvinfo(&self, pv_name: &str) -> Result<StructureDesc, PvGetError> {
        let ChannelConn {
            mut stream,
            sid,
            version: _,
            is_be,
        } = self.open_channel(pv_name).await?;

        let ioid = alloc_ioid();
        let msg = encode_get_field_request(sid, ioid, None, PVA_VERSION, is_be);
        stream.write_all(&msg).await?;

        let resp_bytes = read_until(
            &mut stream,
            self.timeout,
            |cmd| matches!(cmd, PvaPacketCommand::Op(op) if op.command == 17),
        )
        .await?;

        decode_init_introspection(&resp_bytes, "GET_FIELD")
    }

    // ─── pvlist ──────────────────────────────────────────────────────────

    /// List PV names served by a specific server (via `__pvlist` GET).
    pub async fn pvlist(&self, server_addr: SocketAddr) -> Result<Vec<String>, PvGetError> {
        let opts = self.opts("__pvlist");
        crate::pvlist::pvlist(&opts, server_addr).await
    }

    /// List PV names with automatic fallback through all strategies.
    ///
    /// Tries: `__pvlist` → GET_FIELD (opt-in) → Server RPC → Server GET.
    pub async fn pvlist_with_fallback(
        &self,
        server_addr: SocketAddr,
    ) -> Result<(Vec<String>, crate::pvlist::PvListSource), PvGetError> {
        let opts = self.opts("__pvlist");
        crate::pvlist::pvlist_with_fallback(&opts, server_addr).await
    }
}

// ─── PvaChannel ──────────────────────────────────────────────────────────────

/// A persistent PVA channel for high-rate streaming PUT operations.
///
/// Created via [`PvaClient::open_put_channel`], this keeps the TCP connection
/// open and reuses the PUT introspection for repeated writes without
/// per-operation handshake overhead.
///
/// # Example
///
/// ```rust,ignore
/// let client = PvaClient::builder().build();
/// let mut channel = client.open_put_channel("MY:PV").await?;
/// for value in 0..100 {
///     channel.put(value as f64).await?;
/// }
/// ```
pub struct PvaChannel {
    writer: OwnedWriteHalf,
    sid: u32,
    ioid: u32,
    version: u8,
    is_be: bool,
    put_desc: StructureDesc,
    echo_token: u32,
    last_echo: Instant,
    _reader_handle: JoinHandle<()>,
}

impl PvaChannel {
    /// Write a value over the persistent channel.
    ///
    /// Automatically sends echo keepalive pings when more than 10 seconds
    /// have elapsed since the last one.
    pub async fn put(&mut self, value: impl Into<Value>) -> Result<(), PvGetError> {
        // Echo keepalive
        if self.last_echo.elapsed() >= Duration::from_secs(10) {
            let msg = encode_control_message(false, self.is_be, self.version, 3, self.echo_token);
            self.echo_token = self.echo_token.wrapping_add(1);
            let _ = self.writer.write_all(&msg).await;
            self.last_echo = Instant::now();
        }

        let json_val = value.into();
        let payload = encode_put_payload(&self.put_desc, &json_val, self.is_be)
            .map_err(|e| PvGetError::Protocol(format!("put encode: {e}")))?;
        let req = encode_put_request(
            self.sid,
            self.ioid,
            0x00,
            &payload,
            self.version,
            self.is_be,
        );
        self.writer.write_all(&req).await?;
        Ok(())
    }

    /// Returns the PUT introspection for this channel.
    pub fn introspection(&self) -> &StructureDesc {
        &self.put_desc
    }
}

impl Drop for PvaChannel {
    fn drop(&mut self) {
        self._reader_handle.abort();
    }
}

// ─── Standalone convenience functions ────────────────────────────────────────

/// Write a value to a PV (one-shot).
///
/// ```rust,ignore
/// use spvirit_client::{pvput, PvOptions};
///
/// pvput(&PvOptions::new("MY:PV".into()), 42.0).await?;
/// ```
pub async fn pvput(opts: &PvOptions, value: impl Into<Value>) -> Result<(), PvGetError> {
    let client = client_from_opts(opts);
    client.pvput(&opts.pv_name, value).await
}

/// Subscribe to a PV and receive live updates (one-shot).
///
/// The callback returns [`ControlFlow::Continue`] to keep listening or
/// [`ControlFlow::Break`] to stop.
pub async fn pvmonitor<F>(opts: &PvOptions, callback: F) -> Result<(), PvGetError>
where
    F: FnMut(&DecodedValue) -> ControlFlow<()>,
{
    let client = client_from_opts(opts);
    client.pvmonitor(&opts.pv_name, callback).await
}

/// Retrieve the field/structure description for a PV (one-shot).
pub async fn pvinfo(opts: &PvOptions) -> Result<StructureDesc, PvGetError> {
    let client = client_from_opts(opts);
    client.pvinfo(&opts.pv_name).await
}

// ─── Internal helpers ────────────────────────────────────────────────────────

/// Build a PvaClient inheriting configuration from PvOptions.
pub fn client_from_opts(opts: &PvOptions) -> PvaClient {
    let mut b = PvaClient::builder()
        .port(opts.tcp_port)
        .udp_port(opts.udp_port)
        .timeout(opts.timeout);
    if opts.no_broadcast {
        b = b.no_broadcast();
    }
    for ns in &opts.name_servers {
        b = b.name_server(*ns);
    }
    if let Some(ref u) = opts.authnz_user {
        b = b.authnz_user(u.clone());
    }
    if let Some(ref h) = opts.authnz_host {
        b = b.authnz_host(h.clone());
    }
    if let Some(addr) = opts.server_addr {
        b = b.server_addr(addr);
    }
    if let Some(addr) = opts.search_addr {
        b = b.search_addr(addr);
    }
    if let Some(addr) = opts.bind_addr {
        b = b.bind_addr(addr);
    }
    if opts.debug {
        b = b.debug();
    }
    b.build()
}

/// Decode an INIT response to extract the introspection StructureDesc.
fn decode_init_introspection(raw: &[u8], label: &str) -> Result<StructureDesc, PvGetError> {
    let mut pkt = PvaPacket::new(raw);
    let cmd = pkt
        .decode_payload()
        .ok_or_else(|| PvGetError::Decode(format!("{label} init response decode failed")))?;

    match cmd {
        PvaPacketCommand::Op(op) => {
            if let Some(ref st) = op.status
                && st.is_error()
            {
                let msg = st
                    .message
                    .clone()
                    .unwrap_or_else(|| format!("code={}", st.code));
                return Err(PvGetError::Protocol(format!("{label} init error: {msg}")));
            }
            op.introspection
                .ok_or_else(|| PvGetError::Decode(format!("missing {label} introspection")))
        }
        _ => Err(PvGetError::Protocol(format!(
            "unexpected {label} init response"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let c = PvaClient::builder().build();
        assert_eq!(c.tcp_port, 5075);
        assert_eq!(c.udp_port, 5076);
        assert_eq!(c.timeout, Duration::from_secs(5));
        assert!(!c.no_broadcast);
        assert!(c.name_servers.is_empty());
    }

    #[test]
    fn builder_overrides() {
        let c = PvaClient::builder()
            .port(9075)
            .udp_port(9076)
            .timeout(Duration::from_secs(10))
            .no_broadcast()
            .name_server("127.0.0.1:5075".parse().unwrap())
            .authnz_user("testuser")
            .authnz_host("testhost")
            .build();
        assert_eq!(c.tcp_port, 9075);
        assert_eq!(c.udp_port, 9076);
        assert_eq!(c.timeout, Duration::from_secs(10));
        assert!(c.no_broadcast);
        assert_eq!(c.name_servers.len(), 1);
        assert_eq!(c.authnz_user.as_deref(), Some("testuser"));
        assert_eq!(c.authnz_host.as_deref(), Some("testhost"));
    }

    #[test]
    fn opts_inherits_client_config() {
        let c = PvaClient::builder()
            .port(9075)
            .udp_port(9076)
            .timeout(Duration::from_secs(10))
            .no_broadcast()
            .build();
        let o = c.opts("TEST:PV");
        assert_eq!(o.pv_name, "TEST:PV");
        assert_eq!(o.tcp_port, 9075);
        assert_eq!(o.udp_port, 9076);
        assert_eq!(o.timeout, Duration::from_secs(10));
        assert!(o.no_broadcast);
    }

    #[test]
    fn client_from_opts_roundtrip() {
        let mut opts = PvOptions::new("X:Y".into());
        opts.tcp_port = 8075;
        opts.udp_port = 8076;
        opts.timeout = Duration::from_secs(3);
        opts.no_broadcast = true;
        let c = client_from_opts(&opts);
        assert_eq!(c.tcp_port, 8075);
        assert_eq!(c.udp_port, 8076);
        assert!(c.no_broadcast);
    }

    #[test]
    fn pv_get_options_alias_works() {
        // PvGetOptions is a type alias for PvOptions — verify it compiles and works
        let opts: crate::types::PvGetOptions = PvOptions::new("ALIAS:TEST".into());
        assert_eq!(opts.pv_name, "ALIAS:TEST");
    }
}
