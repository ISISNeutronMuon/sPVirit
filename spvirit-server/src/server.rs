//! Top-level PVA server orchestration.
//!
//! [`run_pva_server`] binds UDP + TCP + beacon and runs until cancelled.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use regex::Regex;
use tracing::{error, info};

use crate::beacon::{BeaconConfig, run_beacon};
use crate::handler::{PvListMode, ServerState, rand_guid, run_tcp_server, run_udp_search};
use crate::monitor::MonitorRegistry;
use crate::pvstore::PvStore;

/// Configuration for the PVA server.
pub struct PvaServerConfig {
    /// IP address to listen on (default: 0.0.0.0).
    pub listen_ip: IpAddr,
    /// TCP port (default: 5075).
    pub tcp_port: u16,
    /// UDP port (default: 5076).
    pub udp_port: u16,
    /// Address to advertise in search responses (None = auto).
    pub advertise_ip: Option<IpAddr>,
    /// Beacon target address (default: 224.0.0.128:5076).
    pub beacon_target: SocketAddr,
    /// Beacon period in seconds.
    pub beacon_period_secs: u64,
    /// Idle connection timeout.
    pub conn_timeout: Duration,
    /// Whether to compute alarms from limits.
    pub compute_alarms: bool,
    /// PV list mode.
    pub pvlist_mode: PvListMode,
    /// Maximum PV names in pvlist responses.
    pub pvlist_max: usize,
    /// Optional regex filter for pvlist.
    pub pvlist_allow_pattern: Option<Regex>,
}

impl Default for PvaServerConfig {
    fn default() -> Self {
        Self {
            listen_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            tcp_port: 5075,
            udp_port: 5076,
            advertise_ip: None,
            beacon_target: "224.0.0.128:5076".parse().unwrap(),
            beacon_period_secs: 15,
            conn_timeout: Duration::from_secs(64000),
            compute_alarms: false,
            pvlist_mode: PvListMode::List,
            pvlist_max: 1024,
            pvlist_allow_pattern: None,
        }
    }
}

/// Shared server state wrapping a [`PvStore`] implementation.
///
/// Consumers can hold an `Arc<PvaServerState<S>>` to inspect or mutate the
/// underlying store while the server tasks are running.
pub struct PvaServerState<S: PvStore> {
    pub inner: Arc<ServerState<S>>,
    pub registry: Arc<MonitorRegistry>,
}

impl<S: PvStore> PvaServerState<S> {
    pub fn new(store: Arc<S>, config: &PvaServerConfig) -> Self {
        Self::with_registry(store, config, Arc::new(MonitorRegistry::new()))
    }

    pub fn with_registry(
        store: Arc<S>,
        config: &PvaServerConfig,
        registry: Arc<MonitorRegistry>,
    ) -> Self {
        let inner = Arc::new(ServerState::new(
            store,
            registry.clone(),
            config.compute_alarms,
            config.pvlist_mode,
            config.pvlist_max,
            config.pvlist_allow_pattern.clone(),
        ));
        Self { inner, registry }
    }
}

/// Run a PVA server (UDP search + TCP handler + beacon).
///
/// This function drives the three server tasks in a `tokio::select!` loop and
/// returns when any task errors or the future is dropped.
pub async fn run_pva_server<S: PvStore>(
    store: Arc<S>,
    config: PvaServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let registry = Arc::new(MonitorRegistry::new());
    run_pva_server_with_registry(store, config, registry).await
}

/// Like [`run_pva_server`] but re-uses an existing [`MonitorRegistry`].
pub async fn run_pva_server_with_registry<S: PvStore>(
    store: Arc<S>,
    config: PvaServerConfig,
    registry: Arc<MonitorRegistry>,
) -> Result<(), Box<dyn std::error::Error>> {
    let server_state = PvaServerState::with_registry(store, &config, registry);
    let state = server_state.inner;

    let guid = rand_guid();
    let tcp_addr = SocketAddr::new(config.listen_ip, config.tcp_port);
    let udp_addr = SocketAddr::new(config.listen_ip, config.udp_port);

    info!(
        "Starting PVA server: udp={} tcp={} pvlist_mode={:?} pvlist_max={} filter={}",
        udp_addr,
        tcp_addr,
        config.pvlist_mode,
        config.pvlist_max,
        config
            .pvlist_allow_pattern
            .as_ref()
            .map(|r| r.as_str())
            .unwrap_or("<none>")
    );

    let beacon_config = BeaconConfig {
        target: config.beacon_target,
        guid,
        tcp_port: config.tcp_port,
        advertise_ip: config.advertise_ip,
        listen_ip: config.listen_ip,
        period_secs: config.beacon_period_secs,
    };

    let udp_state = state.clone();
    let udp_task = tokio::spawn(async move {
        if let Err(e) = run_udp_search(
            udp_state,
            udp_addr,
            config.tcp_port,
            guid,
            config.advertise_ip,
        )
        .await
        {
            error!("UDP search server error: {}", e);
        }
    });

    let tcp_state = state.clone();
    let tcp_task = tokio::spawn(async move {
        if let Err(e) = run_tcp_server(tcp_state, tcp_addr, config.conn_timeout).await {
            error!("TCP server error: {}", e);
        }
    });

    let beacon_change = state.beacon_change.clone();
    let beacon_task = tokio::spawn(async move {
        if let Err(e) = run_beacon(beacon_config, beacon_change).await {
            error!("Beacon task error: {}", e);
        }
    });

    let _ = tokio::join!(udp_task, tcp_task, beacon_task);
    Ok(())
}
