//! PVA beacon sender.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use spvirit_codec::spvirit_encode::{encode_beacon, ip_to_bytes};
use tokio::net::UdpSocket;

/// Configuration for the beacon sender.
pub struct BeaconConfig {
    pub target: SocketAddr,
    pub guid: [u8; 12],
    pub tcp_port: u16,
    pub advertise_ip: Option<IpAddr>,
    pub listen_ip: IpAddr,
    pub period_secs: u64,
}

/// Run the beacon sender loop.  `change_counter` is polled atomically each tick.
pub async fn run_beacon(
    config: BeaconConfig,
    change_counter: Arc<AtomicU16>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if config.period_secs == 0 {
        return Ok(());
    }
    let bind_addr = if config.target.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind_addr).await?;
    socket.set_broadcast(true)?;
    let mut interval = tokio::time::interval(Duration::from_secs(config.period_secs));
    let mut seq: u8 = 0;

    loop {
        interval.tick().await;
        let resp_ip = if let Some(ip) = config.advertise_ip {
            ip
        } else if !config.listen_ip.is_unspecified() {
            config.listen_ip
        } else {
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        };
        let addr_bytes = if resp_ip.is_unspecified() {
            [0u8; 16]
        } else {
            ip_to_bytes(resp_ip)
        };
        let change_count = change_counter.load(Ordering::SeqCst);
        let msg = encode_beacon(
            config.guid,
            seq,
            change_count,
            addr_bytes,
            config.tcp_port,
            "tcp",
            2,
            false,
        );
        let _ = socket.send_to(&msg, config.target).await;
        seq = seq.wrapping_add(1);
    }
}
