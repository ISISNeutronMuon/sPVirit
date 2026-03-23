use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use dns_lookup::lookup_host;
use get_if_addrs::{get_if_addrs, IfAddr};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::AsyncWriteExt;
use tokio::net::UdpSocket;
use tracing::debug;

use crate::pva_client::transport::read_packet;
use crate::pva_client::types::{PvGetError, PvGetOptions};
use pva_codec::epics_decode::{PvaPacket, PvaPacketCommand};
use pva_codec::pva_encode::{encode_header, ip_to_bytes};

#[derive(Clone, Copy, Debug)]
pub struct SearchTarget {
    pub target: IpAddr,
    pub bind: IpAddr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DiscoveredServer {
    pub guid: [u8; 12],
    pub tcp_addr: SocketAddr,
}

fn encode_size(size: usize, is_be: bool) -> Vec<u8> {
    if size == 0 {
        return vec![0x00];
    }
    if size < 254 {
        return vec![size as u8];
    }
    let mut out = vec![0xFE];
    let bytes = if is_be {
        (size as u32).to_be_bytes()
    } else {
        (size as u32).to_le_bytes()
    };
    out.extend_from_slice(&bytes);
    out
}

fn encode_string(value: &str, is_be: bool) -> Vec<u8> {
    let bytes = value.as_bytes();
    let mut out = encode_size(bytes.len(), is_be);
    out.extend_from_slice(bytes);
    out
}

fn encode_search_request(
    seq: u32,
    port: u16,
    reply_addr: [u8; 16],
    pv_requests: &[(u32, &str)],
    version: u8,
    is_be: bool,
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&if is_be {
        seq.to_be_bytes()
    } else {
        seq.to_le_bytes()
    });
    payload.push(0x81); // mask: reply required + unicast
    payload.extend_from_slice(&[0u8; 3]); // reserved
    payload.extend_from_slice(&reply_addr);
    payload.extend_from_slice(&if is_be {
        port.to_be_bytes()
    } else {
        port.to_le_bytes()
    });
    payload.extend_from_slice(&encode_size(1, is_be));
    payload.extend_from_slice(&encode_string("tcp", is_be));
    payload.extend_from_slice(&if is_be {
        (pv_requests.len() as u16).to_be_bytes()
    } else {
        (pv_requests.len() as u16).to_le_bytes()
    });
    for (cid, pv_name) in pv_requests {
        payload.extend_from_slice(&if is_be {
            cid.to_be_bytes()
        } else {
            cid.to_le_bytes()
        });
        payload.extend_from_slice(&encode_string(pv_name, is_be));
    }

    let mut out = encode_header(false, is_be, false, version, 3, payload.len() as u32);
    out.extend_from_slice(&payload);
    out
}

pub fn parse_addr_list(env: &str) -> Vec<IpAddr> {
    env.split(|c| c == ',' || c == ' ' || c == '\t')
        .filter(|s| !s.trim().is_empty())
        .filter_map(|s| parse_search_target_ip(s.trim()))
        .collect()
}

fn parse_search_target_ip(token: &str) -> Option<IpAddr> {
    if token.is_empty() {
        return None;
    }

    if let Ok(ip) = token.parse::<IpAddr>() {
        return Some(ip);
    }
    if let Ok(sock) = token.parse::<SocketAddr>() {
        return Some(sock.ip());
    }

    // Accept host:port where host may be a name or an IP literal.
    // For IPv6 bracket notation [::1]:port, SocketAddr::parse above already handles it.
    if let Some((host, port_str)) = token.rsplit_once(':') {
        if !host.is_empty()
            && !port_str.is_empty()
            && port_str.chars().all(|c| c.is_ascii_digit())
            && !host.contains(']')
        {
            if let Ok(ip) = host.parse::<IpAddr>() {
                return Some(ip);
            }
            if let Ok(addrs) = lookup_host(host) {
                // Prefer IPv4 for backward compat, fall back to first IPv6
                let addrs: Vec<IpAddr> = addrs.collect();
                if let Some(ip) = addrs.iter().find(|ip| ip.is_ipv4()).copied()
                    .or_else(|| addrs.into_iter().next())
                {
                    return Some(ip);
                }
            }
        }
    }

    if let Ok(addrs) = lookup_host(token) {
        // Prefer IPv4, fall back to first IPv6
        let addrs: Vec<IpAddr> = addrs.collect();
        if let Some(ip) = addrs.iter().find(|ip| ip.is_ipv4()).copied()
            .or_else(|| addrs.into_iter().next())
        {
            return Some(ip);
        }
    }

    None
}

/// Return a default unspecified bind address matching the target's address family.
fn unspecified_for(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    }
}

pub fn build_search_targets(
    search_addr: Option<IpAddr>,
    bind_addr: Option<IpAddr>,
) -> Vec<SearchTarget> {
    // Explicit --search-addr overrides everything (single target).
    if let Some(ip) = search_addr {
        return vec![SearchTarget {
            target: ip,
            bind: bind_addr.unwrap_or_else(|| unspecified_for(ip)),
        }];
    }

    let mut targets = Vec::new();
    let mut seen = HashSet::new();

    // Addresses from EPICS_PVA_ADDR_LIST.
    if let Ok(env) = std::env::var("EPICS_PVA_ADDR_LIST") {
        for ip in parse_addr_list(&env) {
            if seen.insert(ip) {
                targets.push(SearchTarget {
                    target: ip,
                    bind: bind_addr.unwrap_or_else(|| unspecified_for(ip)),
                });
            }
        }
    }

    // Merge auto-discovered broadcast addresses unless explicitly disabled.
    // This matches EPICS Base behaviour: ADDR_LIST + auto-broadcast combined.
    if is_auto_addr_list_enabled() {
        for t in build_auto_broadcast_targets() {
            if seen.insert(t.target) {
                targets.push(SearchTarget {
                    target: t.target,
                    bind: bind_addr.unwrap_or(t.bind),
                });
            }
        }
    }

    targets
}

pub fn is_auto_addr_list_enabled() -> bool {
    match std::env::var("EPICS_PVA_AUTO_ADDR_LIST") {
        Ok(v) => {
            let v = v.trim().to_ascii_uppercase();
            v == "YES" || v == "Y" || v == "1" || v == "TRUE"
        }
        Err(_) => true,
    }
}

fn ipv4_is_link_local(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    octets[0] == 169 && octets[1] == 254
}

fn choose_default_bind_v4() -> Option<Ipv4Addr> {
    let ifaces = get_if_addrs().ok()?;
    for iface in ifaces {
        if let IfAddr::V4(v4) = iface.addr {
            let ip = v4.ip;
            if ip.is_loopback() || ipv4_is_link_local(ip) {
                continue;
            }
            return Some(ip);
        }
    }
    None
}

fn choose_default_bind_v6() -> Option<Ipv6Addr> {
    let ifaces = get_if_addrs().ok()?;
    for iface in ifaces {
        if let IfAddr::V6(v6) = iface.addr {
            let ip = v6.ip;
            if ip.is_loopback() {
                continue;
            }
            // Skip link-local (fe80::/10) — not routable without scope id
            let segs = ip.segments();
            if segs[0] & 0xffc0 == 0xfe80 {
                continue;
            }
            return Some(ip);
        }
    }
    None
}

fn broadcast_for(ip: Ipv4Addr, netmask: Ipv4Addr) -> Ipv4Addr {
    let ip_u = u32::from(ip);
    let mask_u = u32::from(netmask);
    Ipv4Addr::from(ip_u | !mask_u)
}

fn discovery_target_for(ip: Ipv4Addr, netmask: Ipv4Addr) -> Ipv4Addr {
    let limited_broadcast = Ipv4Addr::new(255, 255, 255, 255);
    if netmask == Ipv4Addr::new(255, 255, 255, 255) || netmask.is_unspecified() {
        return limited_broadcast;
    }
    let directed = broadcast_for(ip, netmask);
    if directed == ip {
        limited_broadcast
    } else {
        directed
    }
}

pub fn build_auto_broadcast_targets() -> Vec<SearchTarget> {
    let mut targets = Vec::new();
    let mut fallback_targets = Vec::new();
    let mut fallback_seen = HashSet::new();
    let mut added_v4_multicast = false;
    let mut added_v6_multicast = false;
    let ifaces = match get_if_addrs() {
        Ok(v) => v,
        Err(_) => return targets,
    };
    for iface in &ifaces {
        if let IfAddr::V4(v4) = &iface.addr {
            let ip = v4.ip;
            if ip.is_loopback() || ipv4_is_link_local(ip) {
                continue;
            }
            let bcast = discovery_target_for(ip, v4.netmask);
            targets.push(SearchTarget {
                target: IpAddr::V4(bcast),
                bind: IpAddr::V4(ip),
            });
            // Also send to IPv4 multicast group (matching PVXS behaviour).
            // Docker overlay networks may block broadcast but allow multicast.
            targets.push(SearchTarget {
                target: IpAddr::V4(PVA_MULTICAST_V4),
                bind: IpAddr::V4(ip),
            });
            if fallback_seen.insert(IpAddr::V4(bcast)) {
                fallback_targets.push(SearchTarget {
                    target: IpAddr::V4(bcast),
                    bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                });
            }
            if !added_v4_multicast {
                added_v4_multicast = true;
                fallback_targets.push(SearchTarget {
                    target: IpAddr::V4(PVA_MULTICAST_V4),
                    bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                });
            }
        }
    }
    // Add IPv6 multicast targets for each non-loopback, non-link-local v6 iface.
    for iface in &ifaces {
        if let IfAddr::V6(v6) = &iface.addr {
            let ip = v6.ip;
            if ip.is_loopback() {
                continue;
            }
            let segs = ip.segments();
            if segs[0] & 0xffc0 == 0xfe80 {
                continue; // skip link-local
            }
            let multicast_target = IpAddr::V6(PVA_MULTICAST_V6);
            targets.push(SearchTarget {
                target: multicast_target,
                bind: IpAddr::V6(ip),
            });
            if !added_v6_multicast {
                added_v6_multicast = true;
                fallback_targets.push(SearchTarget {
                    target: multicast_target,
                    bind: IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                });
            }
        }
    }
    targets.extend(fallback_targets);
    targets
}

/// PVA multicast group (IPv4).
const PVA_MULTICAST_V4: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 128);

/// PVA multicast group (IPv6 link-local, ff02::42:1).
const PVA_MULTICAST_V6: Ipv6Addr = Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0x42, 1);

/// Best-effort join the PVA multicast group appropriate for the bind address.
fn join_multicast_any(socket: &std::net::UdpSocket, bind: IpAddr) {
    match bind {
        IpAddr::V4(iface) => {
            let _ = socket.join_multicast_v4(&PVA_MULTICAST_V4, &iface);
        }
        IpAddr::V6(_) => {
            // interface index 0 = OS picks the default interface
            let _ = socket.join_multicast_v6(&PVA_MULTICAST_V6, 0);
        }
    }
}

fn decode_search_response_addr(addr: [u8; 16], port: u16, src: SocketAddr) -> SocketAddr {
    let mut ip = if addr[0..12] == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF] {
        IpAddr::V4(Ipv4Addr::new(addr[12], addr[13], addr[14], addr[15]))
    } else {
        IpAddr::V6(addr.into())
    };
    // Some servers respond with 0.0.0.0 or ::; fall back to the UDP source address.
    if ip.is_unspecified() {
        ip = src.ip();
    }
    SocketAddr::new(ip, port)
}

fn normalize_discovered_servers(items: Vec<DiscoveredServer>) -> Vec<DiscoveredServer> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for item in items {
        if seen.insert((item.guid, item.tcp_addr)) {
            out.push(item);
        }
    }
    out.sort_by(|a, b| a.tcp_addr.to_string().cmp(&b.tcp_addr.to_string()));
    out
}

/// Create a UDP socket with SO_REUSEADDR set (matching PVXS behaviour),
/// allowing multiple processes to share the search port.
///
/// On Windows SO_REUSEADDR has different (unsafe) semantics — it allows
/// a second socket to steal an actively-used port — so we only enable it
/// on Unix where it merely permits rebinding during TIME_WAIT.
fn bind_udp_reuse(addr: SocketAddr) -> std::io::Result<std::net::UdpSocket> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let sock = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    #[cfg(unix)]
    sock.set_reuse_address(true)?;
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    Ok(sock.into())
}

pub async fn search_pv(
    pv_name: &str,
    udp_port: u16,
    timeout_dur: Duration,
    targets: &[SearchTarget],
    debug_enabled: bool,
) -> Result<SocketAddr, PvGetError> {
    if targets.is_empty() {
        return Err(PvGetError::Search("no search targets"));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let seq = (now.as_nanos() as u32).wrapping_add(std::process::id());
    let cid = seq ^ 0x9E37_79B9;

    let mut last_io_error: Option<std::io::Error> = None;
    let deadline = tokio::time::Instant::now() + timeout_dur;

    // Group targets by bind address so we can share a socket per bind.
    let mut bind_groups: Vec<(IpAddr, Vec<IpAddr>)> = Vec::new();
    for t in targets {
        if let Some(group) = bind_groups.iter_mut().find(|(b, _)| *b == t.bind) {
            group.1.push(t.target);
        } else {
            bind_groups.push((t.bind, vec![t.target]));
        }
    }

    // Open sockets and send to all targets first, then collect responses.
    // Store (socket, message, destinations) for retransmission.
    let mut socket_info: Vec<(Arc<UdpSocket>, Vec<u8>, Vec<SocketAddr>)> = Vec::new();

    for (bind_ip, group_targets) in &bind_groups {
        let bind_addr = SocketAddr::new(*bind_ip, udp_port);
        let (std_sock, actual_bind_addr) = match bind_udp_reuse(bind_addr) {
            Ok(sock) => (sock, bind_addr),
            Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => {
                let fallback = SocketAddr::new(*bind_ip, 0);
                match bind_udp_reuse(fallback) {
                    Ok(sock) => {
                        let actual = sock.local_addr().unwrap_or(fallback);
                        if debug_enabled {
                            debug!(
                                "pva search bind={} failed (in use), fallback bind={}",
                                bind_addr, actual
                            );
                        }
                        (sock, actual)
                    }
                    Err(fallback_err) => {
                        if debug_enabled {
                            debug!(
                                "pva search skipping bind={} step=bind-fallback kind={:?} err={}",
                                bind_addr,
                                fallback_err.kind(),
                                fallback_err
                            );
                        }
                        last_io_error = Some(fallback_err);
                        continue;
                    }
                }
            }
            Err(err) => {
                if debug_enabled {
                    debug!(
                        "pva search skipping bind={} step=bind kind={:?} err={}",
                        bind_addr,
                        err.kind(),
                        err
                    );
                }
                last_io_error = Some(err);
                continue;
            }
        };
        if let Err(err) = std_sock.set_broadcast(true) {
            if debug_enabled {
                debug!(
                    "pva search skipping bind={} step=set_broadcast kind={:?} err={}",
                    bind_addr,
                    err.kind(),
                    err
                );
            }
            last_io_error = Some(err);
            continue;
        }

        join_multicast_any(&std_sock, *bind_ip);

        let reply_addr = ip_to_bytes(*bind_ip);
        let reply_port = match std_sock.local_addr() {
            Ok(addr) => addr.port(),
            Err(err) => {
                if debug_enabled {
                    debug!(
                        "pva search skipping bind={} step=local_addr kind={:?} err={}",
                        bind_addr,
                        err.kind(),
                        err
                    );
                }
                last_io_error = Some(err);
                continue;
            }
        };
        let requests = [(cid, pv_name)];
        let msg = encode_search_request(seq, reply_port, reply_addr, &requests, 2, false);

        let socket = match UdpSocket::from_std(std_sock) {
            Ok(socket) => socket,
            Err(err) => {
                if debug_enabled {
                    debug!(
                        "pva search skipping bind={} step=from_std kind={:?} err={}",
                        bind_addr,
                        err.kind(),
                        err
                    );
                }
                last_io_error = Some(err);
                continue;
            }
        };

        let dests: Vec<SocketAddr> = group_targets
            .iter()
            .map(|ip| SocketAddr::new(*ip, udp_port))
            .collect();

        // Send to every target in this bind group immediately.
        for dest in &dests {
            if debug_enabled {
                debug!(
                    "pva search bind={} target={} server_port={} reply_port={}",
                    actual_bind_addr, dest.ip(), udp_port, reply_port
                );
                debug!("pva search seq={} cid={}", seq, cid);
                debug!("pva search send {} bytes to {}", msg.len(), dest);
            }
            if let Err(err) = socket.send_to(&msg, dest).await {
                if debug_enabled {
                    debug!(
                        "pva search send_to target={} kind={:?} err={}",
                        dest,
                        err.kind(),
                        err
                    );
                }
                last_io_error = Some(err);
            }
        }

        socket_info.push((Arc::new(socket), msg, dests));
    }

    if socket_info.is_empty() {
        if let Some(err) = last_io_error {
            return Err(PvGetError::Io(err));
        }
        return Err(PvGetError::Timeout("search response"));
    }

    // Spawn a receiver task per socket that forwards packets into a shared channel.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(Vec<u8>, SocketAddr)>(64);
    for (sock, _, _) in &socket_info {
        let sock = Arc::clone(sock);
        let tx = tx.clone();
        tokio::spawn(async move {
            loop {
                let mut buf = vec![0u8; 2048];
                match sock.recv_from(&mut buf).await {
                    Ok((len, src)) => {
                        buf.truncate(len);
                        if tx.send((buf, src)).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
    }
    drop(tx); // Only spawned tasks hold senders; channel closes when they exit.

    // Retransmit schedule: exponential backoff from start.
    let retransmit_offsets = [100u64, 500, 1000, 2000];
    let start = tokio::time::Instant::now();
    let mut next_retransmit = 0usize;

    loop {
        // Compute the next wake-up: either the next retransmit or the deadline.
        let next_retransmit_at = if next_retransmit < retransmit_offsets.len() {
            start + Duration::from_millis(retransmit_offsets[next_retransmit])
        } else {
            deadline
        };
        let wake_at = next_retransmit_at.min(deadline);

        tokio::select! {
            recv = rx.recv() => {
                let Some((buf, src)) = recv else { break };
                let mut pkt = PvaPacket::new(&buf);
                let cmd = pkt
                    .decode_payload()
                    .ok_or(PvGetError::Search("failed to decode search response"))?;
                if let PvaPacketCommand::SearchResponse(payload) = cmd {
                    if debug_enabled {
                        debug!(
                            "pva search response found={} cids={:?} addr={:?} port={}",
                            payload.found, payload.cids, payload.addr, payload.port
                        );
                    }
                    if payload.seq != seq {
                        continue;
                    }
                    if !payload.protocol.is_empty() && !payload.protocol.eq_ignore_ascii_case("tcp") {
                        continue;
                    }
                    if !payload.found {
                        continue;
                    }
                    if !payload.cids.is_empty() && !payload.cids.contains(&cid) {
                        continue;
                    }

                    let addr = decode_search_response_addr(payload.addr, payload.port, src);
                    if debug_enabled {
                        debug!("pva search response from {}", addr);
                    }
                    return Ok(addr);
                }
            }
            _ = tokio::time::sleep_until(wake_at) => {
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                // Retransmit to all targets on all sockets.
                if next_retransmit < retransmit_offsets.len() {
                    if debug_enabled {
                        debug!("pva search retransmit round {}", next_retransmit + 1);
                    }
                    for (sock, msg, dests) in &socket_info {
                        for dest in dests {
                            let _ = sock.send_to(msg, dest).await;
                        }
                    }
                    next_retransmit += 1;
                }
            }
        }
    }

    Err(PvGetError::Timeout("search response"))
}

pub fn default_bind_ip() -> Option<IpAddr> {
    choose_default_bind_v4()
        .map(IpAddr::V4)
        .or_else(|| choose_default_bind_v6().map(IpAddr::V6))
}

/// Parse `EPICS_PVA_NAME_SERVERS` value into socket addresses.
/// Accepts space/comma separated entries: `host:port`, `ip`, `hostname`
/// (port defaults to 5075).
pub fn parse_name_servers(env_val: &str) -> Vec<SocketAddr> {
    let mut out = Vec::new();
    for token in env_val.split(|c| c == ',' || c == ' ' || c == '\t') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        if let Ok(addr) = token.parse::<SocketAddr>() {
            out.push(addr);
            continue;
        }
        if let Ok(ip) = token.parse::<IpAddr>() {
            out.push(SocketAddr::new(ip, 5075));
            continue;
        }
        use std::net::ToSocketAddrs;
        if let Ok(mut addrs) = token.to_socket_addrs() {
            if let Some(addr) = addrs.next() {
                out.push(addr);
                continue;
            }
        }
        let with_port = format!("{}:5075", token);
        if let Ok(mut addrs) = with_port.to_socket_addrs() {
            if let Some(addr) = addrs.next() {
                out.push(addr);
            }
        }
    }
    out
}

/// Build a minimal PVA ConnectionValidation response for name server search.
fn encode_search_validation(version: u8, is_be: bool) -> Vec<u8> {
    let mut payload = Vec::new();
    let buf_size = 87040u32;
    payload.extend_from_slice(&if is_be {
        buf_size.to_be_bytes()
    } else {
        buf_size.to_le_bytes()
    });
    let reg_size = 32767u16;
    payload.extend_from_slice(&if is_be {
        reg_size.to_be_bytes()
    } else {
        reg_size.to_le_bytes()
    });
    let qos = 0u16;
    payload.extend_from_slice(&if is_be {
        qos.to_be_bytes()
    } else {
        qos.to_le_bytes()
    });
    payload.extend_from_slice(&encode_string("ca", is_be));
    let user = std::env::var("USER")
        .or_else(|_| std::env::var("LOGNAME"))
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    payload.extend_from_slice(&[0xFD, 0x01, 0x00, 0x80, 0x00]);
    payload.push(0x02);
    payload.push(0x04);
    payload.extend_from_slice(b"user");
    payload.push(0x60);
    payload.push(0x04);
    payload.extend_from_slice(b"host");
    payload.push(0x60);
    let user_bytes = user.as_bytes();
    let host_bytes = host.as_bytes();
    payload.push(user_bytes.len() as u8);
    payload.extend_from_slice(user_bytes);
    payload.push(host_bytes.len() as u8);
    payload.extend_from_slice(host_bytes);
    let mut out = encode_header(false, is_be, false, version, 1, payload.len() as u32);
    out.extend_from_slice(&payload);
    out
}

/// Search for a PV via a TCP connection to a PVA name server.
///
/// Connects to the name server, performs the PVA handshake, sends a search
/// request over TCP, and returns the server address from the search response.
pub async fn search_pv_tcp(
    pv_name: &str,
    name_server: SocketAddr,
    timeout_dur: Duration,
    debug_enabled: bool,
) -> Result<SocketAddr, PvGetError> {
    let deadline = tokio::time::Instant::now() + timeout_dur;

    let mut stream =
        tokio::time::timeout(timeout_dur, tokio::net::TcpStream::connect(name_server))
            .await
            .map_err(|_| PvGetError::Timeout("name server connect"))??;

    let mut version = 2u8;
    let mut is_be = false;

    // Read SET_BYTE_ORDER + ConnectionValidation from name server.
    for _ in 0..2 {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(PvGetError::Timeout("name server handshake"));
        }
        let remaining = deadline - now;
        if let Ok(bytes) = read_packet(&mut stream, remaining).await {
            let mut pkt = PvaPacket::new(&bytes);
            if let Some(cmd) = pkt.decode_payload() {
                match cmd {
                    PvaPacketCommand::Control(payload) => {
                        if payload.command == 2 {
                            is_be = pkt.header.flags.is_msb;
                        }
                    }
                    PvaPacketCommand::ConnectionValidation(_) => {
                        version = pkt.header.version;
                        is_be = pkt.header.flags.is_msb;
                    }
                    _ => {}
                }
            }
        }
    }

    let validation = encode_search_validation(version, is_be);
    stream.write_all(&validation).await?;

    // Wait for ConnectionValidated.
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(PvGetError::Timeout("name server validated"));
        }
        let remaining = deadline - now;
        let bytes = read_packet(&mut stream, remaining).await?;
        let mut pkt = PvaPacket::new(&bytes);
        if let Some(cmd) = pkt.decode_payload() {
            if matches!(cmd, PvaPacketCommand::ConnectionValidated(_)) {
                break;
            }
        }
    }

    // Send search request over TCP.
    let now_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let seq = (now_ts.as_nanos() as u32).wrapping_add(std::process::id());
    let cid = seq ^ 0x9E37_79B9;
    let requests = [(cid, pv_name)];
    let msg = encode_search_request(seq, 0, [0u8; 16], &requests, version, is_be);
    stream.write_all(&msg).await?;

    if debug_enabled {
        debug!(
            "pva tcp search sent to name_server={} pv={}",
            name_server, pv_name
        );
    }

    // Read search response.
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(PvGetError::Timeout("name server search response"));
        }
        let remaining = deadline - now;
        let bytes = read_packet(&mut stream, remaining).await?;
        let mut pkt = PvaPacket::new(&bytes);
        if let Some(cmd) = pkt.decode_payload() {
            if let PvaPacketCommand::SearchResponse(payload) = cmd {
                if !payload.found {
                    continue;
                }
                if !payload.cids.is_empty() && !payload.cids.contains(&cid) {
                    continue;
                }
                let addr =
                    decode_search_response_addr(payload.addr, payload.port, name_server);
                if debug_enabled {
                    debug!(
                        "pva tcp search response from name_server={}: {}",
                        name_server, addr
                    );
                }
                return Ok(addr);
            }
        }
    }
}

/// Resolve the PVA server for a PV using name servers (TCP) and/or UDP search.
///
/// - If `opts.server_addr` is set, returns it directly.
/// - Tries each name server from `opts.name_servers` and `EPICS_PVA_NAME_SERVERS`
///   via TCP search.
/// - Falls back to UDP search using `build_search_targets()`.
pub async fn resolve_pv_server(opts: &PvGetOptions) -> Result<SocketAddr, PvGetError> {
    if let Some(addr) = opts.server_addr {
        return Ok(addr);
    }

    let mut name_servers = opts.name_servers.clone();
    if let Ok(env) = std::env::var("EPICS_PVA_NAME_SERVERS") {
        name_servers.extend(parse_name_servers(&env));
    }

    let no_broadcast = opts.no_broadcast;

    // Fail fast when no search strategy is available.
    if no_broadcast && name_servers.is_empty() {
        return Err(PvGetError::Search(
            "no search strategy: specify --name-server or --server when using --no-broadcast",
        ));
    }

    // Launch all search strategies concurrently — TCP name servers + UDP broadcast.
    // Return the first successful result.
    let targets = build_search_targets(opts.search_addr, opts.bind_addr);

    let pv = opts.pv_name.clone();
    let timeout_dur = opts.timeout;
    let debug_enabled = opts.debug;
    let udp_port = opts.udp_port;
    let tcp_port = opts.tcp_port;

    let mut set = tokio::task::JoinSet::new();

    for ns in name_servers {
        let pv = pv.clone();
        set.spawn(async move {
            let addr = search_pv_tcp(&pv, ns, timeout_dur, debug_enabled).await?;
            Ok::<SocketAddr, PvGetError>(SocketAddr::new(addr.ip(), tcp_port))
        });
    }

    if !no_broadcast {
        let pv = pv.clone();
        let targets = targets.clone();
        set.spawn(async move {
            let addr = search_pv(&pv, udp_port, timeout_dur, &targets, debug_enabled).await?;
            Ok(SocketAddr::new(addr.ip(), tcp_port))
        });
    }

    let mut last_err = None;
    while let Some(result) = set.join_next().await {
        match result {
            Ok(Ok(addr)) => {
                set.abort_all();
                return Ok(addr);
            }
            Ok(Err(e)) => {
                if debug_enabled {
                    debug!("pva search strategy failed: {}", e);
                }
                last_err = Some(e);
            }
            Err(join_err) => {
                if debug_enabled {
                    debug!("pva search task panicked: {}", join_err);
                }
            }
        }
    }

    Err(last_err.unwrap_or(PvGetError::Timeout("search response")))
}

pub async fn discover_servers(
    udp_port: u16,
    timeout_dur: Duration,
    targets: &[SearchTarget],
    debug_enabled: bool,
) -> Result<Vec<DiscoveredServer>, PvGetError> {
    if targets.is_empty() {
        return Err(PvGetError::Search("no search targets"));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let seq = (now.as_nanos() as u32).wrapping_add(std::process::id());

    let mut found: Vec<DiscoveredServer> = Vec::new();
    let mut last_io_error: Option<std::io::Error> = None;
    let deadline = tokio::time::Instant::now() + timeout_dur;

    // Group targets by bind address so we can share a socket per bind.
    let mut bind_groups: Vec<(IpAddr, Vec<IpAddr>)> = Vec::new();
    for t in targets {
        if let Some(group) = bind_groups.iter_mut().find(|(b, _)| *b == t.bind) {
            group.1.push(t.target);
        } else {
            bind_groups.push((t.bind, vec![t.target]));
        }
    }

    // Open sockets and send to all targets first, then collect responses.
    // Store (socket, message, destinations) for retransmission.
    let mut socket_info: Vec<(Arc<UdpSocket>, Vec<u8>, Vec<SocketAddr>)> = Vec::new();

    for (bind_ip, group_targets) in &bind_groups {
        let bind_addr = SocketAddr::new(*bind_ip, udp_port);
        let (std_sock, actual_bind_addr) = match bind_udp_reuse(bind_addr) {
            Ok(sock) => (sock, bind_addr),
            Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => {
                let fallback = SocketAddr::new(*bind_ip, 0);
                match bind_udp_reuse(fallback) {
                    Ok(sock) => {
                        let actual = sock.local_addr().unwrap_or(fallback);
                        if debug_enabled {
                            debug!(
                                "pva discover bind={} failed (in use), fallback bind={}",
                                bind_addr, actual
                            );
                        }
                        (sock, actual)
                    }
                    Err(fallback_err) => {
                        if debug_enabled {
                            debug!(
                                "pva discover skipping bind={} step=bind-fallback kind={:?} err={}",
                                bind_addr,
                                fallback_err.kind(),
                                fallback_err
                            );
                        }
                        last_io_error = Some(fallback_err);
                        continue;
                    }
                }
            }
            Err(err) => {
                if debug_enabled {
                    debug!(
                        "pva discover skipping bind={} step=bind kind={:?} err={}",
                        bind_addr,
                        err.kind(),
                        err
                    );
                }
                last_io_error = Some(err);
                continue;
            }
        };
        if let Err(err) = std_sock.set_broadcast(true) {
            if debug_enabled {
                debug!(
                    "pva discover skipping bind={} step=set_broadcast kind={:?} err={}",
                    bind_addr,
                    err.kind(),
                    err
                );
            }
            last_io_error = Some(err);
            continue;
        }

        join_multicast_any(&std_sock, *bind_ip);

        let reply_addr = ip_to_bytes(*bind_ip);
        let reply_port = match std_sock.local_addr() {
            Ok(addr) => addr.port(),
            Err(err) => {
                if debug_enabled {
                    debug!(
                        "pva discover skipping bind={} step=local_addr kind={:?} err={}",
                        bind_addr,
                        err.kind(),
                        err
                    );
                }
                last_io_error = Some(err);
                continue;
            }
        };
        let msg = encode_search_request(seq, reply_port, reply_addr, &[], 2, false);

        let socket = match UdpSocket::from_std(std_sock) {
            Ok(socket) => socket,
            Err(err) => {
                if debug_enabled {
                    debug!(
                        "pva discover skipping bind={} step=from_std kind={:?} err={}",
                        bind_addr,
                        err.kind(),
                        err
                    );
                }
                last_io_error = Some(err);
                continue;
            }
        };

        let dests: Vec<SocketAddr> = group_targets
            .iter()
            .map(|ip| SocketAddr::new(*ip, udp_port))
            .collect();

        // Send to every target in this bind group immediately.
        for dest in &dests {
            if debug_enabled {
                debug!(
                    "pva discover bind={} target={} server_port={} reply_port={} seq={}",
                    actual_bind_addr, dest.ip(), udp_port, reply_port, seq
                );
            }
            if let Err(err) = socket.send_to(&msg, dest).await {
                if debug_enabled {
                    debug!(
                        "pva discover send_to target={} kind={:?} err={}",
                        dest,
                        err.kind(),
                        err
                    );
                }
                last_io_error = Some(err);
            }
        }

        socket_info.push((Arc::new(socket), msg, dests));
    }

    if socket_info.is_empty() {
        if let Some(err) = last_io_error {
            return Err(PvGetError::Io(err));
        }
        return Err(PvGetError::Search("no search targets"));
    }

    // Spawn a receiver task per socket that forwards packets into a shared channel.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(Vec<u8>, SocketAddr)>(64);
    for (sock, _, _) in &socket_info {
        let sock = Arc::clone(sock);
        let tx = tx.clone();
        tokio::spawn(async move {
            loop {
                let mut buf = vec![0u8; 2048];
                match sock.recv_from(&mut buf).await {
                    Ok((len, src)) => {
                        buf.truncate(len);
                        if tx.send((buf, src)).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
    }
    drop(tx); // Only spawned tasks hold senders; channel closes when they exit.

    // Retransmit schedule: exponential backoff from start.
    let retransmit_offsets = [100u64, 500, 1000, 2000];
    let start = tokio::time::Instant::now();
    let mut next_retransmit = 0usize;

    loop {
        // Compute the next wake-up: either the next retransmit or the deadline.
        let next_retransmit_at = if next_retransmit < retransmit_offsets.len() {
            start + Duration::from_millis(retransmit_offsets[next_retransmit])
        } else {
            deadline
        };
        let wake_at = next_retransmit_at.min(deadline);

        tokio::select! {
            recv = rx.recv() => {
                let Some((buf, src)) = recv else { break };
                let mut pkt = PvaPacket::new(&buf);
                let Some(cmd) = pkt.decode_payload() else {
                    continue;
                };
                if let PvaPacketCommand::SearchResponse(payload) = cmd {
                    if payload.seq != seq {
                        continue;
                    }
                    if !payload.protocol.is_empty() && !payload.protocol.eq_ignore_ascii_case("tcp") {
                        continue;
                    }
                    let tcp_addr = decode_search_response_addr(payload.addr, payload.port, src);
                    found.push(DiscoveredServer {
                        guid: payload.guid,
                        tcp_addr,
                    });
                }
            }
            _ = tokio::time::sleep_until(wake_at) => {
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                // Retransmit to all targets on all sockets.
                if next_retransmit < retransmit_offsets.len() {
                    if debug_enabled {
                        debug!("pva discover retransmit round {}", next_retransmit + 1);
                    }
                    for (sock, msg, dests) in &socket_info {
                        for dest in dests {
                            let _ = sock.send_to(msg, dest).await;
                        }
                    }
                    next_retransmit += 1;
                }
            }
        }
    }

    Ok(normalize_discovered_servers(found))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pva_codec::epics_decode::{PvaPacket, PvaPacketCommand};

    #[test]
    fn encode_decode_search_request_roundtrip() {
        let seq = 1234;
        let cid = 42;
        let port = 5076;
        let pv_name = "TEST:PV";
        let reply_addr = ip_to_bytes(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)));
        let requests = [(cid, pv_name)];
        let msg = encode_search_request(seq, port, reply_addr, &requests, 2, false);
        let mut pkt = PvaPacket::new(&msg);
        let cmd = pkt.decode_payload().expect("decoded");
        match cmd {
            PvaPacketCommand::Search(payload) => {
                assert_eq!(payload.seq, seq);
                assert_eq!(payload.mask, 0x81);
                assert_eq!(payload.addr, reply_addr);
                assert_eq!(payload.port, port);
                assert_eq!(payload.protocols, vec!["tcp".to_string()]);
                assert_eq!(payload.pv_requests.len(), 1);
                assert_eq!(payload.pv_requests[0].0, cid);
                assert_eq!(payload.pv_requests[0].1, pv_name.to_string());
            }
            other => panic!("unexpected decode: {:?}", other),
        }
    }

    #[test]
    fn encode_decode_server_discovery_request_roundtrip() {
        let seq = 4321;
        let port = 5076;
        let reply_addr = ip_to_bytes(IpAddr::V4(Ipv4Addr::new(10, 20, 30, 40)));
        let msg = encode_search_request(seq, port, reply_addr, &[], 2, false);
        let mut pkt = PvaPacket::new(&msg);
        let cmd = pkt.decode_payload().expect("decoded");
        match cmd {
            PvaPacketCommand::Search(payload) => {
                assert_eq!(payload.seq, seq);
                assert_eq!(payload.pv_requests.len(), 0);
                assert_eq!(payload.protocols, vec!["tcp".to_string()]);
            }
            other => panic!("unexpected decode: {:?}", other),
        }
    }

    #[test]
    fn normalize_discovered_servers_deduplicates_by_guid_and_addr() {
        let guid = [1u8; 12];
        let s1 = DiscoveredServer {
            guid,
            tcp_addr: "127.0.0.1:5075".parse().unwrap(),
        };
        let s2 = DiscoveredServer {
            guid,
            tcp_addr: "127.0.0.1:5075".parse().unwrap(),
        };
        let s3 = DiscoveredServer {
            guid: [2u8; 12],
            tcp_addr: "127.0.0.1:5075".parse().unwrap(),
        };
        let normalized = normalize_discovered_servers(vec![s1, s2, s3]);
        assert_eq!(normalized.len(), 2);
    }

    #[test]
    fn parse_addr_list_accepts_ip_and_ip_port() {
        let items = parse_addr_list("192.168.1.10 10.0.0.1:5076");
        assert!(items.contains(&IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10))));
        assert!(items.contains(&IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
    }

    #[test]
    fn discovery_target_falls_back_to_limited_broadcast_for_invalid_netmask() {
        let ip = Ipv4Addr::new(130, 246, 90, 92);
        assert_eq!(
            discovery_target_for(ip, Ipv4Addr::new(255, 255, 255, 255)),
            Ipv4Addr::new(255, 255, 255, 255)
        );
        assert_eq!(
            discovery_target_for(ip, Ipv4Addr::new(0, 0, 0, 0)),
            Ipv4Addr::new(255, 255, 255, 255)
        );
    }

    #[test]
    fn discovery_target_uses_directed_broadcast_for_normal_subnet() {
        let ip = Ipv4Addr::new(192, 168, 56, 1);
        let netmask = Ipv4Addr::new(255, 255, 255, 0);
        assert_eq!(
            discovery_target_for(ip, netmask),
            Ipv4Addr::new(192, 168, 56, 255)
        );
    }

    #[test]
    fn parse_name_servers_ip_with_port() {
        let addrs = parse_name_servers("192.168.1.10:5075");
        assert_eq!(addrs, vec!["192.168.1.10:5075".parse::<SocketAddr>().unwrap()]);
    }

    #[test]
    fn parse_name_servers_ip_without_port_defaults_to_5075() {
        let addrs = parse_name_servers("10.0.0.1");
        assert_eq!(addrs, vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5075)]);
    }

    #[test]
    fn parse_name_servers_multiple_comma_separated() {
        let addrs = parse_name_servers("10.0.0.1:5075,10.0.0.2:9876");
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs[0], "10.0.0.1:5075".parse::<SocketAddr>().unwrap());
        assert_eq!(addrs[1], "10.0.0.2:9876".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn parse_name_servers_multiple_space_separated() {
        let addrs = parse_name_servers("10.0.0.1 10.0.0.2:5075");
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs[0], SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5075));
        assert_eq!(addrs[1], "10.0.0.2:5075".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn parse_name_servers_empty_string() {
        let addrs = parse_name_servers("");
        assert!(addrs.is_empty());
    }

    #[test]
    fn parse_name_servers_whitespace_only() {
        let addrs = parse_name_servers("  \t  ");
        assert!(addrs.is_empty());
    }

    #[test]
    fn parse_name_servers_mixed_separators() {
        let addrs = parse_name_servers("10.0.0.1:5075, 10.0.0.2  ,  10.0.0.3:9999");
        assert_eq!(addrs.len(), 3);
        assert_eq!(addrs[0], "10.0.0.1:5075".parse::<SocketAddr>().unwrap());
        assert_eq!(addrs[1], SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 5075));
        assert_eq!(addrs[2], "10.0.0.3:9999".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn parse_name_servers_ipv6_with_port() {
        let addrs = parse_name_servers("[::1]:5075");
        assert_eq!(addrs, vec![SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 5075)]);
    }

    #[test]
    fn parse_name_servers_ipv6_without_port() {
        let addrs = parse_name_servers("::1");
        assert_eq!(addrs, vec![SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 5075)]);
    }
}
