use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{interval, timeout};

use crate::spvirit_client::client::{
    build_client_validation, encode_create_channel_request, encode_get_field_request,
    encode_get_request, encode_monitor_request, format_pva_status, is_pva_status_error, pvget,
};
use crate::spvirit_client::transport::read_packet;
use crate::spvirit_client::types::{PvGetError, PvGetOptions, PvGetResult};
use spvirit_codec::epics_decode::PvaPacketCommand;
use spvirit_codec::spvirit_encode::{encode_control_message, encode_header};
use spvirit_codec::spvd_decode::{
    extract_nt_scalar_value, DecodedValue, FieldDesc, FieldType, PvdDecoder, StructureDesc,
};
use spvirit_codec::spvd_encode::{encode_string_pvd, encode_structure_desc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PvListSource {
    PvList,
    GetField,
    ServerRpc,
    ServerGet,
}

pub fn normalize_pv_names(mut names: Vec<String>) -> Vec<String> {
    names.retain(|name| !name.trim().is_empty());
    names.sort();
    names.dedup();
    names
}

const PV_REQUEST_EMPTY: [u8; 6] = [0xfd, 0x02, 0x00, 0x80, 0x00, 0x00];

fn is_get_field_fallback_enabled() -> bool {
    match std::env::var("EPICS_PVA_ENABLE_GET_FIELD_FALLBACK") {
        Ok(v) => {
            let v = v.trim().to_ascii_uppercase();
            v == "YES" || v == "Y" || v == "1" || v == "TRUE"
        }
        Err(_) => false,
    }
}

pub fn parse_pvlist_value(value: &DecodedValue) -> Option<Vec<String>> {
    let root = extract_nt_scalar_value(value).unwrap_or(value);
    let DecodedValue::Array(items) = root else {
        return None;
    };

    let mut out = Vec::with_capacity(items.len());
    for item in items {
        if let DecodedValue::String(name) = item {
            out.push(name.clone());
        } else {
            return None;
        }
    }
    Some(out)
}

fn candidate_server_addrs(opts: &PvGetOptions, server_addr: SocketAddr) -> Vec<SocketAddr> {
    let mut out = vec![server_addr];
    let default_addr = SocketAddr::new(server_addr.ip(), opts.tcp_port);
    if default_addr != server_addr {
        out.push(default_addr);
    }
    out
}

fn encode_rpc_request(
    sid: u32,
    ioid: u32,
    subcmd: u8,
    extra: &[u8],
    version: u8,
    is_be: bool,
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&if is_be {
        sid.to_be_bytes()
    } else {
        sid.to_le_bytes()
    });
    payload.extend_from_slice(&if is_be {
        ioid.to_be_bytes()
    } else {
        ioid.to_le_bytes()
    });
    payload.push(subcmd);
    payload.extend_from_slice(extra);
    let mut out = encode_header(false, is_be, false, version, 20, payload.len() as u32);
    out.extend_from_slice(&payload);
    out
}

fn encode_server_rpc_channels_request(is_be: bool) -> Vec<u8> {
    // EPICS pvlist server RPC request payload:
    // NTURI { scheme="pva", path="server", query={ op="channels" } }
    let desc = StructureDesc {
        struct_id: Some("epics:nt/NTURI:1.0".to_string()),
        fields: vec![
            FieldDesc {
                name: "scheme".to_string(),
                field_type: FieldType::String,
            },
            FieldDesc {
                name: "path".to_string(),
                field_type: FieldType::String,
            },
            FieldDesc {
                name: "query".to_string(),
                field_type: FieldType::Structure(StructureDesc {
                    struct_id: None,
                    fields: vec![FieldDesc {
                        name: "op".to_string(),
                        field_type: FieldType::String,
                    }],
                }),
            },
        ],
    };

    let mut out = Vec::new();
    out.push(0x80);
    out.extend_from_slice(&encode_structure_desc(&desc, is_be));
    out.extend_from_slice(&encode_string_pvd("pva", is_be));
    out.extend_from_slice(&encode_string_pvd("server", is_be));
    out.extend_from_slice(&encode_string_pvd("channels", is_be));
    out
}

fn collect_strings_from_decoded(value: &DecodedValue, out: &mut Vec<String>) {
    match value {
        DecodedValue::String(s) => out.push(s.clone()),
        DecodedValue::Array(items) => {
            for item in items {
                collect_strings_from_decoded(item, out);
            }
        }
        DecodedValue::Structure(fields) => {
            for (_, item) in fields {
                collect_strings_from_decoded(item, out);
            }
        }
        _ => {}
    }
}

fn looks_like_pv_name(candidate: &str) -> bool {
    if candidate.is_empty() || candidate.len() > 128 {
        return false;
    }
    if candidate.chars().any(|c| c.is_whitespace()) {
        return false;
    }
    // Remove obvious metadata/type labels often seen in NT structures.
    let lower = candidate.to_ascii_lowercase();
    let deny = [
        "value",
        "alarm",
        "timestamp",
        "display",
        "control",
        "severity",
        "message",
        "seconds",
        "nanoseconds",
        "units",
    ];
    if deny.iter().any(|d| lower == *d) {
        return false;
    }
    if lower.starts_with("epics:") {
        return false;
    }
    true
}

fn extract_ascii_candidates(raw: &[u8], out: &mut Vec<String>) {
    let mut i = 0usize;
    while i < raw.len() {
        if raw[i].is_ascii_alphanumeric() {
            let start = i;
            i += 1;
            while i < raw.len() {
                let b = raw[i];
                if b.is_ascii_alphanumeric()
                    || b == b':'
                    || b == b'.'
                    || b == b'_'
                    || b == b'-'
                    || b == b'/'
                {
                    i += 1;
                } else {
                    break;
                }
            }
            let len = i - start;
            if (3..=128).contains(&len) {
                if let Ok(s) = std::str::from_utf8(&raw[start..start + len]) {
                    out.push(s.to_string());
                }
            }
        } else {
            i += 1;
        }
    }
}



pub fn choose_list_result<F>(
    primary: Result<Vec<String>, PvGetError>,
    fallback: F,
) -> Result<(Vec<String>, PvListSource), PvGetError>
where
    F: FnOnce() -> Result<Vec<String>, PvGetError>,
{
    match primary {
        Ok(names) => Ok((normalize_pv_names(names), PvListSource::PvList)),
        Err(_) => Ok((normalize_pv_names(fallback()?), PvListSource::GetField)),
    }
}

async fn read_until<F>(
    stream: &mut TcpStream,
    timeout_dur: Duration,
    mut predicate: F,
) -> Result<PvaPacketCommand, PvGetError>
where
    F: FnMut(&PvaPacketCommand) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout_dur;
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(PvGetError::Timeout("read_until"));
        }
        let remaining = deadline - now;
        let bytes = read_packet(stream, remaining).await?;
        let mut pkt = spvirit_codec::epics_decode::PvaPacket::new(&bytes);
        if let Some(cmd) = pkt.decode_payload() {
            if predicate(&cmd) {
                return Ok(cmd);
            }
        }
    }
}

async fn list_pvs_via_pvlist(
    opts: &PvGetOptions,
    server_addr: SocketAddr,
) -> Result<Vec<String>, PvGetError> {
    let mut get_opts = opts.clone();
    get_opts.pv_name = "__pvlist".to_string();
    get_opts.server_addr = Some(server_addr);
    let result = pvget(&get_opts).await?;
    let names = parse_pvlist_value(&result.value)
        .ok_or_else(|| PvGetError::Decode("failed to decode __pvlist value".to_string()))?;
    Ok(normalize_pv_names(names))
}

pub async fn list_pvs_via_get_field(
    opts: &PvGetOptions,
    server_addr: SocketAddr,
    field_pattern: Option<&str>,
) -> Result<Vec<String>, PvGetError> {
    let mut stream = timeout(opts.timeout, TcpStream::connect(server_addr))
        .await
        .map_err(|_| PvGetError::Timeout("connect"))??;

    let mut version = 2u8;
    let mut is_be = false;

    // Read initial server messages: SET_BYTE_ORDER and server validation.
    for _ in 0..2 {
        if let Ok(bytes) = read_packet(&mut stream, opts.timeout).await {
            let mut pkt = spvirit_codec::epics_decode::PvaPacket::new(&bytes);
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

    let validation = build_client_validation(opts, version, is_be);
    stream.write_all(&validation).await?;

    let _ = read_until(&mut stream, opts.timeout, |cmd| {
        matches!(cmd, PvaPacketCommand::ConnectionValidated(_))
    })
    .await?;

    let get_field = encode_get_field_request(0, field_pattern, version, is_be);
    stream.write_all(&get_field).await?;

    let cmd = read_until(
        &mut stream,
        opts.timeout,
        |cmd| matches!(cmd, PvaPacketCommand::GetField(payload) if payload.is_server),
    )
    .await?;

    let PvaPacketCommand::GetField(payload) = cmd else {
        return Err(PvGetError::Protocol(
            "unexpected GET_FIELD response".to_string(),
        ));
    };

    if is_pva_status_error(payload.status.as_ref()) {
        let detail = payload
            .status
            .as_ref()
            .map(format_pva_status)
            .unwrap_or_default();
        return Err(PvGetError::Protocol(format!(
            "get_field listing error: {}",
            detail
        )));
    }

    let desc = payload
        .introspection
        .ok_or_else(|| PvGetError::Decode("missing GET_FIELD introspection".to_string()))?;

    let names = desc.fields.into_iter().map(|f| f.name).collect::<Vec<_>>();
    Ok(normalize_pv_names(names))
}

async fn list_pvs_via_server_rpc_channel(
    opts: &PvGetOptions,
    server_addr: SocketAddr,
    rpc_channel: &str,
) -> Result<Vec<String>, PvGetError> {
    let mut stream = timeout(opts.timeout, TcpStream::connect(server_addr))
        .await
        .map_err(|_| PvGetError::Timeout("connect"))??;

    let mut version = 2u8;
    let mut is_be = false;

    // Read initial server messages: SET_BYTE_ORDER and server validation.
    for _ in 0..2 {
        if let Ok(bytes) = read_packet(&mut stream, opts.timeout).await {
            let mut pkt = spvirit_codec::epics_decode::PvaPacket::new(&bytes);
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

    let validation = build_client_validation(opts, version, is_be);
    stream.write_all(&validation).await?;
    let _ = read_until(&mut stream, opts.timeout, |cmd| {
        matches!(cmd, PvaPacketCommand::ConnectionValidated(_))
    })
    .await?;

    let cid = 1u32;
    let create = encode_create_channel_request(cid, rpc_channel, version, is_be);
    stream.write_all(&create).await?;
    let create_cmd = read_until(&mut stream, opts.timeout, |cmd| {
        matches!(cmd, PvaPacketCommand::CreateChannel(_))
    })
    .await?;
    let sid = match create_cmd {
        PvaPacketCommand::CreateChannel(payload) => {
            if is_pva_status_error(payload.status.as_ref()) {
                let detail = payload
                    .status
                    .as_ref()
                    .map(format_pva_status)
                    .unwrap_or_default();
                return Err(PvGetError::Protocol(format!(
                    "server RPC channel '{}' create failed: {}",
                    rpc_channel, detail
                )));
            }
            payload.sid
        }
        _ => {
            return Err(PvGetError::Protocol(
                "unexpected create_channel response".to_string(),
            ));
        }
    };

    let ioid = 1u32;
    // Use a standard RPC init->execute flow for better interoperability.
    let rpc_init = encode_rpc_request(sid, ioid, 0x08, &PV_REQUEST_EMPTY, version, is_be);
    stream.write_all(&rpc_init).await?;

    let init_cmd = read_until(&mut stream, opts.timeout, |cmd| match cmd {
        PvaPacketCommand::Op(op) => op.command == 20 && op.ioid == ioid && (op.subcmd & 0x08) != 0,
        _ => false,
    })
    .await?;
    if let PvaPacketCommand::Op(op) = init_cmd {
        if is_pva_status_error(op.status.as_ref()) {
            let detail = op
                .status
                .as_ref()
                .map(format_pva_status)
                .unwrap_or_default();
            return Err(PvGetError::Protocol(format!("rpc init failed: {}", detail)));
        }
    }

    let rpc_payload = encode_server_rpc_channels_request(is_be);
    let rpc_req = encode_rpc_request(sid, ioid, 0x00, &rpc_payload, version, is_be);
    stream.write_all(&rpc_req).await?;

    let rpc_cmd = read_until(&mut stream, opts.timeout, |cmd| match cmd {
        PvaPacketCommand::Op(op) => op.command == 20 && op.ioid == ioid && op.subcmd == 0x00,
        _ => false,
    })
    .await?;

    let PvaPacketCommand::Op(op) = rpc_cmd else {
        return Err(PvGetError::Protocol("unexpected RPC response".to_string()));
    };
    if is_pva_status_error(op.status.as_ref()) {
        let detail = op
            .status
            .as_ref()
            .map(format_pva_status)
            .unwrap_or_default();
        return Err(PvGetError::Protocol(format!(
            "rpc execute failed: {}",
            detail
        )));
    }

    if op.body.is_empty() {
        return Err(PvGetError::Decode("empty RPC response".to_string()));
    }

    // Typical RPC payload starts with full introspection followed by value.
    let decoder = PvdDecoder::new(is_be);
    let (desc, consumed) = decoder
        .parse_introspection_with_len(&op.body)
        .ok_or_else(|| PvGetError::Decode("RPC missing introspection".to_string()))?;
    let value_raw = op
        .body
        .get(consumed..)
        .ok_or_else(|| PvGetError::Decode("RPC malformed payload".to_string()))?;
    let (decoded, _) = decoder
        .decode_structure(value_raw, &desc)
        .ok_or_else(|| PvGetError::Decode("RPC decode failed".to_string()))?;

    let mut strings = Vec::new();
    collect_strings_from_decoded(&decoded, &mut strings);
    if strings.is_empty() {
        return Err(PvGetError::Decode(
            "RPC list returned no strings".to_string(),
        ));
    }
    Ok(normalize_pv_names(strings))
}

pub async fn list_pvs_via_server_rpc(
    opts: &PvGetOptions,
    server_addr: SocketAddr,
) -> Result<Vec<String>, PvGetError> {
    let mut errs = Vec::new();
    for channel in ["server", "__server"] {
        match list_pvs_via_server_rpc_channel(opts, server_addr, channel).await {
            Ok(names) => return Ok(names),
            Err(err) => errs.push(format!("{}: {}", channel, err)),
        }
    }
    Err(PvGetError::Protocol(format!(
        "server RPC unavailable: {}",
        errs.join(" | ")
    )))
}

pub async fn list_pvs_via_server_get(
    opts: &PvGetOptions,
    server_addr: SocketAddr,
) -> Result<Vec<String>, PvGetError> {
    async fn list_pvs_via_server_get_channel(
        opts: &PvGetOptions,
        server_addr: SocketAddr,
        channel: &str,
    ) -> Result<Vec<String>, PvGetError> {
        let mut stream = timeout(opts.timeout, TcpStream::connect(server_addr))
            .await
            .map_err(|_| PvGetError::Timeout("connect"))??;

        let mut version = 2u8;
        let mut is_be = false;
        for _ in 0..2 {
            if let Ok(bytes) = read_packet(&mut stream, opts.timeout).await {
                let mut pkt = spvirit_codec::epics_decode::PvaPacket::new(&bytes);
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

        let validation = build_client_validation(opts, version, is_be);
        stream.write_all(&validation).await?;
        let _ = read_until(&mut stream, opts.timeout, |cmd| {
            matches!(cmd, PvaPacketCommand::ConnectionValidated(_))
        })
        .await?;

        let cid = 1u32;
        let create = encode_create_channel_request(cid, channel, version, is_be);
        stream.write_all(&create).await?;
        let create_cmd = read_until(&mut stream, opts.timeout, |cmd| {
            matches!(cmd, PvaPacketCommand::CreateChannel(_))
        })
        .await?;
        let sid = match create_cmd {
            PvaPacketCommand::CreateChannel(payload) => {
                if is_pva_status_error(payload.status.as_ref()) {
                    let detail = payload
                        .status
                        .as_ref()
                        .map(format_pva_status)
                        .unwrap_or_default();
                    return Err(PvGetError::Protocol(format!(
                        "server GET channel '{}' create failed: {}",
                        channel, detail
                    )));
                }
                payload.sid
            }
            _ => {
                return Err(PvGetError::Protocol(
                    "unexpected create_channel response".to_string(),
                ));
            }
        };

        let ioid = 1u32;
        let init_req = encode_get_request(sid, ioid, 0x08, &PV_REQUEST_EMPTY, version, is_be);
        stream.write_all(&init_req).await?;
        let init_cmd = read_until(&mut stream, opts.timeout, |cmd| match cmd {
            PvaPacketCommand::Op(op) => {
                op.command == 10 && op.ioid == ioid && (op.subcmd & 0x08) != 0
            }
            _ => false,
        })
        .await?;
        let init_desc = match init_cmd {
            PvaPacketCommand::Op(op) => {
                if is_pva_status_error(op.status.as_ref()) {
                    let detail = op
                        .status
                        .as_ref()
                        .map(format_pva_status)
                        .unwrap_or_default();
                    return Err(PvGetError::Protocol(format!(
                        "server GET init failed: {}",
                        detail
                    )));
                }
                op.introspection
            }
            _ => None,
        };

        let data_req = encode_get_request(sid, ioid, 0x00, &[], version, is_be);
        stream.write_all(&data_req).await?;
        let data_cmd = read_until(&mut stream, opts.timeout, |cmd| match cmd {
            PvaPacketCommand::Op(op) => op.command == 10 && op.ioid == ioid && op.subcmd == 0x00,
            _ => false,
        })
        .await?;

        let mut names = Vec::new();
        let PvaPacketCommand::Op(mut op) = data_cmd else {
            return Err(PvGetError::Protocol(
                "unexpected GET data response".to_string(),
            ));
        };
        if is_pva_status_error(op.status.as_ref()) {
            let detail = op
                .status
                .as_ref()
                .map(format_pva_status)
                .unwrap_or_default();
            return Err(PvGetError::Protocol(format!(
                "server GET data failed: {}",
                detail
            )));
        }

        names.extend(op.pv_names.clone());
        extract_ascii_candidates(&op.body, &mut names);
        if let Some(desc) = &init_desc {
            for field in &desc.fields {
                names.push(field.name.clone());
            }
            op.decode_with_field_desc(desc, is_be);
            if let Some(decoded) = &op.decoded_value {
                collect_strings_from_decoded(decoded, &mut names);
            }
        }

        let mut names = normalize_pv_names(names);
        names.retain(|n| looks_like_pv_name(n));
        if names.is_empty() {
            return Err(PvGetError::Decode(
                "server GET returned no PV-like names".to_string(),
            ));
        }
        Ok(names)
    }

    let mut errs = Vec::new();
    for channel in ["server", "__server"] {
        match list_pvs_via_server_get_channel(opts, server_addr, channel).await {
            Ok(names) => return Ok(names),
            Err(err) => errs.push(format!("{}: {}", channel, err)),
        }
    }
    Err(PvGetError::Protocol(format!(
        "server GET unavailable: {}",
        errs.join(" | ")
    )))
}

pub async fn list_pvs_with_fallback(
    opts: &PvGetOptions,
    server_addr: SocketAddr,
) -> Result<(Vec<String>, PvListSource), PvGetError> {
    list_pvs_with_fallback_progress(opts, server_addr, |_| {}).await
}

pub async fn list_pvs_with_fallback_progress<F>(
    opts: &PvGetOptions,
    server_addr: SocketAddr,
    mut on_progress: F,
) -> Result<(Vec<String>, PvListSource), PvGetError>
where
    F: FnMut(&str),
{
    let addrs = candidate_server_addrs(opts, server_addr);
    let mut attempts = Vec::new();
    let get_field_fallback = is_get_field_fallback_enabled();

    if addrs.len() > 1 {
        on_progress(&format!(
            "Trying {} candidate server endpoints...",
            addrs.len()
        ));
    }
    if !get_field_fallback {
        on_progress(
            "GET_FIELD fallback is disabled by default (set EPICS_PVA_ENABLE_GET_FIELD_FALLBACK=YES to enable)",
        );
    }

    for addr in addrs {
        on_progress(&format!("Trying __pvlist on {}", addr));
        let primary = list_pvs_via_pvlist(opts, addr).await;
        match primary {
            Ok(names) => return Ok((normalize_pv_names(names), PvListSource::PvList)),
            Err(primary_err) => {
                let get_field_result = if get_field_fallback {
                    // Try GET_FIELD with "*" first, then with an empty field path.
                    on_progress(&format!(
                        "__pvlist unavailable on {}; trying GET_FIELD(*)",
                        addr
                    ));
                    let fallback_star = list_pvs_via_get_field(opts, addr, Some("*")).await;
                    match fallback_star {
                        Ok(names) => {
                            return Ok((normalize_pv_names(names), PvListSource::GetField));
                        }
                        Err(star_err) => {
                            on_progress(&format!(
                                "GET_FIELD(*) unavailable on {}; trying GET_FIELD(<empty>)",
                                addr
                            ));
                            let fallback_empty = list_pvs_via_get_field(opts, addr, None).await;
                            match fallback_empty {
                                Ok(names) => {
                                    return Ok((normalize_pv_names(names), PvListSource::GetField));
                                }
                                Err(empty_err) => Some(format!(
                                    "GET_FIELD(*): {}; GET_FIELD(<empty>): {}",
                                    star_err, empty_err
                                )),
                            }
                        }
                    }
                } else {
                    None
                };

                on_progress(&format!(
                    "__pvlist unavailable on {}; trying RPC(server)",
                    addr
                ));
                match list_pvs_via_server_rpc(opts, addr).await {
                    Ok(names) => return Ok((normalize_pv_names(names), PvListSource::ServerRpc)),
                    Err(rpc_err) => {
                        on_progress(&format!(
                            "RPC(server) unavailable on {}; trying GET(server)",
                            addr
                        ));
                        match list_pvs_via_server_get(opts, addr).await {
                            Ok(names) => {
                                return Ok((normalize_pv_names(names), PvListSource::ServerGet))
                            }
                            Err(get_err) => {
                                let get_field_msg = get_field_result
                                    .unwrap_or_else(|| "GET_FIELD: disabled".to_string());
                                attempts.push(format!(
                                    "{} => __pvlist: {}; {}; RPC(server): {}; GET(server): {}",
                                    addr, primary_err, get_field_msg, rpc_err, get_err
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    Err(PvGetError::Protocol(format!(
        "failed to list PVs from {}: {}",
        server_addr,
        attempts.join(" | ")
    )))
}

pub async fn fetch_snapshot_from_server(
    opts: &PvGetOptions,
    server_addr: SocketAddr,
    pv_name: &str,
) -> Result<PvGetResult, PvGetError> {
    let addrs = candidate_server_addrs(opts, server_addr);
    let mut errs = Vec::new();

    for addr in addrs {
        let mut get_opts = opts.clone();
        get_opts.server_addr = Some(addr);
        get_opts.pv_name = pv_name.to_string();
        match pvget(&get_opts).await {
            Ok(result) => return Ok(result),
            Err(err) => errs.push(format!("{} => {}", addr, err)),
        }
    }

    Err(PvGetError::Protocol(format!(
        "failed to fetch '{}' from {}: {}",
        pv_name,
        server_addr,
        errs.join(" | ")
    )))
}

async fn monitor_pv_at_addr<F, P>(
    opts: &PvGetOptions,
    server_addr: SocketAddr,
    pv_name: &str,
    on_update: &mut F,
    on_progress: &mut P,
) -> Result<(), PvGetError>
where
    F: FnMut(PvGetResult),
    P: FnMut(&str),
{
    on_progress(&format!("Connecting monitor to {}...", server_addr));
    let mut stream = timeout(opts.timeout, TcpStream::connect(server_addr))
        .await
        .map_err(|_| PvGetError::Timeout("connect"))??;

    let mut version = 2u8;
    let mut is_be = false;

    // Read initial server messages: SET_BYTE_ORDER and server validation.
    for _ in 0..2 {
        if let Ok(bytes) = read_packet(&mut stream, opts.timeout).await {
            let mut pkt = spvirit_codec::epics_decode::PvaPacket::new(&bytes);
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

    let validation = build_client_validation(opts, version, is_be);
    stream.write_all(&validation).await?;
    let _ = read_until(&mut stream, opts.timeout, |cmd| {
        matches!(cmd, PvaPacketCommand::ConnectionValidated(_))
    })
    .await?;

    let cid = 1u32;
    let create = encode_create_channel_request(cid, pv_name, version, is_be);
    stream.write_all(&create).await?;
    let create_cmd = read_until(&mut stream, opts.timeout, |cmd| {
        matches!(cmd, PvaPacketCommand::CreateChannel(_))
    })
    .await?;
    let sid = match create_cmd {
        PvaPacketCommand::CreateChannel(payload) => {
            if is_pva_status_error(payload.status.as_ref()) {
                let detail = payload
                    .status
                    .as_ref()
                    .map(format_pva_status)
                    .unwrap_or_default();
                return Err(PvGetError::Protocol(format!(
                    "create_channel error: {}",
                    detail
                )));
            }
            payload.sid
        }
        _ => {
            return Err(PvGetError::Protocol(
                "unexpected create_channel response".to_string(),
            ))
        }
    };

    let ioid = 1u32;
    let mon_init = encode_monitor_request(sid, ioid, 0x08, &PV_REQUEST_EMPTY, version, is_be);
    stream.write_all(&mon_init).await?;
    let init_cmd = read_until(&mut stream, opts.timeout, |cmd| match cmd {
        PvaPacketCommand::Op(op) => op.command == 13 && op.ioid == ioid && (op.subcmd & 0x08) != 0,
        _ => false,
    })
    .await?;
    let desc = match init_cmd {
        PvaPacketCommand::Op(op) => {
            if is_pva_status_error(op.status.as_ref()) {
                let detail = op
                    .status
                    .as_ref()
                    .map(format_pva_status)
                    .unwrap_or_default();
                return Err(PvGetError::Protocol(format!(
                    "monitor init failed: {}",
                    detail
                )));
            }
            op.introspection
                .ok_or_else(|| PvGetError::Decode("missing monitor introspection".to_string()))?
        }
        _ => {
            return Err(PvGetError::Protocol(
                "unexpected monitor init response".to_string(),
            ))
        }
    };

    let mon_start = encode_monitor_request(sid, ioid, 0x44, &[], version, is_be);
    stream.write_all(&mon_start).await?;
    on_progress(&format!(
        "Monitoring {} on {} (streaming updates)...",
        pv_name, server_addr
    ));

    let mut echo_interval = interval(Duration::from_secs(10));
    let mut echo_token: u32 = 1;

    loop {
        tokio::select! {
            _ = echo_interval.tick() => {
                let msg = encode_control_message(false, is_be, version, 3, echo_token);
                echo_token = echo_token.wrapping_add(1);
                let _ = stream.write_all(&msg).await;
            }
            res = read_packet(&mut stream, opts.timeout) => {
                let bytes = match res {
                    Ok(b) => b,
                    Err(PvGetError::Timeout(_)) => continue,
                    Err(e) => return Err(e),
                };
                let mut pkt = spvirit_codec::epics_decode::PvaPacket::new(&bytes);
                let Some(cmd) = pkt.decode_payload() else {
                    continue;
                };
                let PvaPacketCommand::Op(mut op) = cmd else {
                    continue;
                };
                if op.command != 13 || op.ioid != ioid || (op.subcmd != 0x00 && op.subcmd != 0x10) {
                    continue;
                }

                op.decode_with_field_desc(&desc, is_be);
                if let Some(value) = op.decoded_value {
                    on_update(PvGetResult {
                        pv_name: pv_name.to_string(),
                        value,
                        raw_pva: bytes.clone(),
                        raw_pvd: op.body,
                        introspection: desc.clone(),
                    });
                }
                if op.subcmd == 0x10 {
                    return Ok(());
                }
            }
        }
    }
}

pub async fn monitor_pv_from_server<F, P>(
    opts: &PvGetOptions,
    server_addr: SocketAddr,
    pv_name: &str,
    mut on_update: F,
    mut on_progress: P,
) -> Result<(), PvGetError>
where
    F: FnMut(PvGetResult),
    P: FnMut(&str),
{
    let addrs = candidate_server_addrs(opts, server_addr);
    let mut errs = Vec::new();

    for addr in addrs {
        match monitor_pv_at_addr(opts, addr, pv_name, &mut on_update, &mut on_progress).await {
            Ok(()) => return Ok(()),
            Err(err) => errs.push(format!("{} => {}", addr, err)),
        }
    }

    Err(PvGetError::Protocol(format!(
        "failed to monitor '{}' from {}: {}",
        pv_name,
        server_addr,
        errs.join(" | ")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use spvirit_codec::epics_decode::PvaStatus;

    #[test]
    fn parse_pvlist_value_extracts_ntscalararray_strings() {
        let value = DecodedValue::Structure(vec![
            (
                "value".to_string(),
                DecodedValue::Array(vec![
                    DecodedValue::String("SIM:AI".to_string()),
                    DecodedValue::String("SIM:AO".to_string()),
                ]),
            ),
            ("alarm".to_string(), DecodedValue::Structure(vec![])),
        ]);

        let parsed = parse_pvlist_value(&value).expect("parsed");
        assert_eq!(parsed, vec!["SIM:AI".to_string(), "SIM:AO".to_string()]);
    }

    #[test]
    fn choose_list_result_uses_fallback_when_primary_fails() {
        let primary = Err(PvGetError::Search("primary failed"));
        let result = choose_list_result(primary, || Ok(vec!["B".to_string(), "A".to_string()]))
            .expect("fallback result");
        assert_eq!(result.1, PvListSource::GetField);
        assert_eq!(result.0, vec!["A".to_string(), "B".to_string()]);
    }

    #[test]
    fn candidate_server_addrs_adds_default_tcp_port_fallback() {
        let mut opts = PvGetOptions::new(String::new());
        opts.tcp_port = 5075;
        let addr: SocketAddr = "10.0.0.2:6000".parse().unwrap();
        let addrs = candidate_server_addrs(&opts, addr);
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs[0], addr);
        assert_eq!(addrs[1], "10.0.0.2:5075".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn collect_strings_from_decoded_extracts_nested_strings() {
        let value = DecodedValue::Structure(vec![
            ("a".to_string(), DecodedValue::String("ONE".to_string())),
            (
                "b".to_string(),
                DecodedValue::Array(vec![DecodedValue::String("TWO".to_string())]),
            ),
        ]);
        let mut out = Vec::new();
        collect_strings_from_decoded(&value, &mut out);
        assert_eq!(out, vec!["ONE".to_string(), "TWO".to_string()]);
    }

    #[test]
    fn pva_status_code_zero_is_not_an_error() {
        let ok = PvaStatus {
            code: 0,
            message: None,
            stack: None,
        };
        let err = PvaStatus {
            code: 2,
            message: Some("bad".to_string()),
            stack: None,
        };
        assert!(!is_pva_status_error(None));
        assert!(!is_pva_status_error(Some(&ok)));
        assert!(is_pva_status_error(Some(&err)));
    }

    #[test]
    fn extract_ascii_candidates_finds_pv_like_tokens() {
        let raw = b"\x00SIM:AI\x00junk\x00IOC-01:PV1\x00";
        let mut out = Vec::new();
        extract_ascii_candidates(raw, &mut out);
        assert!(out.iter().any(|s| s == "SIM:AI"));
        assert!(out.iter().any(|s| s == "IOC-01:PV1"));
    }

    #[test]
    fn encode_server_rpc_channels_request_uses_nturi_channels() {
        let payload = encode_server_rpc_channels_request(false);
        assert_eq!(payload.first(), Some(&0x80));

        let decoder = PvdDecoder::new(false);
        let (desc, consumed) = decoder
            .parse_introspection_with_len(&payload)
            .expect("introspection");
        assert_eq!(desc.struct_id.as_deref(), Some("epics:nt/NTURI:1.0"));

        let (decoded, _) = decoder
            .decode_structure(&payload[consumed..], &desc)
            .expect("decode payload");
        let DecodedValue::Structure(fields) = decoded else {
            panic!("expected structure");
        };

        let mut scheme = None;
        let mut path = None;
        let mut op = None;
        for (name, value) in fields {
            match (name.as_str(), value) {
                ("scheme", DecodedValue::String(v)) => scheme = Some(v),
                ("path", DecodedValue::String(v)) => path = Some(v),
                ("query", DecodedValue::Structure(query_fields)) => {
                    for (qname, qvalue) in query_fields {
                        if qname == "op" {
                            if let DecodedValue::String(v) = qvalue {
                                op = Some(v);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        assert_eq!(scheme.as_deref(), Some("pva"));
        assert_eq!(path.as_deref(), Some("server"));
        assert_eq!(op.as_deref(), Some("channels"));
    }
}
