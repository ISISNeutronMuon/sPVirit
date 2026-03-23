use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::spvirit_client::search::resolve_pv_server;
use crate::spvirit_client::transport::{read_packet, read_until};
use crate::spvirit_client::types::{PvGetError, PvGetOptions, PvGetResult};
use spvirit_codec::epics_decode::{decode_string, PvaPacket, PvaPacketCommand, PvaStatus};
use spvirit_codec::spvirit_encode::encode_header;

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

fn encode_authnz_blob(user: &str, host: &str) -> Vec<u8> {
    let mut out = Vec::new();
    // Prefix observed in working pvget capture (AuthNZ descriptor).
    out.extend_from_slice(&[0xFD, 0x01, 0x00, 0x80, 0x00]);
    // Field count = 2, fields: "user" (string), "host" (string).
    out.push(0x02);
    out.push(0x04);
    out.extend_from_slice(b"user");
    out.push(0x60);
    out.push(0x04);
    out.extend_from_slice(b"host");
    out.push(0x60);
    // Field values (strings with 1-byte length).
    let user_bytes = user.as_bytes();
    let host_bytes = host.as_bytes();
    out.push(user_bytes.len() as u8);
    out.extend_from_slice(user_bytes);
    out.push(host_bytes.len() as u8);
    out.extend_from_slice(host_bytes);
    out
}

fn authnz_user_override(opts: &crate::spvirit_client::types::PvGetOptions) -> Option<String> {
    opts.authnz_user.clone()
}

fn authnz_host_override(opts: &crate::spvirit_client::types::PvGetOptions) -> Option<String> {
    opts.authnz_host.clone()
}

fn authnz_user_fallback() -> String {
    std::env::var("PVA_AUTHNZ_USER")
        .or_else(|_| std::env::var("USER"))
        .or_else(|_| std::env::var("LOGNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

fn authnz_host_fallback() -> String {
    std::env::var("PVA_AUTHNZ_HOST")
        .or_else(|_| std::env::var("HOSTNAME"))
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "unknown".to_string())
}

pub fn build_client_validation(
    opts: &crate::spvirit_client::types::PvGetOptions,
    version: u8,
    is_be: bool,
) -> Vec<u8> {
    let user = authnz_user_override(opts).unwrap_or_else(authnz_user_fallback);
    let host = authnz_host_override(opts).unwrap_or_else(authnz_host_fallback);
    encode_connection_validation_client(87_040, 32_767, 0, "ca", &user, &host, version, is_be)
}

fn encode_connection_validation_client(
    buffer_size: u32,
    introspection_registry_size: u16,
    qos: u16,
    authz: &str,
    user: &str,
    host: &str,
    version: u8,
    is_be: bool,
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&if is_be {
        buffer_size.to_be_bytes()
    } else {
        buffer_size.to_le_bytes()
    });
    payload.extend_from_slice(&if is_be {
        introspection_registry_size.to_be_bytes()
    } else {
        introspection_registry_size.to_le_bytes()
    });
    payload.extend_from_slice(&if is_be {
        qos.to_be_bytes()
    } else {
        qos.to_le_bytes()
    });
    // AuthNZ string only (no flags). "ca" length 0x02 is encoded by encode_string.
    payload.extend_from_slice(&encode_string(authz, is_be));
    payload.extend_from_slice(&encode_authnz_blob(user, host));
    let mut out = encode_header(false, is_be, false, version, 1, payload.len() as u32);
    out.extend_from_slice(&payload);
    out
}

pub fn encode_create_channel_request(cid: u32, pv_name: &str, version: u8, is_be: bool) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&if is_be {
        1u16.to_be_bytes()
    } else {
        1u16.to_le_bytes()
    });
    payload.extend_from_slice(&if is_be {
        cid.to_be_bytes()
    } else {
        cid.to_le_bytes()
    });
    payload.extend_from_slice(&encode_string(pv_name, is_be));
    let mut out = encode_header(false, is_be, false, version, 7, payload.len() as u32);
    out.extend_from_slice(&payload);
    out
}

pub fn encode_get_field_request(
    cid: u32,
    field_name: Option<&str>,
    version: u8,
    is_be: bool,
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&if is_be {
        cid.to_be_bytes()
    } else {
        cid.to_le_bytes()
    });
    // Always encode a field-name string (empty string when not provided).
    // Some servers expect this length-prefixed string to be present.
    payload.extend_from_slice(&encode_string(field_name.unwrap_or(""), is_be));
    let mut out = encode_header(false, is_be, false, version, 17, payload.len() as u32);
    out.extend_from_slice(&payload);
    out
}

pub fn encode_get_request(
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
    if !extra.is_empty() {
        payload.extend_from_slice(extra);
    }
    let mut out = encode_header(false, is_be, false, version, 10, payload.len() as u32);
    out.extend_from_slice(&payload);
    out
}

pub fn encode_put_request(
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
    if !extra.is_empty() {
        payload.extend_from_slice(extra);
    }
    let mut out = encode_header(false, is_be, false, version, 11, payload.len() as u32);
    out.extend_from_slice(&payload);
    out
}

pub fn encode_monitor_request(
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
    if !extra.is_empty() {
        payload.extend_from_slice(extra);
    }
    let mut out = encode_header(false, is_be, false, version, 13, payload.len() as u32);
    out.extend_from_slice(&payload);
    out
}

pub fn decode_put_status(raw: &[u8], is_be: bool) -> Option<PvaStatus> {
    if raw.is_empty() {
        return None;
    }
    let code = raw[0];
    if code == 0xFF {
        return None;
    }
    let mut idx = 1usize;
    let mut message: Option<String> = None;
    let mut stack: Option<String> = None;
    if let Some((msg, consumed)) = decode_string(&raw[idx..], is_be) {
        message = Some(msg);
        idx += consumed;
        if let Some((st, consumed2)) = decode_string(&raw[idx..], is_be) {
            stack = Some(st);
            idx += consumed2;
        }
    }
    Some(PvaStatus {
        code,
        message,
        stack,
    })
}

pub fn op_response_status(raw: &[u8], is_be: bool) -> Result<Option<PvaStatus>, PvGetError> {
    let pkt = PvaPacket::new(raw);
    let payload_len = pkt.header.payload_length as usize;
    if raw.len() < 8 + payload_len {
        return Err(PvGetError::Protocol("op response truncated".to_string()));
    }
    let payload = &raw[8..8 + payload_len];
    if payload.len() < 5 {
        return Err(PvGetError::Protocol(
            "op response payload too short".to_string(),
        ));
    }
    let body = &payload[5..];
    Ok(decode_put_status(body, is_be))
}

pub fn ensure_status_ok(raw: &[u8], is_be: bool, step: &str) -> Result<(), PvGetError> {
    match op_response_status(raw, is_be)? {
        None => Ok(()),
        Some(st) if st.code == 0 => Ok(()),
        Some(st) => Err(PvGetError::Protocol(format!(
            "{} failed: {}",
            step,
            st.message.unwrap_or_else(|| format!("code={}", st.code))
        ))),
    }
}

pub fn is_pva_status_error(status: Option<&PvaStatus>) -> bool {
    matches!(status, Some(s) if s.code != 0)
}

pub fn format_pva_status(status: &PvaStatus) -> String {
    format!(
        "code={} message={} stack={}",
        status.code,
        status.message.clone().unwrap_or_default(),
        status.stack.clone().unwrap_or_default()
    )
}

pub struct ChannelConn {
    pub stream: TcpStream,
    pub sid: u32,
    pub version: u8,
    pub is_be: bool,
}

pub async fn establish_channel(
    target: std::net::SocketAddr,
    opts: &PvGetOptions,
) -> Result<ChannelConn, PvGetError> {
    let mut stream = timeout(opts.timeout, TcpStream::connect(target))
        .await
        .map_err(|_| PvGetError::Timeout("connect"))??;

    let mut version = 2u8;
    let mut is_be = false;

    for _ in 0..2 {
        if let Ok(bytes) = read_packet(&mut stream, opts.timeout).await {
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

    let validation = build_client_validation(opts, version, is_be);
    stream.write_all(&validation).await?;

    let _ = read_until(&mut stream, opts.timeout, |cmd| {
        matches!(cmd, PvaPacketCommand::ConnectionValidated(_))
    })
    .await?;

    let cid = 1u32;
    let create = encode_create_channel_request(cid, &opts.pv_name, version, is_be);
    stream.write_all(&create).await?;

    let create_resp = read_until(&mut stream, opts.timeout, |cmd| {
        matches!(cmd, PvaPacketCommand::CreateChannel(_))
    })
    .await?;
    let mut pkt = PvaPacket::new(&create_resp);
    let cmd = pkt.decode_payload().ok_or(PvGetError::Protocol(
        "create_channel decode failed".to_string(),
    ))?;
    let sid = match cmd {
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

    Ok(ChannelConn {
        stream,
        sid,
        version,
        is_be,
    })
}

pub async fn pvget(opts: &PvGetOptions) -> Result<PvGetResult, PvGetError> {
    let target = resolve_pv_server(opts).await?;

    let conn = establish_channel(target, opts).await?;
    let ChannelConn {
        mut stream,
        sid,
        version,
        is_be,
    } = conn;

    let ioid = 1u32;
    // Match EPICS pvget GET init payload (extra pvRequest bytes observed in capture).
    let get_init_req = encode_get_request(
        sid,
        ioid,
        0x08,
        &[0xfd, 0x02, 0x00, 0x80, 0x00, 0x00],
        version,
        is_be,
    );
    stream.write_all(&get_init_req).await?;

    let init_resp = read_until(
        &mut stream,
        opts.timeout,
        |cmd| matches!(cmd, PvaPacketCommand::Op(op) if (op.subcmd & 0x08) != 0),
    )
    .await?;
    let mut pkt = PvaPacket::new(&init_resp);
    let cmd = pkt.decode_payload().ok_or(PvGetError::Protocol(
        "get init response decode failed".to_string(),
    ))?;

    let desc = match cmd {
        PvaPacketCommand::Op(op) => op
            .introspection
            .ok_or_else(|| PvGetError::Decode("missing introspection".to_string()))?,
        _ => {
            return Err(PvGetError::Protocol(
                "unexpected get init response".to_string(),
            ))
        }
    };

    let get_data_req = encode_get_request(sid, ioid, 0x00, &[], version, is_be);
    stream.write_all(&get_data_req).await?;

    let data_resp = read_until(
        &mut stream,
        opts.timeout,
        |cmd| matches!(cmd, PvaPacketCommand::Op(op) if op.subcmd == 0x00),
    )
    .await?;
    let mut pkt = PvaPacket::new(&data_resp);
    let cmd = pkt.decode_payload().ok_or(PvGetError::Protocol(
        "get data response decode failed".to_string(),
    ))?;

    match cmd {
        PvaPacketCommand::Op(mut op) => {
            op.decode_with_field_desc(&desc, is_be);
            if let Some(value) = op.decoded_value {
                return Ok(PvGetResult {
                    pv_name: opts.pv_name.clone(),
                    value,
                    raw_pva: data_resp,
                    raw_pvd: op.body,
                    introspection: desc,
                });
            }
            Err(PvGetError::Decode("no decoded value".to_string()))
        }
        _ => Err(PvGetError::Protocol(
            "unexpected get data response".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spvirit_codec::epics_decode::{PvaPacket, PvaPacketCommand};

    #[test]
    fn encode_decode_monitor_request_roundtrip() {
        let msg =
            encode_monitor_request(1, 2, 0x08, &[0xfd, 0x02, 0x00, 0x80, 0x00, 0x00], 2, false);
        let mut pkt = PvaPacket::new(&msg);
        let cmd = pkt.decode_payload().expect("decoded");
        match cmd {
            PvaPacketCommand::Op(op) => {
                assert_eq!(op.command, 13);
                assert_eq!(op.subcmd, 0x08);
                assert_eq!(op.sid_or_cid, 1);
                assert_eq!(op.ioid, 2);
            }
            other => panic!("unexpected decode: {:?}", other),
        }
    }

    #[test]
    fn encode_decode_put_init_roundtrip() {
        let msg = encode_put_request(5, 6, 0x08, &[0xfd, 0x02, 0x00, 0x80, 0x00, 0x00], 2, false);
        let mut pkt = PvaPacket::new(&msg);
        let cmd = pkt.decode_payload().expect("decoded");
        match cmd {
            PvaPacketCommand::Op(op) => {
                assert_eq!(op.command, 11);
                assert_eq!(op.subcmd, 0x08);
                assert_eq!(op.sid_or_cid, 5);
                assert_eq!(op.ioid, 6);
            }
            other => panic!("unexpected decode: {:?}", other),
        }
    }

    #[test]
    fn encode_decode_get_field_request_roundtrip() {
        let msg = encode_get_field_request(7, Some("*"), 2, false);
        let mut pkt = PvaPacket::new(&msg);
        let cmd = pkt.decode_payload().expect("decoded");
        match cmd {
            PvaPacketCommand::GetField(payload) => {
                assert!(!payload.is_server);
                assert_eq!(payload.cid, 7);
                assert_eq!(payload.field_name.as_deref(), Some("*"));
            }
            other => panic!("unexpected decode: {:?}", other),
        }
    }

    #[test]
    fn encode_decode_get_field_request_empty_field_roundtrip() {
        let msg = encode_get_field_request(7, None, 2, false);
        let mut pkt = PvaPacket::new(&msg);
        let cmd = pkt.decode_payload().expect("decoded");
        match cmd {
            PvaPacketCommand::GetField(payload) => {
                assert!(!payload.is_server);
                assert_eq!(payload.cid, 7);
                assert_eq!(payload.field_name.as_deref(), Some(""));
            }
            other => panic!("unexpected decode: {:?}", other),
        }
    }

    #[test]
    fn pva_status_code_zero_is_not_an_error() {
        let ok = PvaStatus {
            code: 0,
            message: None,
            stack: None,
        };
        let err = PvaStatus {
            code: 1,
            message: Some("bad".to_string()),
            stack: None,
        };
        assert!(!is_pva_status_error(None));
        assert!(!is_pva_status_error(Some(&ok)));
        assert!(is_pva_status_error(Some(&err)));
    }
}
