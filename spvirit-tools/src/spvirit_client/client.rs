use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::spvirit_client::auth::{resolved_authnz_host, resolved_authnz_user};
use crate::spvirit_client::search::resolve_pv_server;
use crate::spvirit_client::transport::{read_packet, read_until};
use crate::spvirit_client::types::{PvGetError, PvGetOptions, PvGetResult};
use spvirit_codec::epics_decode::{
    decode_op_response_status as codec_decode_op_response_status, decode_status, PvaPacket,
    PvaPacketCommand, PvaStatus,
};
pub use spvirit_codec::spvirit_encode::{
    encode_create_channel_request, encode_get_field_request, encode_get_request,
    encode_monitor_request, encode_put_request,
};
use spvirit_codec::spvirit_encode::encode_client_connection_validation;

pub fn build_client_validation(
    opts: &crate::spvirit_client::types::PvGetOptions,
    version: u8,
    is_be: bool,
) -> Vec<u8> {
    let user = resolved_authnz_user(opts);
    let host = resolved_authnz_host(opts);
    encode_client_connection_validation(87_040, 32_767, 0, "ca", &user, &host, version, is_be)
}

pub fn decode_put_status(raw: &[u8], is_be: bool) -> Option<PvaStatus> {
    decode_status(raw, is_be).0
}

pub fn op_response_status(raw: &[u8], is_be: bool) -> Result<Option<PvaStatus>, PvGetError> {
    codec_decode_op_response_status(raw, is_be).map_err(PvGetError::Protocol)
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
    status.is_some_and(PvaStatus::is_error)
}

pub fn format_pva_status(status: &PvaStatus) -> String {
    status.to_string()
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
