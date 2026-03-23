use std::net::SocketAddr;
use argparse::{ArgumentParser, Store, StoreTrue};
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;

use spvirit_tools::spvirit_client::cli::CommonClientArgs;
use spvirit_tools::spvirit_client::client::{
    encode_get_field_request, establish_channel, pvget, ChannelConn,
};
use spvirit_tools::spvirit_client::search::resolve_pv_server;
use spvirit_tools::spvirit_client::transport::read_until;
use spvirit_tools::spvirit_client::types::{PvGetError, PvGetOptions};
use spvirit_codec::epics_decode::{PvaPacket, PvaPacketCommand};
use spvirit_codec::spvirit_encode::encode_header;
use spvirit_codec::spvd_decode::{extract_subfield_desc, format_structure_tree, StructureDesc};

fn encode_get_field_request_without_field_name(
    cid: u32,
    version: u8,
    is_be: bool,
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&if is_be {
        cid.to_be_bytes()
    } else {
        cid.to_le_bytes()
    });
    let mut out = encode_header(false, is_be, false, version, 17, payload.len() as u32);
    out.extend_from_slice(&payload);
    out
}

fn should_retry_get_field_without_name(err: &PvGetError) -> bool {
    match err {
        PvGetError::Io(io) => matches!(
            io.kind(),
            std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
        ),
        PvGetError::Timeout("read_until") => true,
        _ => false,
    }
}

fn should_fallback_to_pvget(err: &PvGetError) -> bool {
    should_retry_get_field_without_name(err)
        || matches!(err, PvGetError::Protocol(msg) if msg.contains("get_field"))
}

async fn pvinfo_once(
    target: SocketAddr,
    opts: &PvGetOptions,
    subfield: &str,
    include_empty_field_name: bool,
) -> Result<StructureDesc, PvGetError> {
    let conn = establish_channel(target, opts).await?;
    let ChannelConn {
        mut stream,
        sid: _,
        version,
        is_be,
    } = conn;
    let cid = 1u32;

    let get_field = if subfield.is_empty() {
        if include_empty_field_name {
            encode_get_field_request(cid, None, version, is_be)
        } else {
            encode_get_field_request_without_field_name(cid, version, is_be)
        }
    } else {
        encode_get_field_request(cid, Some(subfield), version, is_be)
    };
    stream.write_all(&get_field).await?;

    // Read GET_FIELD response
    let field_resp = read_until(&mut stream, opts.timeout, |cmd| {
        matches!(cmd, PvaPacketCommand::GetField(_))
    })
    .await?;
    let mut pkt = PvaPacket::new(&field_resp);
    let cmd = pkt.decode_payload().ok_or(PvGetError::Protocol(
        "get_field response decode failed".to_string(),
    ))?;
    let desc = match cmd {
        PvaPacketCommand::GetField(payload) => payload.introspection.ok_or_else(|| {
            let status_msg = payload
                .status
                .map(|s| {
                    format!(
                        "code={} message={}",
                        s.code,
                        s.message.unwrap_or_default()
                    )
                })
                .unwrap_or_else(|| "unknown error".to_string());
            PvGetError::Protocol(format!("get_field failed: {}", status_msg))
        })?,
        _ => {
            return Err(PvGetError::Protocol(
                "unexpected get_field response".to_string(),
            ))
        }
    };

    Ok(desc)
}

async fn pvinfo(opts: &PvGetOptions, subfield: &str) -> Result<StructureDesc, PvGetError> {
    let target = resolve_pv_server(opts).await?;

    let primary = pvinfo_once(target, opts, subfield, true).await;
    if let Ok(desc) = primary {
        return Ok(desc);
    }

    let primary_err = primary.err().expect("primary error exists");

    if subfield.is_empty() && should_retry_get_field_without_name(&primary_err) {
        match pvinfo_once(target, opts, subfield, false).await {
            Ok(desc) => return Ok(desc),
            Err(retry_err) => {
                if should_fallback_to_pvget(&retry_err) {
                    return pvget(opts)
                        .await
                        .map(|result| result.introspection)
                        .map_err(|get_err| {
                            PvGetError::Protocol(format!(
                                "pvinfo GET_FIELD failed ({}) and pvget fallback failed ({})",
                                retry_err, get_err
                            ))
                        });
                }
                return Err(retry_err);
            }
        }
    }

    if should_fallback_to_pvget(&primary_err) {
        return pvget(opts)
            .await
            .map(|result| result.introspection)
            .map_err(|get_err| {
                PvGetError::Protocol(format!(
                    "pvinfo GET_FIELD failed ({}) and pvget fallback failed ({})",
                    primary_err, get_err
                ))
            });
    }

    Err(primary_err)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut pv_name = String::new();
    let mut subfield = String::new();
    let mut terse = false;
    let mut common = CommonClientArgs::new();

    {
        let mut ap = ArgumentParser::new();
        ap.set_description(
            "PVA pvinfo client - query PV type introspection (field names and types) \
             without fetching values. Uses the CMD_GET_FIELD (0x11) protocol command.",
        );
        ap.refer(&mut pv_name)
            .add_argument("pv", Store, "PV name to inspect");
        ap.refer(&mut subfield).add_option(
            &["--field", "-f"],
            Store,
            "Sub-field name to inspect (dot-separated path, e.g. 'value' or 'alarm.severity')",
        );
        common.add_to_parser(&mut ap);
        ap.refer(&mut terse).add_option(
            &["--terse", "-t"],
            StoreTrue,
            "Print compact one-line type summary instead of tree",
        );
        ap.parse_args_or_exit();
    }

    if pv_name.is_empty() {
        eprintln!("Error: PV name is required");
        std::process::exit(1);
    }

    common.init_tracing();
    let opts = common.into_pv_get_options(pv_name.clone())?;

    let rt = Runtime::new()?;
    let desc = rt.block_on(pvinfo(&opts, &subfield))?;

    // If a sub-field was requested, filter the result client-side
    let display_desc = if !subfield.is_empty() {
        match extract_subfield_desc(&desc, &subfield) {
            Some(sub) => sub,
            None => {
                // The server may have already filtered, so show what we got
                desc
            }
        }
    } else {
        desc
    };

    if terse {
        use spvirit_codec::spvd_decode::format_structure_desc;
        println!("{}: {}", pv_name, format_structure_desc(&display_desc));
    } else {
        println!("{}:", pv_name);
        println!("{}", format_structure_tree(&display_desc));
    }

    Ok(())
}
