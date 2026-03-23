use std::time::Duration;

use argparse::{ArgumentParser, List, StoreTrue};
use hex::encode as hex_encode;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use tokio::time::interval;

use pva_tools::pva_client::cli::CommonClientArgs;
use pva_tools::pva_client::client::{
    encode_monitor_request, establish_channel, ChannelConn,
};
use pva_tools::pva_client::format::{format_output, OutputFormat, RenderOptions};
use pva_tools::pva_client::search::resolve_pv_server;
use pva_tools::pva_client::transport::{read_packet, read_until};
use pva_tools::pva_client::types::{PvGetError, PvGetOptions};
use pva_codec::epics_decode::{PvaPacket, PvaPacketCommand};
use pva_codec::pva_encode::encode_control_message;

async fn pvmonitor(opts: PvGetOptions, raw: bool, json: bool) -> Result<(), PvGetError> {
    let target = resolve_pv_server(&opts).await?;

    let conn = establish_channel(target, &opts).await?;
    let ChannelConn {
        mut stream,
        sid,
        version,
        is_be,
    } = conn;

    let ioid = 1u32;
    let mon_init = encode_monitor_request(
        sid,
        ioid,
        0x08,
        &[0xfd, 0x02, 0x00, 0x80, 0x00, 0x00],
        version,
        is_be,
    );
    stream.write_all(&mon_init).await?;

    let init_resp = read_until(&mut stream, opts.timeout, |cmd| {
        matches!(cmd, PvaPacketCommand::Op(op) if op.command == 13 && (op.subcmd & 0x08) != 0)
    })
    .await?;
    let mut pkt = PvaPacket::new(&init_resp);
    let cmd = pkt.decode_payload().ok_or(PvGetError::Protocol(
        "monitor init response decode failed".to_string(),
    ))?;

    let desc = match cmd {
        PvaPacketCommand::Op(op) => op
            .introspection
            .ok_or_else(|| PvGetError::Decode("missing introspection".to_string()))?,
        _ => {
            return Err(PvGetError::Protocol(
                "unexpected monitor init response".to_string(),
            ))
        }
    };

    // Subscription is initially Stopped. Send start request (subcommand 0x44).
    let mon_start = encode_monitor_request(sid, ioid, 0x44, &[], version, is_be);
    stream.write_all(&mon_start).await?;

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
                let mut pkt = PvaPacket::new(&bytes);
                let cmd = match pkt.decode_payload() {
                    Some(c) => c,
                    None => continue,
                };
                match cmd {
                    PvaPacketCommand::Op(mut op) => {
                        if op.command != 13 || (op.subcmd != 0x00 && op.subcmd != 0x10) {
                            continue;
                        }
                        op.decode_with_field_desc(&desc, is_be);
                        if let Some(full) = op.decoded_value {
                            let mut render_opts = RenderOptions::default();
                            if json {
                                render_opts.format = OutputFormat::Json;
                            }
                            println!("{}", format_output(&opts.pv_name, &full, &render_opts));
                            if raw {
                                println!("raw_pva: {}", hex_encode(&bytes));
                                println!("raw_pvd: {}", hex_encode(&op.body));
                            }
                        }
                        if op.subcmd == 0x10 {
                            return Ok(());
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut pv_names: Vec<String> = Vec::new();
    let mut raw = false;
    let mut json = false;
    let mut common = CommonClientArgs::new();

    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Minimal PVA pvmonitor client");
        ap.refer(&mut pv_names)
            .add_argument("pv", List, "PV name(s) to monitor");
        common.add_to_parser(&mut ap);
        ap.refer(&mut raw)
            .add_option(&["--raw"], StoreTrue, "Print raw hex payload");
        ap.refer(&mut json)
            .add_option(&["--json"], StoreTrue, "Print JSON output");
        ap.parse_args_or_exit();
    }

    common.init_tracing();

    if pv_names.is_empty() {
        eprintln!("At least one PV name is required");
        std::process::exit(1);
    }

    let base_opts = common.into_pv_get_options(String::new())?;

    let rt = Runtime::new()?;
    rt.block_on(async move {
        let mut handles = Vec::new();
        for pv in pv_names {
            let mut opts = base_opts.clone();
            opts.pv_name = pv.clone();

            let pv_label = pv;
            handles.push(tokio::spawn(async move {
                let res = pvmonitor(opts, raw, json).await;
                (pv_label, res)
            }));
        }

        let mut had_error = false;
        for handle in handles {
            match handle.await {
                Ok((pv, Ok(()))) => {
                    eprintln!("pvmonitor {}: stopped", pv);
                }
                Ok((pv, Err(err))) => {
                    had_error = true;
                    eprintln!("pvmonitor {}: {}", pv, err);
                }
                Err(err) => {
                    had_error = true;
                    eprintln!("pvmonitor task error: {}", err);
                }
            }
        }

        if had_error {
            Err(PvGetError::Protocol("one or more monitors failed".to_string()))
        } else {
            Ok(())
        }
    })?;
    Ok(())
}
