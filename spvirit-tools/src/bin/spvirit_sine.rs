use std::f64::consts::PI;
use std::time::Duration;

use argparse::{ArgumentParser, Store};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;
use tokio::time::{interval, Instant};

use spvirit_tools::spvirit_client::cli::CommonClientArgs;
use spvirit_tools::spvirit_client::client::{
    decode_put_status, encode_put_request, establish_channel, ChannelConn,
};
use spvirit_tools::spvirit_client::put_encode::encode_put_payload;
use spvirit_tools::spvirit_client::search::resolve_pv_server;
use spvirit_tools::spvirit_client::transport::read_until;
use spvirit_tools::spvirit_client::types::{PvGetError, PvGetOptions};
use spvirit_codec::epics_decode::{PvaPacket, PvaPacketCommand};
use spvirit_codec::spvirit_encode::encode_control_message;

async fn read_packet_stream<R: AsyncReadExt + Unpin>(
    stream: &mut R,
) -> Result<Vec<u8>, PvGetError> {
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await?;
    let header_parsed = spvirit_codec::epics_decode::PvaHeader::new(&header);
    let payload_len = if header_parsed.flags.is_control {
        0usize
    } else {
        header_parsed.payload_length as usize
    };
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload).await?;
    }
    let mut full = header.to_vec();
    full.extend_from_slice(&payload);
    Ok(full)
}

async fn pvsine(
    opts: &PvGetOptions,
    freq_hz: f64,
    rate_hz: f64,
    amp: f64,
    offset: f64,
    phase: f64,
    duration: Option<f64>,
) -> Result<(), PvGetError> {
    if rate_hz <= 0.0 {
        return Err(PvGetError::Protocol("rate must be > 0".to_string()));
    }

    let target = resolve_pv_server(opts).await?;

    let conn = establish_channel(target, opts).await?;
    let ChannelConn {
        mut stream,
        sid,
        version,
        is_be,
    } = conn;

    let ioid = 1u32;
    let put_init = encode_put_request(
        sid,
        ioid,
        0x08,
        &[0xfd, 0x02, 0x00, 0x80, 0x00, 0x00],
        version,
        is_be,
    );
    stream.write_all(&put_init).await?;

    let init_resp = read_until(&mut stream, opts.timeout, |cmd| {
        matches!(cmd, PvaPacketCommand::Op(op) if op.command == 11 && (op.subcmd & 0x08) != 0)
    })
    .await?;
    let mut pkt = PvaPacket::new(&init_resp);
    let cmd = pkt.decode_payload().ok_or(PvGetError::Protocol(
        "put init response decode failed".to_string(),
    ))?;

    let desc = match cmd {
        PvaPacketCommand::Op(op) => {
            if let Some(status) = op.status {
                return Err(PvGetError::Protocol(format!(
                    "put init error: code={} message={}",
                    status.code,
                    status.message.unwrap_or_default()
                )));
            }
            op.introspection
                .ok_or_else(|| PvGetError::Decode("missing pvPutStructureIF".to_string()))?
        }
        _ => {
            return Err(PvGetError::Protocol(
                "unexpected put init response".to_string(),
            ))
        }
    };

    let (mut reader, mut writer) = stream.into_split();
    let is_be_reader = is_be;
    let read_task = tokio::spawn(async move {
        loop {
            let bytes = match read_packet_stream(&mut reader).await {
                Ok(b) => b,
                Err(_) => break,
            };
            let pkt = PvaPacket::new(&bytes);
            let payload_len = pkt.header.payload_length as usize;
            if pkt.header.command == 11 && !pkt.header.flags.is_control {
                if bytes.len() >= 8 + payload_len && payload_len >= 5 {
                    let body = &bytes[8 + 5..8 + payload_len];
                    if let Some(st) = decode_put_status(body, is_be_reader) {
                        if st.code != 0 {
                            let msg = st.message.unwrap_or_else(|| format!("code={}", st.code));
                            eprintln!("put error: {}", msg);
                        }
                    }
                }
            }
        }
    });

    let mut echo_interval = interval(Duration::from_secs(10));
    let mut echo_token: u32 = 1;
    let mut tick = interval(Duration::from_secs_f64(1.0 / rate_hz));
    let start = Instant::now();

    loop {
        tokio::select! {
            _ = echo_interval.tick() => {
                let msg = encode_control_message(false, is_be, version, 3, echo_token);
                echo_token = echo_token.wrapping_add(1);
                let _ = writer.write_all(&msg).await;
            }
            _ = tick.tick() => {
                let t = start.elapsed().as_secs_f64();
                if let Some(dur) = duration {
                    if t >= dur {
                        break;
                    }
                }
                let value = offset + amp * (2.0 * PI * freq_hz * t + phase).sin();
                let input = Value::Number(serde_json::Number::from_f64(value).unwrap());
                let payload = match encode_put_payload(&desc, &input, is_be) {
                    Ok(p) => p,
                    Err(e) => return Err(PvGetError::Protocol(e)),
                };
                let put_req = encode_put_request(sid, ioid, 0x00, &payload, version, is_be);
                writer.write_all(&put_req).await?;
            }
        }
    }

    read_task.abort();
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut pv_name = String::new();
    let mut freq_hz: f64 = 1.0;
    let mut rate_hz: f64 = 10.0;
    let mut amp: f64 = 1.0;
    let mut offset: f64 = 0.0;
    let mut phase: f64 = 0.0;
    let mut duration_secs: f64 = 0.0;
    let mut common = CommonClientArgs::new();

    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Generate a sine wave and pvput it at a fixed rate");
        ap.refer(&mut pv_name)
            .add_argument("pv", Store, "PV name to write");
        ap.refer(&mut freq_hz)
            .add_option(&["--freq"], Store, "Sine frequency (Hz)");
        ap.refer(&mut rate_hz)
            .add_option(&["--rate"], Store, "Update rate (samples/sec)");
        ap.refer(&mut amp)
            .add_option(&["--amp"], Store, "Amplitude");
        ap.refer(&mut offset)
            .add_option(&["--offset"], Store, "Offset");
        ap.refer(&mut phase)
            .add_option(&["--phase"], Store, "Phase (radians)");
        ap.refer(&mut duration_secs).add_option(
            &["--duration"],
            Store,
            "Duration (seconds, 0=run forever)",
        );
        common.add_to_parser(&mut ap);
        ap.parse_args_or_exit();
    }

    common.init_tracing();
    let opts = common.into_pv_get_options(pv_name.clone())?;

    let duration = if duration_secs > 0.0 {
        Some(duration_secs)
    } else {
        None
    };

    let rt = Runtime::new()?;
    let result =
        rt.block_on(
            async move { pvsine(&opts, freq_hz, rate_hz, amp, offset, phase, duration).await },
        );
    match result {
        Ok(()) => Ok(()),
        Err(e) => {
            eprintln!("{} ERROR {}", pv_name, e);
            Err(e.into())
        }
    }
}
