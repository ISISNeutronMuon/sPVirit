use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::types::PvGetError;
use spvirit_codec::epics_decode::{PvaHeader, PvaPacket, PvaPacketCommand};

pub async fn read_packet(
    stream: &mut TcpStream,
    timeout_dur: Duration,
) -> Result<Vec<u8>, PvGetError> {
    let mut header = [0u8; 8];
    timeout(timeout_dur, stream.read_exact(&mut header))
        .await
        .map_err(|_| PvGetError::Timeout("read header"))??;

    let header_parsed = PvaHeader::new(&header);
    let payload_len = if header_parsed.flags.is_control {
        0usize
    } else {
        header_parsed.payload_length as usize
    };

    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        timeout(timeout_dur, stream.read_exact(&mut payload))
            .await
            .map_err(|_| PvGetError::Timeout("read payload"))??;
    }

    let mut full = header.to_vec();
    full.extend_from_slice(&payload);
    Ok(full)
}

pub async fn read_until<F>(
    stream: &mut TcpStream,
    timeout_dur: Duration,
    mut predicate: F,
) -> Result<Vec<u8>, PvGetError>
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
        let mut pkt = PvaPacket::new(&bytes);
        if let Some(cmd) = pkt.decode_payload() {
            if predicate(&cmd) {
                return Ok(bytes);
            }
        }
    }
}
