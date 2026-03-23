mod protocol;

use pva_codec::epics_decode::{PvaPacket, PvaPacketCommand};
use pva_codec::pva_encode::encode_header;
use pva_codec::pvd_decode::PvdDecoder;
use protocol::coverage_matrix::command_coverage;

fn build_frame(
    command: u8,
    payload: &[u8],
    is_server: bool,
    is_be: bool,
    is_control: bool,
) -> Vec<u8> {
    let mut out = encode_header(
        is_server,
        is_be,
        is_control,
        2,
        command,
        payload.len() as u32,
    );
    out.extend_from_slice(payload);
    out
}

fn minimal_payload_for_command(command: u8) -> Vec<u8> {
    match command {
        0 => {
            let mut payload = vec![0u8; 34];
            payload.push(0); // protocol=""
            payload.push(0); // status_if=""
            payload
        }
        1 => vec![0u8; 8],
        2 => vec![],
        3 => {
            let mut payload = vec![0u8; 26];
            payload.push(0); // protocol count
            payload.extend_from_slice(&0u16.to_le_bytes()); // pv count
            payload
        }
        4 => {
            let mut payload = vec![0u8; 34];
            payload.push(0); // protocol=""
            payload
        }
        5 => vec![0],
        6 => vec![0xFF],
        7 => 0u16.to_le_bytes().to_vec(),
        8 => vec![0u8; 8],
        9 => vec![0xFF],
        10 | 11 | 12 | 13 | 14 | 16 | 20 => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&1u32.to_le_bytes()); // sid
            payload.extend_from_slice(&7u32.to_le_bytes()); // ioid
            payload.push(0x00); // subcmd
            payload
        }
        15 => 7u32.to_le_bytes().to_vec(),
        17 => 1u32.to_le_bytes().to_vec(),
        18 => vec![0xFF],
        19 => vec![0], // zero entries
        21 => 7u32.to_le_bytes().to_vec(),
        22 => vec![0u8; 16],
        _ => vec![],
    }
}

#[test]
fn decode_surface_covers_all_protocol_commands() {
    for entry in command_coverage() {
        let payload = minimal_payload_for_command(entry.command);
        let frame = build_frame(entry.command, &payload, false, false, false);
        let mut packet = PvaPacket::new(&frame);
        let decoded = packet.decode_payload();
        assert!(
            decoded.is_some(),
            "decode failed for command {}",
            entry.command
        );

        let decoded = decoded.expect("decoded");
        match (entry.command, decoded) {
            (0, PvaPacketCommand::Beacon(_))
            | (1, PvaPacketCommand::ConnectionValidation(_))
            | (3, PvaPacketCommand::Search(_))
            | (4, PvaPacketCommand::SearchResponse(_))
            | (5, PvaPacketCommand::AuthNZ(_))
            | (6, PvaPacketCommand::AclChange(_))
            | (7, PvaPacketCommand::CreateChannel(_))
            | (8, PvaPacketCommand::DestroyChannel(_))
            | (9, PvaPacketCommand::ConnectionValidated(_))
            | (10, PvaPacketCommand::Op(_))
            | (11, PvaPacketCommand::Op(_))
            | (12, PvaPacketCommand::Op(_))
            | (13, PvaPacketCommand::Op(_))
            | (14, PvaPacketCommand::Op(_))
            | (15, PvaPacketCommand::DestroyRequest(_))
            | (16, PvaPacketCommand::Op(_))
            | (17, PvaPacketCommand::GetField(_))
            | (18, PvaPacketCommand::Message(_))
            | (19, PvaPacketCommand::MultipleData(_))
            | (20, PvaPacketCommand::Op(_))
            | (21, PvaPacketCommand::CancelRequest(_))
            | (22, PvaPacketCommand::OriginTag(_)) => {}
            (2, PvaPacketCommand::Echo(_)) => {}
            (id, other) => panic!("unexpected decoded variant for cmd {}: {:?}", id, other),
        }
    }
}

#[test]
fn malformed_packet_length_is_rejected() {
    let mut frame = encode_header(false, false, false, 2, 7, 16);
    frame.extend_from_slice(&[0u8; 2]);
    let mut packet = PvaPacket::new(&frame);
    assert!(packet.decode_payload().is_none());
}

#[test]
fn malformed_reserved_flag_bits_mark_header_invalid() {
    let mut raw = vec![0xCA, 0x02, 0x0E, 0x07];
    raw.extend_from_slice(&0u32.to_le_bytes());
    let packet = PvaPacket::new(&raw);
    assert!(!packet.is_valid());
}

#[test]
fn malformed_magic_mark_header_invalid() {
    let mut raw = vec![0xCB, 0x02, 0x00, 0x07];
    raw.extend_from_slice(&0u32.to_le_bytes());
    let packet = PvaPacket::new(&raw);
    assert!(!packet.is_valid());
}

#[test]
fn malformed_unknown_pvd_type_decodes_to_none() {
    let decoder = PvdDecoder::new(false);
    let field_desc = [1u8, b'v', 0x06];
    assert!(decoder.parse_field_desc(&field_desc).is_none());
}

#[test]
fn golden_spec_vectors_decode() {
    let vectors: &[(&str, &[u8], u8)] = &[
        (
            "cmd01_connection_validation_le.bin",
            include_bytes!("protocol/spec_vectors/cmd01_connection_validation_le.bin"),
            1,
        ),
        (
            "cmd07_create_channel_req_le.bin",
            include_bytes!("protocol/spec_vectors/cmd07_create_channel_req_le.bin"),
            7,
        ),
        (
            "cmd10_get_init_req_le.bin",
            include_bytes!("protocol/spec_vectors/cmd10_get_init_req_le.bin"),
            10,
        ),
        (
            "cmd11_put_init_req_le.bin",
            include_bytes!("protocol/spec_vectors/cmd11_put_init_req_le.bin"),
            11,
        ),
        (
            "cmd13_monitor_init_req_le.bin",
            include_bytes!("protocol/spec_vectors/cmd13_monitor_init_req_le.bin"),
            13,
        ),
        (
            "cmd18_status_error_message_le.bin",
            include_bytes!("protocol/spec_vectors/cmd18_status_error_message_le.bin"),
            18,
        ),
    ];

    for (name, bytes, expected_cmd) in vectors {
        let mut packet = PvaPacket::new(bytes);
        assert_eq!(packet.header.command, *expected_cmd, "vector {}", name);
        let decoded = packet.decode_payload();
        assert!(decoded.is_some(), "vector {} failed to decode", name);
    }
}
