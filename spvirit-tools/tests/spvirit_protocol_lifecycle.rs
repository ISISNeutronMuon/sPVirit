mod protocol;

use std::collections::HashSet;
use std::time::Duration;

use protocol::frame_harness::TestServer;
use protocol::scenario_harness::ScenarioSession;
use spvirit_codec::epics_decode::PvaPacketCommand;
use spvirit_codec::spvd_decode::{DecodedValue, extract_nt_scalar_value};
use spvirit_codec::spvirit_encode::encode_header;
use tokio::net::UdpSocket;

const PV_REQUEST_EMPTY: [u8; 6] = [0xfd, 0x02, 0x00, 0x80, 0x00, 0x00];

fn encode_size(size: usize) -> Vec<u8> {
    if size == 0 {
        return vec![0];
    }
    if size < 254 {
        return vec![size as u8];
    }
    let mut out = vec![0xFE];
    out.extend_from_slice(&(size as u32).to_le_bytes());
    out
}

fn encode_string(value: &str) -> Vec<u8> {
    let mut out = encode_size(value.len());
    out.extend_from_slice(value.as_bytes());
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_get_put_monitor_paths() {
    let server = match TestServer::spawn() {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };

    let mut scenario = ScenarioSession::connect(&server)
        .await
        .expect("connect scenario");
    let initial = scenario
        .get_scalar_value("SIM:AO")
        .await
        .expect("get initial");
    let initial_value = extract_nt_scalar_value(&initial).expect("value field");
    match initial_value {
        DecodedValue::Float64(v) => assert!((*v - 2.50).abs() < 1e-6),
        other => panic!("unexpected initial value type: {:?}", other),
    }

    scenario
        .put_scalar_number("SIM:AO", 4.321)
        .await
        .expect("put");
    let after = scenario
        .get_scalar_value("SIM:AO")
        .await
        .expect("get after put");
    let after_value = extract_nt_scalar_value(&after).expect("value field");
    match after_value {
        DecodedValue::Float64(v) => assert!((*v - 4.321).abs() < 1e-6),
        other => panic!("unexpected updated value type: {:?}", other),
    }

    scenario
        .monitor_init_start_stop("SIM:AO")
        .await
        .expect("monitor lifecycle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_multiple_channels_and_ioids() {
    let server = match TestServer::spawn() {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };

    let mut scenario = ScenarioSession::connect(&server)
        .await
        .expect("connect scenario");
    let sid_ai = scenario.ensure_channel("SIM:AI").await.expect("sid ai");
    let sid_ao = scenario.ensure_channel("SIM:AO").await.expect("sid ao");

    let ioid_ai = 4001u32;
    let ioid_ao = 4002u32;
    scenario
        .session
        .send_client_op(10, sid_ai, ioid_ai, 0x08, &PV_REQUEST_EMPTY)
        .await
        .expect("get init ai");
    scenario
        .session
        .send_client_op(10, sid_ao, ioid_ao, 0x08, &PV_REQUEST_EMPTY)
        .await
        .expect("get init ao");

    let mut init_seen = HashSet::new();
    while init_seen.len() < 2 {
        let cmd = scenario
            .session
            .read_until(Duration::from_secs(2), |cmd| {
                matches!(cmd, PvaPacketCommand::Op(op) if op.command == 10 && (op.subcmd & 0x08) != 0)
            })
            .await
            .expect("init response");
        if let PvaPacketCommand::Op(op) = cmd {
            init_seen.insert(op.ioid);
        }
    }
    assert!(init_seen.contains(&ioid_ai));
    assert!(init_seen.contains(&ioid_ao));

    scenario
        .session
        .send_client_op(10, sid_ai, ioid_ai, 0x00, &[])
        .await
        .expect("get data ai");
    scenario
        .session
        .send_client_op(10, sid_ao, ioid_ao, 0x00, &[])
        .await
        .expect("get data ao");

    let mut data_seen = HashSet::new();
    while data_seen.len() < 2 {
        let cmd = scenario
            .session
            .read_until(Duration::from_secs(2), |cmd| {
                matches!(cmd, PvaPacketCommand::Op(op) if op.command == 10 && op.subcmd == 0x00)
            })
            .await
            .expect("data response");
        if let PvaPacketCommand::Op(op) = cmd {
            data_seen.insert(op.ioid);
        }
    }
    assert!(data_seen.contains(&ioid_ai));
    assert!(data_seen.contains(&ioid_ao));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_put_without_init_returns_error() {
    let server = match TestServer::spawn() {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };

    let mut scenario = ScenarioSession::connect(&server)
        .await
        .expect("connect scenario");
    let sid = scenario.ensure_channel("SIM:AO").await.expect("sid ao");
    let ioid = 5010u32;

    scenario
        .session
        .send_client_op(11, sid, ioid, 0x00, &[0x00, 0x02])
        .await
        .expect("put data without init");
    let cmd = scenario
        .session
        .read_until(
            Duration::from_secs(2),
            |cmd| matches!(cmd, PvaPacketCommand::Op(op) if op.command == 11 && op.ioid == ioid),
        )
        .await
        .expect("put error response");

    match cmd {
        PvaPacketCommand::Op(op) => {
            let status = op.status.expect("status");
            assert_ne!(status.code, 0xFF);
            let msg = status.message.unwrap_or_default();
            assert!(
                msg.contains("PUT without init"),
                "unexpected message: {}",
                msg
            );
        }
        other => panic!("unexpected command: {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_destroy_channel_invalidates_sid() {
    let server = match TestServer::spawn() {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };

    let mut scenario = ScenarioSession::connect(&server)
        .await
        .expect("connect scenario");
    let sid = scenario.ensure_channel("SIM:AI").await.expect("sid ai");
    let cid = 9901u32;
    let ioid = 9902u32;

    let mut destroy_payload = Vec::new();
    destroy_payload.extend_from_slice(&sid.to_le_bytes());
    destroy_payload.extend_from_slice(&cid.to_le_bytes());
    scenario
        .session
        .send_client_application(8, &destroy_payload)
        .await
        .expect("destroy channel");

    scenario
        .session
        .send_client_op(10, sid, ioid, 0x08, &PV_REQUEST_EMPTY)
        .await
        .expect("get after destroy");
    let cmd = scenario
        .session
        .read_until(Duration::from_secs(2), |cmd| {
            matches!(cmd, PvaPacketCommand::Op(op) if op.command == 10 && op.ioid == ioid && (op.subcmd & 0x08) != 0)
        })
        .await
        .expect("op error");

    match cmd {
        PvaPacketCommand::Op(op) => {
            let status = op.status.expect("status");
            let msg = status.message.unwrap_or_default();
            assert!(msg.contains("Unknown SID"), "unexpected error msg: {}", msg);
        }
        other => panic!("unexpected command after destroy: {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_application_echo_roundtrip() {
    let server = match TestServer::spawn() {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };

    let mut scenario = ScenarioSession::connect(&server)
        .await
        .expect("connect scenario");

    let payload = b"echo-smoke";
    scenario
        .session
        .send_client_application(2, payload)
        .await
        .expect("send application echo");

    let cmd = scenario
        .session
        .read_until(Duration::from_secs(2), |cmd| {
            matches!(cmd, PvaPacketCommand::Echo(_))
        })
        .await
        .expect("echo response");

    match cmd {
        PvaPacketCommand::Echo(bytes) => assert_eq!(bytes, payload),
        other => panic!("unexpected echo response: {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_rpc_list_mode_gated_behavior() {
    let server = match TestServer::spawn_with_args(&["--pvlist-mode", "list"]) {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };

    let mut scenario = ScenarioSession::connect(&server)
        .await
        .expect("connect scenario");
    let sid = scenario.ensure_channel("server").await.expect("sid server");
    let ioid = 6101u32;

    scenario
        .session
        .send_client_op(20, sid, ioid, 0x08, &PV_REQUEST_EMPTY)
        .await
        .expect("rpc init");
    let init_cmd = scenario
        .session
        .read_until(Duration::from_secs(2), |cmd| {
            matches!(cmd, PvaPacketCommand::Op(op) if op.command == 20 && op.ioid == ioid && (op.subcmd & 0x08) != 0)
        })
        .await
        .expect("rpc init response");
    match init_cmd {
        PvaPacketCommand::Op(op) => {
            if let Some(status) = op.status {
                assert_eq!(
                    status.code, 0xFF,
                    "unexpected rpc init status: {:?}",
                    status
                );
            }
        }
        other => panic!("unexpected rpc init response: {:?}", other),
    }

    scenario
        .session
        .send_client_op(20, sid, ioid, 0x00, &[])
        .await
        .expect("rpc data");
    let data_cmd = scenario
        .session
        .read_until(Duration::from_secs(2), |cmd| {
            matches!(cmd, PvaPacketCommand::Op(op) if op.command == 20 && op.ioid == ioid && op.subcmd == 0x00)
        })
        .await
        .expect("rpc data response");
    match data_cmd {
        PvaPacketCommand::Op(op) => {
            assert!(
                !op.body.is_empty(),
                "rpc data response should contain payload body"
            );
        }
        other => panic!("unexpected rpc data response: {:?}", other),
    }

    let server = match TestServer::spawn_with_args(&["--pvlist-mode", "off"]) {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };
    let mut scenario = ScenarioSession::connect(&server)
        .await
        .expect("connect scenario");
    let sid = scenario.ensure_channel("SIM:AI").await.expect("sid ai");
    let ioid = 6201u32;
    scenario
        .session
        .send_client_op(20, sid, ioid, 0x08, &PV_REQUEST_EMPTY)
        .await
        .expect("rpc init off mode");
    let denied_cmd = scenario
        .session
        .read_until(Duration::from_secs(2), |cmd| {
            matches!(cmd, PvaPacketCommand::Op(op) if op.command == 20 && op.ioid == ioid && (op.subcmd & 0x08) != 0)
        })
        .await
        .expect("rpc deny response");
    match denied_cmd {
        PvaPacketCommand::Op(op) => {
            let status = op.status.expect("expected error status");
            let msg = status.message.unwrap_or_default();
            assert!(
                msg.contains("Operation not supported"),
                "unexpected rpc deny message: {}",
                msg
            );
        }
        other => panic!("unexpected rpc denied response: {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_udp_search_returns_matching_cid() {
    let server = match TestServer::spawn() {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };

    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind udp");
    let local = socket.local_addr().expect("local addr");

    let seq = 0x12345678u32;
    let cid = 0x01020304u32;
    let mut payload = Vec::new();
    payload.extend_from_slice(&seq.to_le_bytes());
    payload.push(0x81);
    payload.extend_from_slice(&[0u8; 3]);
    payload.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 127, 0, 0, 1]);
    payload.extend_from_slice(&local.port().to_le_bytes());
    payload.extend_from_slice(&encode_size(1));
    payload.extend_from_slice(&encode_string("tcp"));
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.extend_from_slice(&cid.to_le_bytes());
    payload.extend_from_slice(&encode_string("SIM:AI"));

    let mut frame = encode_header(false, false, false, 2, 3, payload.len() as u32);
    frame.extend_from_slice(&payload);
    socket
        .send_to(&frame, format!("127.0.0.1:{}", server.udp_port))
        .await
        .expect("send search");

    let mut buf = vec![0u8; 2048];
    let (len, _) = tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut buf))
        .await
        .expect("search timeout")
        .expect("search recv");
    buf.truncate(len);

    let mut packet = spvirit_codec::epics_decode::PvaPacket::new(&buf);
    let decoded = packet.decode_payload().expect("search decode");
    match decoded {
        PvaPacketCommand::SearchResponse(resp) => {
            assert!(resp.found);
            assert!(resp.cids.contains(&cid));
            assert_eq!(resp.protocol, "tcp");
        }
        other => panic!("unexpected search response {:?}", other),
    }
}
