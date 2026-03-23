mod protocol;

use std::time::Duration;

use pva_codec::epics_decode::PvaPacketCommand;
use pva_codec::pva_encode::encode_header;
use protocol::frame_harness::TestServer;
use tokio::net::UdpSocket;

const PV_REQUEST_EMPTY: [u8; 6] = [0xfd, 0x02, 0x00, 0x80, 0x00, 0x00];

fn encode_size(size: usize) -> Vec<u8> {
    if size == 0 {
        return vec![0x00];
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
async fn get_field_for_existing_channel_returns_introspection() {
    let server = match TestServer::spawn() {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };
    let mut session = server.connect().await.expect("connect");
    session.handshake().await.expect("handshake");
    let cid = 501u32;
    let _sid = session.create_channel(cid, "SIM:AI").await.expect("create");

    session
        .send_get_field(cid, None)
        .await
        .expect("send get_field");
    let response = session
        .read_until(
            Duration::from_secs(2),
            |cmd| matches!(cmd, PvaPacketCommand::GetField(payload) if payload.is_server),
        )
        .await
        .expect("get_field response");

    match response {
        PvaPacketCommand::GetField(payload) => {
            assert!(payload.status.is_none());
            let desc = payload.introspection.expect("introspection");
            assert!(!desc.fields.is_empty());
            assert!(desc.fields.iter().any(|f| f.name == "value"));
        }
        other => panic!("unexpected response {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_field_listing_is_rejected_when_not_in_list_mode() {
    let server = match TestServer::spawn() {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };
    let mut session = server.connect().await.expect("connect");
    session.handshake().await.expect("handshake");

    session
        .send_get_field(0, Some("*"))
        .await
        .expect("send get_field");
    let response = session
        .read_until(
            Duration::from_secs(2),
            |cmd| matches!(cmd, PvaPacketCommand::GetField(payload) if payload.is_server),
        )
        .await
        .expect("get_field response");

    match response {
        PvaPacketCommand::GetField(payload) => {
            let status = payload.status.expect("status");
            assert_ne!(status.code, 0xFF);
            let msg = status.message.unwrap_or_default();
            assert!(msg.contains("disabled"));
        }
        other => panic!("unexpected response {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_field_listing_returns_filtered_names_in_list_mode() {
    let server = match TestServer::spawn_with_args(&[
        "--pvlist-mode",
        "list",
        "--pvlist-max",
        "2",
        "--pvlist-allow-pattern",
        "^SIM:(AI|AO|BO)$",
    ]) {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };
    let mut session = server.connect().await.expect("connect");
    session.handshake().await.expect("handshake");

    session
        .send_get_field(0, Some("SIM:*"))
        .await
        .expect("send get_field");
    let response = session
        .read_until(
            Duration::from_secs(2),
            |cmd| matches!(cmd, PvaPacketCommand::GetField(payload) if payload.is_server),
        )
        .await
        .expect("get_field response");

    match response {
        PvaPacketCommand::GetField(payload) => {
            assert!(payload.status.is_none());
            let desc = payload.introspection.expect("introspection");
            let names: Vec<String> = desc.fields.iter().map(|f| f.name.clone()).collect();
            assert_eq!(names, vec!["SIM:AI".to_string(), "SIM:AO".to_string()]);
        }
        other => panic!("unexpected response {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_search_supports_wildcard_in_discover_mode() {
    let server = match TestServer::spawn() {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };
    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind udp");
    let local = socket.local_addr().expect("local addr");

    let seq = 0x11112222u32;
    let cid = 0xABCDEF01u32;
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
    payload.extend_from_slice(&encode_string("SIM:*"));

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

    let mut packet = pva_codec::epics_decode::PvaPacket::new(&buf);
    let decoded = packet.decode_payload().expect("search decode");
    match decoded {
        PvaPacketCommand::SearchResponse(resp) => {
            assert!(resp.found);
            assert!(resp.cids.contains(&cid));
        }
        other => panic!("unexpected search response {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_search_server_discovery_ping_returns_found() {
    let server = match TestServer::spawn_with_args(&["--pvlist-mode", "discover"]) {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };
    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind udp");
    let local = socket.local_addr().expect("local addr");

    let seq = 0x11113333u32;
    let mut payload = Vec::new();
    payload.extend_from_slice(&seq.to_le_bytes());
    payload.push(0x81); // unicast + response required
    payload.extend_from_slice(&[0u8; 3]);
    payload.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0, 0, 0, 0]); // ::ffff:0.0.0.0
    payload.extend_from_slice(&local.port().to_le_bytes());
    payload.extend_from_slice(&encode_size(1));
    payload.extend_from_slice(&encode_string("tcp"));
    payload.extend_from_slice(&0u16.to_le_bytes()); // zero PV count => server discovery ping

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

    let mut packet = pva_codec::epics_decode::PvaPacket::new(&buf);
    let decoded = packet.decode_payload().expect("search decode");
    match decoded {
        PvaPacketCommand::SearchResponse(resp) => {
            assert!(resp.found);
            assert!(resp.cids.is_empty());
        }
        other => panic!("unexpected search response {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_search_finds_server_rpc_channel_when_enabled() {
    let server = match TestServer::spawn_with_args(&["--pvlist-mode", "discover"]) {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };
    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind udp");
    let local = socket.local_addr().expect("local addr");

    let seq = 0x22223333u32;
    let cid = 0x10203040u32;
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
    payload.extend_from_slice(&encode_string("server"));

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

    let mut packet = pva_codec::epics_decode::PvaPacket::new(&buf);
    let decoded = packet.decode_payload().expect("search decode");
    match decoded {
        PvaPacketCommand::SearchResponse(resp) => {
            assert!(resp.found);
            assert!(resp.cids.contains(&cid));
        }
        other => panic!("unexpected search response {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_search_hides_server_rpc_channel_when_off() {
    let server = match TestServer::spawn_with_args(&["--pvlist-mode", "off"]) {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };
    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind udp");
    let local = socket.local_addr().expect("local addr");

    let seq = 0x33334444u32;
    let cid = 0x55667788u32;
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
    payload.extend_from_slice(&encode_string("server"));

    let mut frame = encode_header(false, false, false, 2, 3, payload.len() as u32);
    frame.extend_from_slice(&payload);
    socket
        .send_to(&frame, format!("127.0.0.1:{}", server.udp_port))
        .await
        .expect("send search");

    let mut buf = vec![0u8; 2048];
    let recv = tokio::time::timeout(Duration::from_millis(500), socket.recv_from(&mut buf)).await;
    if recv.is_err() {
        return;
    }
    let (len, _) = recv.expect("recv timeout handled").expect("search recv");
    buf.truncate(len);
    let mut packet = pva_codec::epics_decode::PvaPacket::new(&buf);
    let decoded = packet.decode_payload().expect("search decode");
    match decoded {
        PvaPacketCommand::SearchResponse(resp) => {
            // When response-required is set, the server may still reply with found=false.
            assert!(
                !resp.found && !resp.cids.contains(&cid),
                "server channel should not be discoverable in off mode"
            );
        }
        other => panic!("unexpected search response {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_rpc_returns_list_in_list_mode() {
    let server = match TestServer::spawn_with_args(&["--pvlist-mode", "list"]) {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };
    let mut session = server.connect().await.expect("connect");
    session.handshake().await.expect("handshake");
    let sid = session
        .create_channel(900, "server")
        .await
        .expect("server channel");

    let ioid = 901u32;
    session
        .send_client_op(20, sid, ioid, 0x08, &PV_REQUEST_EMPTY)
        .await
        .expect("send rpc init");
    let init = session
        .read_until(Duration::from_secs(2), |cmd| {
            matches!(cmd, PvaPacketCommand::Op(op) if op.command == 20 && op.ioid == ioid && (op.subcmd & 0x08) != 0)
        })
        .await
        .expect("rpc init response");
    match init {
        PvaPacketCommand::Op(op) => {
            assert!(op.status.is_none());
            assert!(op.introspection.is_none());
            assert!(op.body.is_empty());
        }
        other => panic!("unexpected init response {:?}", other),
    }

    session
        .send_client_op(20, sid, ioid, 0x00, &[])
        .await
        .expect("send rpc exec");
    let data = session
        .read_until(Duration::from_secs(2), |cmd| {
            matches!(cmd, PvaPacketCommand::Op(op) if op.command == 20 && op.ioid == ioid && op.subcmd == 0x00)
        })
        .await
        .expect("rpc data response");

    match data {
        PvaPacketCommand::Op(op) => {
            assert!(!op.body.is_empty());
            assert_eq!(
                op.body[0], 0x80,
                "rpc payload should start with structure marker"
            );
            let body_text = String::from_utf8_lossy(&op.body);
            assert!(
                body_text.contains("SIM:AI"),
                "rpc payload should include expected PV names"
            );
        }
        other => panic!("unexpected data response {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_rpc_disabled_returns_explicit_error() {
    let server = match TestServer::spawn_with_args(&["--pvlist-mode", "discover"]) {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };
    let mut session = server.connect().await.expect("connect");
    session.handshake().await.expect("handshake");
    let sid = session
        .create_channel(1000, "server")
        .await
        .expect("server channel");

    let ioid = 1001u32;
    session
        .send_client_op(20, sid, ioid, 0x08, &PV_REQUEST_EMPTY)
        .await
        .expect("send rpc init");
    let response = session
        .read_until(Duration::from_secs(2), |cmd| {
            matches!(cmd, PvaPacketCommand::Op(op) if op.command == 20 && op.ioid == ioid && (op.subcmd & 0x08) != 0)
        })
        .await
        .expect("rpc error response");
    match response {
        PvaPacketCommand::Op(op) => {
            let status = op.status.expect("status");
            assert_ne!(status.code, 0xFF);
            assert!(status.message.unwrap_or_default().contains("disabled"));
        }
        other => panic!("unexpected response {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_rpc_execute_disabled_preserves_subcommand() {
    let server = match TestServer::spawn_with_args(&["--pvlist-mode", "discover"]) {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("Skipping test, cannot spawn server: {}", msg);
            return;
        }
    };
    let mut session = server.connect().await.expect("connect");
    session.handshake().await.expect("handshake");
    let sid = session
        .create_channel(1100, "server")
        .await
        .expect("server channel");

    let ioid = 1101u32;
    session
        .send_client_op(20, sid, ioid, 0x00, &[])
        .await
        .expect("send rpc execute");
    let response = session
        .read_until(
            Duration::from_secs(2),
            |cmd| matches!(cmd, PvaPacketCommand::Op(op) if op.command == 20 && op.ioid == ioid),
        )
        .await
        .expect("rpc execute error response");
    match response {
        PvaPacketCommand::Op(op) => {
            assert_eq!(op.subcmd, 0x00, "execute error must keep execute subcmd");
        }
        other => panic!("unexpected response {:?}", other),
    }
}
