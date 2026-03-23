mod protocol;

use std::time::Duration;

use spvirit_codec::epics_decode::PvaPacketCommand;
use protocol::frame_harness::TestServer;
use protocol::scenario_harness::ScenarioSession;

const PV_REQUEST_EMPTY: [u8; 6] = [0xfd, 0x02, 0x00, 0x80, 0x00, 0x00];

fn unsupported_payload(command: u8) -> Vec<u8> {
    match command {
        2 => vec![],
        5 => vec![0],
        6 => vec![0xFF],
        18 => vec![0xFF],
        19 => vec![0],
        21 => 7u32.to_le_bytes().to_vec(),
        22 => vec![0u8; 16],
        _ => vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_op_commands_return_status_errors() {
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
    let sid = scenario.ensure_channel("SIM:AO").await.expect("sid");

    for (ioid, cmd_id) in [(7001u32, 14u8), (7002u32, 16u8), (7003u32, 20u8)] {
        scenario
            .session
            .send_client_op(cmd_id, sid, ioid, 0x08, &PV_REQUEST_EMPTY)
            .await
            .expect("send unsupported op");
        let response = scenario
            .session
            .read_until(Duration::from_secs(2), |cmd| {
                matches!(cmd, PvaPacketCommand::Op(op) if op.command == cmd_id && op.ioid == ioid && (op.subcmd & 0x08) != 0)
            })
            .await
            .expect("unsupported op response");

        match response {
            PvaPacketCommand::Op(op) => {
                let status = op.status.expect("status");
                assert_ne!(status.code, 0xFF);
                let msg = status.message.unwrap_or_default();
                assert!(
                    msg.contains("Operation not supported"),
                    "unexpected unsupported-op message '{}'",
                    msg
                );
            }
            other => panic!("unexpected response for op {}: {:?}", cmd_id, other),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_non_op_commands_are_rejected_and_connection_survives() {
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
    let _sid = scenario.ensure_channel("SIM:AO").await.expect("sid");

    for command in [5u8, 6, 18, 19, 21, 22] {
        let payload = unsupported_payload(command);
        scenario
            .session
            .send_client_application(command, &payload)
            .await
            .expect("send unsupported command");

        let response = scenario
            .session
            .read_until(Duration::from_secs(2), |cmd| {
                matches!(cmd, PvaPacketCommand::Message(_))
            })
            .await
            .expect("message error response");

        match response {
            PvaPacketCommand::Message(msg) => {
                let status = msg.status.expect("status");
                assert_ne!(status.code, 0xFF);
                let text = status.message.unwrap_or_default();
                assert!(
                    text.contains("not supported")
                        || text.contains("Unexpected command")
                        || text.contains("Unknown command"),
                    "unexpected response text '{}'",
                    text
                );
            }
            other => panic!("unexpected response for cmd {}: {:?}", command, other),
        }

        // Health check after each rejected command: standard GET still works.
        let decoded = scenario
            .get_scalar_value("SIM:AO")
            .await
            .expect("post-rejection GET");
        assert!(matches!(
            decoded,
            spvirit_codec::spvd_decode::DecodedValue::Structure(_)
        ));
    }
}
