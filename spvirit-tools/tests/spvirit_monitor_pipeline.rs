//! End-to-end coverage for PVAccess monitor pipelining (flow control).
//!
//! Spawns the in-tree `spserver` binary and uses the Rust client's
//! `pvmonitor_with_options` to verify that:
//!
//! - the pipeline handshake (subcmd bit 0x80 on INIT + trailing queueSize,
//!   START bit 0x80, ACK subcmd 0x80) works end-to-end against the
//!   pipeline-aware spvirit server, and
//! - credit returns via ACK allow the server to continue streaming updates
//!   beyond the initial `queueSize` (i.e. a flow of > `queueSize` frames
//!   reaches the client when the callback is draining them).

mod protocol;

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use spvirit_client::{MonitorOptions, PvaClient};

use protocol::frame_harness::TestServer;

const PV: &str = "SIM:AO";

fn build_client(server: &TestServer) -> PvaClient {
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", server.tcp_port)
        .parse()
        .expect("server addr parse");
    PvaClient::builder()
        .port(server.tcp_port)
        .udp_port(server.udp_port)
        .timeout(Duration::from_secs(2))
        .server_addr(addr)
        .build()
}

/// A pipelined monitor with `queueSize=2` should still deliver more than
/// two updates over its lifetime once the client ACKs them: the server
/// initially grants 2 credits, the client ACKs as it consumes frames
/// (auto-ACK threshold is `queueSize/2`), and the server replenishes and
/// keeps sending as the underlying PV changes.
#[tokio::test]
async fn pipelined_monitor_receives_updates_beyond_queue_size() {
    let server = TestServer::spawn().expect("spawn server");
    let client = build_client(&server);

    // Drive the PV from a separate task so the monitor loop sees multiple
    // updates. Each PUT fans out through the server's monitor registry.
    let driver_client = build_client(&server);
    let driver = tokio::spawn(async move {
        // Small settling delay so the monitor is subscribed before we
        // start generating traffic.
        tokio::time::sleep(Duration::from_millis(200)).await;
        for i in 0..8u32 {
            // Ignore individual errors; the monitor side has the final say.
            let _ = driver_client.pvput(PV, 10.0 + i as f64).await;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    });

    let count: Arc<StdMutex<u32>> = Arc::new(StdMutex::new(0));
    let count_cb = count.clone();
    // Target > queueSize so we only succeed if credits are being
    // replenished by ACKs.
    let target: u32 = 5;

    let monitor =
        client.pvmonitor_with_options(PV, &[], MonitorOptions::pipelined(2), move |_value| {
            let mut c = count_cb.lock().unwrap();
            *c += 1;
            if *c >= target {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });

    tokio::time::timeout(Duration::from_secs(5), monitor)
        .await
        .expect("pipelined monitor timed out before reaching target frame count")
        .expect("pipelined monitor returned error");

    let _ = driver.await;

    let final_count = *count.lock().unwrap();
    assert!(
        final_count >= target,
        "pipelined monitor delivered only {final_count} frames (expected >= {target}); \
         ACK credit return may be broken",
    );
}

/// A non-pipelined monitor (the default) must remain unaffected by the
/// new code path — this is a regression guard for the refactor that
/// introduced `MonitorOptions`.
#[tokio::test]
async fn non_pipelined_monitor_still_works() {
    let server = TestServer::spawn().expect("spawn server");
    let client = build_client(&server);

    let got_one = Arc::new(StdMutex::new(false));
    let flag = got_one.clone();

    let monitor =
        client.pvmonitor_with_options(PV, &[], MonitorOptions::default(), move |_value| {
            *flag.lock().unwrap() = true;
            ControlFlow::Break(())
        });

    tokio::time::timeout(Duration::from_secs(3), monitor)
        .await
        .expect("non-pipelined monitor timed out")
        .expect("non-pipelined monitor returned error");

    assert!(
        *got_one.lock().unwrap(),
        "non-pipelined monitor got no frames"
    );
}
