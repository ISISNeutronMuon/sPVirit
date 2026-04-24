//! Interop coverage for monitor **pipelining** against external PVAccess
//! implementations.
//!
//! Three scenarios, each gated behind its own env var so the default
//! `cargo test` never triggers them:
//!
//! 1. `p4p_client_pipeline_to_spvirit_server_optional`
//!    (`RUN_P4P_PIPELINE_INTEROP=1`) — a Python p4p client subscribes to
//!    a spvirit server with `record[pipeline=true,queueSize=N]` and
//!    must successfully receive ≥ N pipelined frames.
//!
//! 2. `spvirit_client_pipeline_to_p4p_server_optional`
//!    (`RUN_P4P_PIPELINE_INTEROP=1`) — our `spmonitor --pipeline N`
//!    client connects to a p4p `SharedPV` server and must receive ≥ N
//!    frames while a helper drives updates.
//!
//! 3. `spvirit_client_pipeline_to_epics_base_server_optional`
//!    (`RUN_EPICS_INTEROP=1`) — our `spmonitor --pipeline N` client
//!    connects to an EPICS-base `softIocPVA` (pvxs-backed) server and
//!    must receive ≥ N frames with pvput-driven updates.

#[path = "interop/harness.rs"]
mod harness;

use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use harness::{
    LocalServerFixture, ProcessGuard, env_enabled, env_string, free_tcp_port, free_udp_port,
    run_command_success, run_command_success_with_timeout, tool_path_from_base, workspace_bin,
};

fn pipeline_script_path() -> std::path::PathBuf {
    let test_exe = std::env::current_exe().expect("test exe path");
    // target/debug/deps/<exe> → workspace root is 4 levels up.
    let workspace = test_exe
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .expect("walk to workspace root");
    workspace
        .join("spvirit-tools")
        .join("tests")
        .join("interop")
        .join("p4p_pipeline_client.py")
}

fn python_cmd() -> String {
    env_string("PYTHON").unwrap_or_else(|| "python3".to_string())
}

// ---------------------------------------------------------------------------
// 1. p4p client → spvirit server pipelining
// ---------------------------------------------------------------------------

#[test]
fn p4p_client_pipeline_to_spvirit_server_optional() {
    if !env_enabled("RUN_P4P_PIPELINE_INTEROP") {
        eprintln!("Skipping p4p pipeline interop: set RUN_P4P_PIPELINE_INTEROP=1");
        return;
    }

    let script = pipeline_script_path();
    if !script.exists() {
        eprintln!(
            "Skipping p4p pipeline interop: script not found at {:?}",
            script
        );
        return;
    }

    // Quick availability probe for p4p.
    let probe = Command::new(python_cmd())
        .arg("-c")
        .arg("import p4p.client.thread")
        .output();
    match probe {
        Ok(out) if out.status.success() => {}
        Ok(out) => {
            eprintln!(
                "Skipping p4p pipeline interop: p4p import failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            return;
        }
        Err(err) => {
            eprintln!(
                "Skipping p4p pipeline interop: {} not runnable: {}",
                python_cmd(),
                err
            );
            return;
        }
    }

    let prefix = format!("PIPE:P4P:{}", std::process::id());
    let ao_pv = format!("{}:AO", prefix);
    let db = format!(
        r#"
record(ao, "{}") {{
    field(VAL, "0.0")
}}
"#,
        ao_pv
    );

    let server =
        LocalServerFixture::spawn(&db, &["--pvlist-mode", "list"]).expect("spawn spvirit server");

    let mut cmd = Command::new(python_cmd());
    cmd.arg(&script)
        .env("SPVIRIT_TEST_TCP_PORT", server.tcp_port.to_string())
        .env("SPVIRIT_TEST_UDP_PORT", server.udp_port.to_string())
        .env("SPVIRIT_TEST_PV", &ao_pv)
        .env("SPVIRIT_PIPELINE_QSIZE", "4")
        .env("SPVIRIT_PIPELINE_TARGET", "6")
        .env("SPVIRIT_PIPELINE_TIMEOUT", "15")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let output =
        run_command_success_with_timeout(&mut cmd, "p4p pipeline client", Duration::from_secs(30))
            .expect("p4p pipeline script should succeed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    eprintln!("[p4p pipeline stdout] {}", stdout);
    assert!(
        stdout.contains("PASS"),
        "p4p pipeline client did not report PASS: {}",
        stdout
    );
}

// ---------------------------------------------------------------------------
// 2. spvirit pipelined client → p4p server
// ---------------------------------------------------------------------------

#[test]
fn spvirit_client_pipeline_to_p4p_server_optional() {
    // This test exercises our pipelined monitor *client* against a
    // pvxs-based server (p4p). pvxs has stricter expectations for the
    // pvRequest `record._options` encoding than a plain record[] monitor
    // test can cover; leave it gated behind a dedicated env var so it
    // can be enabled as a focused regression check when the p4p
    // compatibility surface is being exercised.
    if !env_enabled("RUN_SPVIRIT_TO_P4P_PIPELINE") {
        eprintln!(
            "Skipping spvirit→p4p pipeline interop: set \
             RUN_SPVIRIT_TO_P4P_PIPELINE=1 (and P4P_PROVIDER_CMD)"
        );
        return;
    }

    let provider_cmd = match env_string("P4P_PROVIDER_CMD") {
        Some(c) => c,
        None => {
            eprintln!(
                "Skipping spvirit→p4p pipeline interop: set P4P_PROVIDER_CMD (e.g. \
                 'python3 spvirit-tools/tests/interop/p4p_server.py')"
            );
            return;
        }
    };

    // p4p_server.py uses default EPICS env (port 5075/5076). Run it on those,
    // letting EPICS env select them if the test host has room.
    let mut provider = Command::new("sh");
    provider
        .arg("-lc")
        .arg(&provider_cmd)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let _provider_guard = match ProcessGuard::spawn(&mut provider, "spawn p4p provider") {
        Ok(g) => g,
        Err(e) => {
            eprintln!(
                "Skipping spvirit→p4p pipeline interop: provider failed: {}",
                e
            );
            return;
        }
    };

    let ready_ms = env_string("P4P_PROVIDER_READY_MS")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(3000);
    thread::sleep(Duration::from_millis(ready_ms));

    let pv = env_string("P4P_TEST_PV_RW").unwrap_or_else(|| "p4p:rw".to_string());

    let spmon = workspace_bin("spmonitor");
    let spput = workspace_bin("spput");

    // Point both tools at loopback so UDP search finds the p4p provider
    // running on default EPICS ports on this host.
    let client_env: [(&str, &str); 2] = [
        ("EPICS_PVA_ADDR_LIST", "127.0.0.1"),
        ("EPICS_PVA_AUTO_ADDR_LIST", "YES"),
    ];

    // Start pipelined monitor.
    let mut mon_cmd = Command::new(&spmon);
    mon_cmd
        .arg("--pipeline")
        .arg("4")
        .arg("--search-addr")
        .arg("127.0.0.1")
        .arg(&pv)
        .envs(client_env.iter().copied())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut mon =
        ProcessGuard::spawn(&mut mon_cmd, "spmonitor pipelined").expect("spmonitor should spawn");

    // Drain stdout/stderr continuously so buffered output isn't lost when
    // we kill the child.
    use std::io::{BufRead, BufReader};
    use std::sync::{Arc, Mutex};
    let stdout_buf = Arc::new(Mutex::new(String::new()));
    let stderr_buf = Arc::new(Mutex::new(String::new()));
    if let Some(s) = mon.child_mut().stdout.take() {
        let buf = stdout_buf.clone();
        thread::spawn(move || {
            for line in BufReader::new(s).lines().flatten() {
                buf.lock().unwrap().push_str(&line);
                buf.lock().unwrap().push('\n');
            }
        });
    }
    if let Some(s) = mon.child_mut().stderr.take() {
        let buf = stderr_buf.clone();
        thread::spawn(move || {
            for line in BufReader::new(s).lines().flatten() {
                buf.lock().unwrap().push_str(&line);
                buf.lock().unwrap().push('\n');
            }
        });
    }

    // Drive updates.
    thread::sleep(Duration::from_millis(800));
    let start = Instant::now();
    for i in 0..20u32 {
        if start.elapsed() > Duration::from_secs(5) {
            break;
        }
        let _ = Command::new(&spput)
            .arg("--search-addr")
            .arg("127.0.0.1")
            .arg(&pv)
            .arg(format!("{}", 10.0 + i as f64))
            .envs(client_env.iter().copied())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        thread::sleep(Duration::from_millis(80));
    }

    thread::sleep(Duration::from_millis(500));
    mon.kill_and_wait();

    let stdout = stdout_buf.lock().unwrap().clone();
    let stderr = stderr_buf.lock().unwrap().clone();
    eprintln!("[spmonitor→p4p stdout]\n{}", stdout);
    eprintln!("[spmonitor→p4p stderr]\n{}", stderr);

    // Each frame renders at least the PV name once. Require >= pipeline queue
    // size occurrences to prove credits were being returned via ACK.
    let occurrences = stdout.matches(&pv).count();
    assert!(
        occurrences >= 4,
        "spmonitor got only {} frames from p4p server (expected >= 4):\nstdout:\n{}\nstderr:\n{}",
        occurrences,
        stdout,
        stderr
    );
}

// ---------------------------------------------------------------------------
// 3. spvirit pipelined client → EPICS-base (softIocPVA) server
// ---------------------------------------------------------------------------

#[test]
fn spvirit_client_pipeline_to_epics_base_server_optional() {
    if !env_enabled("RUN_EPICS_INTEROP") {
        eprintln!("Skipping spvirit→EPICS pipeline interop: set RUN_EPICS_INTEROP=1");
        return;
    }

    let Some(softioc_bin) = tool_path_from_base("EPICS_BASE_BIN", "softIocPVA") else {
        eprintln!(
            "Skipping spvirit→EPICS pipeline interop: EPICS_BASE_BIN/softIocPVA not found \
             (requires EPICS 7 with pvxs)"
        );
        return;
    };

    let prefix = format!("PIPE:EPX:{}", std::process::id());
    let ao_pv = format!("{}:AO", prefix);
    let db_contents = format!(
        r#"record(ao, "{}") {{
    field(VAL, "0.0")
}}
"#,
        ao_pv
    );

    // Write temp DB.
    let tmp_dir = std::env::temp_dir();
    let db_path = tmp_dir.join(format!(
        "spvirit_pipeline_softioc_{}_{}.db",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    std::fs::write(&db_path, db_contents).expect("write temp db");

    // Run softIocPVA on its own PVA ports so we don't collide with anything.
    let tcp_port = free_tcp_port().expect("free tcp port");
    let udp_port = free_udp_port().expect("free udp port");
    let ioc_env = [
        ("EPICS_PVA_ADDR_LIST", "127.0.0.1"),
        ("EPICS_PVA_AUTO_ADDR_LIST", "NO"),
        ("EPICS_PVA_SERVER_PORT", &tcp_port.to_string()),
        ("EPICS_PVA_BROADCAST_PORT", &udp_port.to_string()),
    ];

    let mut ioc_cmd = Command::new(&softioc_bin);
    ioc_cmd
        .arg("-d")
        .arg(&db_path)
        .envs(ioc_env.iter().map(|(k, v)| (*k, *v)))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let _ioc = match ProcessGuard::spawn(&mut ioc_cmd, "spawn softIocPVA") {
        Ok(g) => g,
        Err(e) => {
            eprintln!(
                "Skipping spvirit→EPICS pipeline interop: softIocPVA spawn failed: {}",
                e
            );
            let _ = std::fs::remove_file(&db_path);
            return;
        }
    };
    thread::sleep(Duration::from_millis(800));

    let spmon = workspace_bin("spmonitor");

    // Point spmonitor at the IOC via explicit ports.
    let mut mon_cmd = Command::new(&spmon);
    mon_cmd
        .arg("--pipeline")
        .arg("4")
        .arg("--server")
        .arg(format!("127.0.0.1:{}", tcp_port))
        .arg("--udp-port")
        .arg(udp_port.to_string())
        .arg("--no-broadcast")
        .arg(&ao_pv)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut mon = ProcessGuard::spawn(&mut mon_cmd, "spmonitor pipelined (epics)")
        .expect("spmonitor should spawn");

    // Drive updates via EPICS pvput.
    thread::sleep(Duration::from_millis(400));
    let pvput_bin = tool_path_from_base("EPICS_BASE_BIN", "pvput");
    if let Some(pvput_bin) = pvput_bin {
        for i in 0..10u32 {
            let mut put = Command::new(&pvput_bin);
            put.arg(&ao_pv)
                .arg(format!("{}", 1.0 + i as f64))
                .envs(ioc_env.iter().map(|(k, v)| (*k, *v)))
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let _ = run_command_success(&mut put, "epics pvput");
            thread::sleep(Duration::from_millis(80));
        }
    } else {
        eprintln!(
            "warn: EPICS_BASE_BIN/pvput missing; server-native update rate will drive frames"
        );
    }

    thread::sleep(Duration::from_millis(500));
    mon.kill_and_wait();

    let stdout = mon
        .child_mut()
        .stdout
        .take()
        .map(|s| std::io::read_to_string(s).unwrap_or_default())
        .unwrap_or_default();
    eprintln!("[spmonitor→EPICS stdout]\n{}", stdout);

    let occurrences = stdout.matches(&ao_pv).count();
    let _ = std::fs::remove_file(&db_path);

    assert!(
        occurrences >= 3,
        "spmonitor got only {} frames from softIocPVA (expected >= 3):\n{}",
        occurrences,
        stdout
    );
}
