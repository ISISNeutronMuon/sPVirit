use std::net::{IpAddr, Ipv4Addr, TcpListener, UdpSocket};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use spvirit_client::pvget;
use spvirit_tools::spvirit_client::types::PvGetOptions;
use spvirit_codec::spvd_decode::DecodedValue;

fn workspace_bin(name: &str) -> String {
    let ext = if cfg!(windows) { ".exe" } else { "" };
    let test_exe = std::env::current_exe().expect("cannot locate test executable");
    test_exe
        .parent().unwrap()
        .parent().unwrap()
        .join(format!("{name}{ext}"))
        .to_string_lossy()
        .to_string()
}

fn free_tcp_port() -> Option<u16> {
    TcpListener::bind("127.0.0.1:0")
        .ok()
        .and_then(|l| l.local_addr().ok())
        .map(|addr| addr.port())
}

fn free_udp_port() -> Option<u16> {
    UdpSocket::bind("127.0.0.1:0")
        .ok()
        .and_then(|s| s.local_addr().ok())
        .map(|addr| addr.port())
}

fn write_array_db() -> String {
    let path = std::env::temp_dir().join(format!("pva_nt_lifecycle_{}.db", std::process::id()));
    let contents = r#"
record(waveform, "SIM:WAVE") {
    field(FTVL, "DOUBLE")
    field(NELM, "4")
    field(NORD, "4")
    field(VAL, "1,2,3,4")
}
record(aai, "SIM:AAI") {
    field(FTVL, "LONG")
    field(NELM, "4")
    field(NORD, "4")
    field(VAL, "5,6,7,8")
}
record(aao, "SIM:AAO") {
    field(FTVL, "LONG")
    field(NELM, "4")
    field(NORD, "4")
    field(VAL, "1,2,3,4")
}
record(subArray, "SIM:SUB") {
    field(FTVL, "LONG")
    field(MALM, "4")
    field(NELM, "2")
    field(NORD, "2")
    field(INDX, "1")
    field(INP, "SIM:AAO")
    field(VAL, "2,3")
}
"#;
    std::fs::write(&path, contents).expect("write db");
    path.to_string_lossy().to_string()
}

fn run_pvput_json(server: &str, pv: &str, json: &str) -> bool {
    let pvput_bin = workspace_bin("spvirit_put");
    Command::new(pvput_bin)
        .arg("--server")
        .arg(server)
        .arg("--json")
        .arg(json)
        .arg(pv)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn extract_value_len(v: &DecodedValue) -> Option<usize> {
    let DecodedValue::Structure(fields) = v else {
        return None;
    };
    for (name, value) in fields {
        if name == "value" {
            if let DecodedValue::Array(items) = value {
                return Some(items.len());
            }
            if let DecodedValue::Structure(inner) = value {
                return Some(inner.len());
            }
        }
    }
    None
}

fn local_pvget_opts(pv_name: &str, tcp_port: u16, udp_port: u16) -> PvGetOptions {
    let mut opts = PvGetOptions::new(pv_name.to_string());
    opts.udp_port = udp_port;
    opts.tcp_port = tcp_port;
    // CI containers may not deliver broadcast discovery to loopback.
    opts.search_addr = Some(IpAddr::V4(Ipv4Addr::LOCALHOST));
    opts.bind_addr = Some(IpAddr::V4(Ipv4Addr::LOCALHOST));
    opts
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_array_and_nt_record_pvs() {
    let tcp_port = match free_tcp_port() {
        Some(p) => p,
        None => {
            eprintln!("Skipping test: cannot bind TCP port");
            return;
        }
    };
    let udp_port = match free_udp_port() {
        Some(p) => p,
        None => {
            eprintln!("Skipping test: cannot bind UDP port");
            return;
        }
    };

    let db_path = write_array_db();

    let server_bin = workspace_bin("spvirit_server");
    let mut child = Command::new(server_bin)
        .arg("--db-file")
        .arg(&db_path)
        .arg("--listen-addr")
        .arg("127.0.0.1")
        .arg("--tcp-port")
        .arg(tcp_port.to_string())
        .arg("--udp-port")
        .arg(udp_port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn spvirit_server");

    thread::sleep(Duration::from_millis(350));

    let mut opts = local_pvget_opts("SIM:WAVE", tcp_port, udp_port);
    let wave = pvget(&opts).await.expect("pvget waveform");
    assert_eq!(
        wave.introspection.struct_id.as_deref(),
        Some("epics:nt/NTScalarArray:1.0")
    );

    let server_addr = format!("127.0.0.1:{}", tcp_port);
    assert!(run_pvput_json(&server_addr, "SIM:AAO", "[9,8,7,6]"));

    opts.pv_name = "SIM:AAO".to_string();
    let updated = pvget(&opts).await.expect("pvget aao");
    assert_eq!(extract_value_len(&updated.value), Some(4));

    assert!(!run_pvput_json(&server_addr, "SIM:AAI", "[1,2,3,4]"));

    let _ = child.kill();
    let _ = child.wait();
}
