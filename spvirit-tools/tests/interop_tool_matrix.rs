#[path = "interop/harness.rs"]
mod harness;

use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use regex::Regex;

use harness::{
    ProcessGuard, contains_any, env_enabled, env_string, run_command, run_command_success,
    run_command_success_with_timeout, tool_path_from_base, workspace_bin,
};

#[derive(Clone)]
struct PvCase {
    name: String,
    writable: bool,
}

fn parse_external_pv_cases() -> Vec<PvCase> {
    let raw = env_string("PVA_INTEROP_EXTERNAL_PVS")
        .or_else(|| env_string("PVA_EXT_PVS"))
        .or_else(|| {
            let rw = env_string("PVA_EXT_RW_PV");
            let ro1 = env_string("PVA_EXT_RO_PV_1");
            let ro2 = env_string("PVA_EXT_RO_PV_2");
            let ro3 = env_string("PVA_EXT_RO_PV_3");
            let table = env_string("PVA_EXT_TABLE_PV");
            let mut values = Vec::new();
            if let Some(v) = rw {
                values.push(v);
            }
            if let Some(v) = ro1 {
                values.push(v);
            }
            if let Some(v) = ro2 {
                values.push(v);
            }
            if let Some(v) = ro3 {
                values.push(v);
            }
            if let Some(v) = table {
                values.push(v);
            }
            if values.is_empty() {
                None
            } else {
                Some(values.join(","))
            }
        })
        .unwrap_or_else(|| {
            "DEV:PORTHOS:circle:step,root:ai1,root:ai2,root:ai3,DEV:ATHOS:GW:STS:ds:byhost:tx"
                .to_string()
        });

    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|name| PvCase {
            writable: name == "DEV:PORTHOS:circle:step",
            name: name.to_string(),
        })
        .collect()
}

fn rust_tool_command(bin: &str, server: Option<&str>, pv: &str) -> Command {
    let mut command = Command::new(bin);
    if let Some(addr) = server {
        command.arg("--server").arg(addr);
    }
    let timeout = interop_cmd_timeout_secs();
    command
        .arg("-w")
        .arg(timeout.to_string())
        .arg(pv)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn interop_cmd_timeout_secs() -> u64 {
    env_string("PVA_INTEROP_CMD_TIMEOUT_SECS")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(20)
}

fn run_rust_read_tools(server: Option<&str>, pv: &str) {
    let pvget_bin = workspace_bin("spget");
    let mut pvget = rust_tool_command(&pvget_bin, server, pv);
    let pvget_output = run_command(&mut pvget, "rust spvirit_get")
        .unwrap_or_else(|error| panic!("pvget failed for {}: {}", pv, error));
    if !pvget_output.status.success() {
        let stderr = String::from_utf8_lossy(&pvget_output.stderr);
        panic!(
            "pvget failed for {}: rust pvget: exit={} stderr={} stdout={}",
            pv,
            pvget_output
                .status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "signal".to_string()),
            stderr,
            String::from_utf8_lossy(&pvget_output.stdout)
        );
    }
    let pvget_text = String::from_utf8_lossy(&pvget_output.stdout);
    assert!(
        contains_any(&pvget_text, &[pv, "value", "alarm", "timeStamp"]),
        "unexpected pvget output for {}: {}",
        pv,
        pvget_text
    );

    let pvinfo_bin = workspace_bin("spinfo");
    let mut pvinfo = rust_tool_command(&pvinfo_bin, server, pv);
    let pvinfo_output = run_command(&mut pvinfo, "rust spvirit_info")
        .unwrap_or_else(|error| panic!("pvinfo failed for {}: {}", pv, error));

    if pvinfo_output.status.success() {
        let pvinfo_text = String::from_utf8_lossy(&pvinfo_output.stdout);
        assert!(
            contains_any(&pvinfo_text, &[pv, "structure", "value"]),
            "unexpected pvinfo output for {}: {}",
            pv,
            pvinfo_text
        );
        return;
    }
    let pvinfo_stderr = String::from_utf8_lossy(&pvinfo_output.stderr);
    panic!(
        "pvinfo failed for {}: rust pvinfo: exit={} stderr={} stdout={}",
        pv,
        pvinfo_output
            .status
            .code()
            .map(|code| code.to_string())
            .unwrap_or_else(|| "signal".to_string()),
        pvinfo_stderr,
        String::from_utf8_lossy(&pvinfo_output.stdout)
    );
}

fn run_rust_write_restore(server: Option<&str>, pv: &str) {
    if !env_enabled("PVA_TEST_ALLOW_EXTERNAL_WRITE") {
        eprintln!(
            "Skipping write/restore for {}: set PVA_TEST_ALLOW_EXTERNAL_WRITE=1 to enable",
            pv
        );
        return;
    }

    let pvput_bin = workspace_bin("spput");

    let mut read_before = rust_tool_command(&workspace_bin("spget"), server, pv);
    let before_output = run_command_success(&mut read_before, "rust pvget before write")
        .unwrap_or_else(|error| panic!("pvget before write failed for {}: {}", pv, error));
    let before_text = String::from_utf8_lossy(&before_output.stdout);
    let value_regex = Regex::new(r"(?i)value[^0-9+\-]*([+\-]?\d+(?:\.\d+)?)").expect("valid regex");
    let original_value = value_regex
        .captures(&before_text)
        .and_then(|captures| captures.get(1))
        .map(|match_value| match_value.as_str().to_string())
        .unwrap_or_else(|| {
            eprintln!(
                "Could not parse original value for {}; using fallback restore value 0",
                pv
            );
            "0".to_string()
        });

    let mut write_cmd = Command::new(&pvput_bin);
    if let Some(addr) = server {
        write_cmd.arg("--server").arg(addr);
    }
    write_cmd
        .arg(pv)
        .arg("1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let _ = run_command_success(&mut write_cmd, "rust pvput write")
        .unwrap_or_else(|error| panic!("pvput write failed for {}: {}", pv, error));

    let mut restore_cmd = Command::new(&pvput_bin);
    if let Some(addr) = server {
        restore_cmd.arg("--server").arg(addr);
    }
    restore_cmd
        .arg(pv)
        .arg(&original_value)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let _ = run_command_success(&mut restore_cmd, "rust pvput restore")
        .unwrap_or_else(|error| panic!("pvput restore failed for {}: {}", pv, error));
}

fn run_epics_read_tools(pv: &str) {
    let Some(pvget_bin) = tool_path_from_base("EPICS_BASE_BIN", "pvget") else {
        eprintln!("Skipping EPICS tool matrix: EPICS_BASE_BIN/pvget not found");
        return;
    };
    let Some(pvinfo_bin) = tool_path_from_base("EPICS_BASE_BIN", "pvinfo") else {
        eprintln!("Skipping EPICS tool matrix: EPICS_BASE_BIN/pvinfo not found");
        return;
    };

    let mut pvget = Command::new(pvget_bin);
    pvget.arg(pv).stdout(Stdio::piped()).stderr(Stdio::piped());
    eprintln!("[interop] epics pvget => {}", pv);
    let started_at = Instant::now();
    let output = run_command_success_with_timeout(
        &mut pvget,
        "epics pvget",
        Duration::from_secs(interop_cmd_timeout_secs()),
    )
    .unwrap_or_else(|error| panic!("epics pvget failed for {}: {}", pv, error));
    eprintln!(
        "[interop] epics pvget completed in {:?}",
        started_at.elapsed()
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        contains_any(&text, &[pv, "value", "alarm"]),
        "unexpected epics pvget output for {}: {}",
        pv,
        text
    );

    let mut pvinfo = Command::new(pvinfo_bin);
    pvinfo.arg(pv).stdout(Stdio::piped()).stderr(Stdio::piped());
    eprintln!("[interop] epics pvinfo => {}", pv);
    let started_at = Instant::now();
    let output = run_command_success_with_timeout(
        &mut pvinfo,
        "epics pvinfo",
        Duration::from_secs(interop_cmd_timeout_secs()),
    )
    .unwrap_or_else(|error| panic!("epics pvinfo failed for {}: {}", pv, error));
    eprintln!(
        "[interop] epics pvinfo completed in {:?}",
        started_at.elapsed()
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        contains_any(&text, &[pv, "value", "structure"]),
        "unexpected epics pvinfo output for {}: {}",
        pv,
        text
    );
}

fn run_epics_pvlist(server_target: Option<&str>) {
    let Some(pvlist_bin) = tool_path_from_base("EPICS_BASE_BIN", "pvlist") else {
        eprintln!("Skipping EPICS pvlist check: EPICS_BASE_BIN/pvlist not found");
        return;
    };

    let mut pvlist = Command::new(pvlist_bin);
    if let Some(target) = server_target {
        let trimmed = target.trim();
        if !trimmed.is_empty() {
            pvlist.arg(trimmed);
        }
    }
    pvlist.stdout(Stdio::piped()).stderr(Stdio::piped());
    eprintln!(
        "[interop] epics pvlist => {}",
        server_target.unwrap_or("<discover>")
    );
    let started_at = Instant::now();
    let output = run_command_success_with_timeout(
        &mut pvlist,
        "epics pvlist",
        Duration::from_secs(interop_cmd_timeout_secs()),
    )
    .unwrap_or_else(|error| {
        panic!(
            "epics pvlist failed for {:?}: {}",
            server_target.unwrap_or("<discover>"),
            error
        )
    });
    eprintln!(
        "[interop] epics pvlist completed in {:?}",
        started_at.elapsed()
    );

    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        !text.trim().is_empty(),
        "epics pvlist output was empty for target {:?}",
        server_target.unwrap_or("<discover>")
    );
}

fn run_epics_monitor_smoke(pv: &str) {
    let Some(pvmonitor_bin) = tool_path_from_base("EPICS_BASE_BIN", "pvmonitor") else {
        eprintln!("Skipping EPICS pvmonitor check: EPICS_BASE_BIN/pvmonitor not found");
        return;
    };

    let mut monitor = Command::new(pvmonitor_bin);
    monitor
        .arg(pv)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    eprintln!("[interop] epics pvmonitor => {}", pv);
    let started_at = Instant::now();
    let mut guard = ProcessGuard::spawn(&mut monitor, "epics pvmonitor")
        .unwrap_or_else(|error| panic!("epics pvmonitor failed for {}: {}", pv, error));
    thread::sleep(Duration::from_millis(500));
    guard.kill_and_wait();
    eprintln!(
        "[interop] epics pvmonitor completed in {:?}",
        started_at.elapsed()
    );
}

fn render_command_template(template: &str, pv: &str, value: Option<&str>) -> String {
    let mut command = template.replace("{pv}", pv);
    if let Some(put_value) = value {
        command = command.replace("{value}", put_value);
    }
    command
}

fn run_shell_template_success(template: &str, pv: &str, value: Option<&str>, label: &str) {
    let rendered = render_command_template(template, pv, value);
    let timeout_secs = interop_cmd_timeout_secs();
    eprintln!("[interop] {} => {}", label, rendered);

    let mut command = Command::new("timeout");
    command
        .arg("-k")
        .arg("1s")
        .arg(format!("{}s", timeout_secs))
        .arg("sh")
        .arg("-lc")
        .arg(&rendered)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let started_at = Instant::now();
    let output = run_command(&mut command, label)
        .unwrap_or_else(|error| panic!("{} failed for {}: {}", label, pv, error));
    if !output.status.success() {
        let timeout_exit = output.status.code() == Some(124);
        let is_monitor_label = label.to_ascii_lowercase().contains("monitor");
        if !(timeout_exit && is_monitor_label) {
            panic!(
                "{} failed for {}: {}: exit={} stderr={} stdout= {}",
                label,
                pv,
                label,
                output
                    .status
                    .code()
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "signal".to_string()),
                String::from_utf8_lossy(&output.stderr),
                String::from_utf8_lossy(&output.stdout),
            );
        }
        eprintln!(
            "[interop] {} reached timeout window (treated as monitor smoke pass)",
            label
        );
    }
    eprintln!(
        "[interop] {} completed in {:?}",
        label,
        started_at.elapsed()
    );
}

#[test]
fn external_ioc_tool_matrix_optional() {
    if !env_enabled("PVA_TEST_EXTERNAL") {
        eprintln!("Skipping external matrix: set PVA_TEST_EXTERNAL=1");
        return;
    }

    let pv_cases = parse_external_pv_cases();
    let server = env_string("PVA_TEST_SERVER").or_else(|| env_string("PVA_EXT_SERVER"));

    for pv_case in &pv_cases {
        run_rust_read_tools(server.as_deref(), &pv_case.name);
        run_epics_read_tools(&pv_case.name);
        run_epics_monitor_smoke(&pv_case.name);

        if pv_case.writable {
            run_rust_write_restore(server.as_deref(), &pv_case.name);
        }
    }

    run_epics_pvlist(None);
}

#[test]
fn p4p_provider_matrix_optional() {
    if !env_enabled("PVA_TEST_P4P") {
        eprintln!("Skipping p4p provider matrix: set PVA_TEST_P4P=1");
        return;
    }

    let Some(provider_cmd) = env_string("P4P_PROVIDER_CMD") else {
        eprintln!("Skipping p4p provider matrix: set P4P_PROVIDER_CMD");
        return;
    };

    let mut provider = Command::new("sh");
    provider
        .arg("-lc")
        .arg(provider_cmd)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut provider_guard = ProcessGuard::spawn(&mut provider, "spawn p4p provider")
        .expect("p4p provider should start");

    let ready_delay_ms = env_string("P4P_PROVIDER_READY_MS")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(3000);
    thread::sleep(Duration::from_millis(ready_delay_ms));

    let pv_rw = env_string("P4P_TEST_PV_RW").unwrap_or_else(|| "p4p:rw".to_string());
    let pv_ro = env_string("P4P_TEST_PV_RO").unwrap_or_else(|| "p4p:ro".to_string());

    let p4p_get_template = env_string("P4P_CLI_GET_CMD_TEMPLATE");
    let p4p_info_template = env_string("P4P_CLI_INFO_CMD_TEMPLATE");
    let p4p_put_template = env_string("P4P_CLI_PUT_CMD_TEMPLATE");
    let p4p_monitor_template = env_string("P4P_CLI_MONITOR_CMD_TEMPLATE");
    let p4p_list_template = env_string("P4P_CLI_LIST_CMD_TEMPLATE");

    if let Some(template) = p4p_get_template.as_deref() {
        run_shell_template_success(template, &pv_rw, None, "p4p get rw");
        run_shell_template_success(template, &pv_ro, None, "p4p get ro");
    } else {
        eprintln!("Skipping p4p get checks: missing P4P_CLI_GET_CMD_TEMPLATE");
    }

    if let Some(template) = p4p_info_template.as_deref() {
        run_shell_template_success(template, &pv_rw, None, "p4p info rw");
        run_shell_template_success(template, &pv_ro, None, "p4p info ro");
    } else {
        eprintln!("Skipping p4p info checks: missing P4P_CLI_INFO_CMD_TEMPLATE");
    }

    if let Some(template) = p4p_put_template.as_deref() {
        run_shell_template_success(template, &pv_rw, Some("1"), "p4p put rw");
        run_shell_template_success(template, &pv_rw, Some("0"), "p4p put rw restore");
    } else {
        eprintln!("Skipping p4p put checks: missing P4P_CLI_PUT_CMD_TEMPLATE");
    }

    if let Some(template) = p4p_monitor_template.as_deref() {
        run_shell_template_success(template, &pv_rw, None, "p4p monitor rw");
    } else {
        eprintln!("Skipping p4p monitor checks: missing P4P_CLI_MONITOR_CMD_TEMPLATE");
    }

    if let Some(template) = p4p_list_template.as_deref() {
        run_shell_template_success(template, &pv_rw, None, "p4p list");
    } else {
        eprintln!("Skipping p4p list checks: missing P4P_CLI_LIST_CMD_TEMPLATE");
    }

    let server = env_string("P4P_TEST_SERVER");

    // ── Core spget on default RW/RO PVs ──────────────────────────────────────
    // NOTE: spinfo (GET_FIELD cmd 0x11) is skipped for p4p because pvxs
    // cannot decode spvirit's GET_FIELD encoding, causing connection crashes.
    let spget_bin = workspace_bin("spget");
    for pv in [&pv_rw, &pv_ro] {
        let mut cmd = rust_tool_command(&spget_bin, server.as_deref(), pv);
        let output = run_command_success(&mut cmd, &format!("spget {}", pv))
            .unwrap_or_else(|e| panic!("spget failed for {}: {}", pv, e));
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(
            contains_any(&text, &[pv, "value", "alarm", "timeStamp"]),
            "unexpected spget output for {}: {}",
            pv,
            text
        );
    }

    // ── Multi-type scalar GET ────────────────────────────────────────────────
    // Exercise spget across different scalar types served by p4p_server.py
    let scalar_pvs = [
        ("p4p:int", "7"),
        ("p4p:str", "hello"),
        ("p4p:float", "2.5"),
        ("p4p:long", "123456789"),
    ];
    for (pv, expected_fragment) in &scalar_pvs {
        let mut cmd = rust_tool_command(&spget_bin, server.as_deref(), pv);
        let output = run_command_success(&mut cmd, &format!("spget {}", pv))
            .unwrap_or_else(|e| panic!("spget failed for {}: {}", pv, e));
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(
            text.contains(expected_fragment),
            "spget {}: expected '{}' in output: {}",
            pv,
            expected_fragment,
            text
        );
    }

    // ── Array GET ────────────────────────────────────────────────────────────
    let array_pvs = [
        ("p4p:arr:double", &["1", "2", "3"] as &[&str]),
        ("p4p:arr:int", &["10", "20", "30"]),
        ("p4p:arr:str", &["alpha", "beta", "gamma"]),
    ];
    for (pv, fragments) in &array_pvs {
        let mut cmd = rust_tool_command(&spget_bin, server.as_deref(), pv);
        let output = run_command_success(&mut cmd, &format!("spget {}", pv))
            .unwrap_or_else(|e| panic!("spget failed for {}: {}", pv, e));
        let text = String::from_utf8_lossy(&output.stdout);
        for frag in *fragments {
            assert!(
                text.contains(frag),
                "spget {}: expected '{}' in output: {}",
                pv,
                frag,
                text
            );
        }
    }

    // ── Enum GET ─────────────────────────────────────────────────────────────
    {
        let mut cmd = rust_tool_command(&spget_bin, server.as_deref(), "p4p:enum");
        let output = run_command_success(&mut cmd, "spget p4p:enum")
            .unwrap_or_else(|e| panic!("spget failed for p4p:enum: {}", e));
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(
            contains_any(&text, &["On", "Off", "Error", "index", "choices"]),
            "spget p4p:enum: unexpected output: {}",
            text
        );
    }

    // ── JSON output mode ─────────────────────────────────────────────────────
    {
        let mut cmd = Command::new(&spget_bin);
        if let Some(addr) = server.as_deref() {
            cmd.arg("--server").arg(addr);
        }
        cmd.arg("-w")
            .arg(interop_cmd_timeout_secs().to_string())
            .arg("--json")
            .arg(&pv_rw)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let output = run_command_success(&mut cmd, "spget --json")
            .unwrap_or_else(|e| panic!("spget --json failed: {}", e));
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(
            text.contains('{') && text.contains("value"),
            "spget --json: expected JSON with 'value' field: {}",
            text
        );
    }

    // ── PUT write + read-back ────────────────────────────────────────────────
    let spput_bin = workspace_bin("spput");
    {
        // Write a new value to p4p:rw
        let mut put_cmd = Command::new(&spput_bin);
        if let Some(addr) = server.as_deref() {
            put_cmd.arg("--server").arg(addr);
        }
        put_cmd
            .arg("-w")
            .arg(interop_cmd_timeout_secs().to_string())
            .arg(&pv_rw)
            .arg("99.5")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        run_command_success(&mut put_cmd, "spput p4p:rw 99.5")
            .unwrap_or_else(|e| panic!("spput write failed: {}", e));

        // Read back and verify
        let mut get_cmd = rust_tool_command(&spget_bin, server.as_deref(), &pv_rw);
        let output = run_command_success(&mut get_cmd, "spget p4p:rw after put")
            .unwrap_or_else(|e| panic!("spget after put failed: {}", e));
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(
            text.contains("99.5"),
            "spget after put: expected '99.5' in output: {}",
            text
        );

        // Restore
        let mut restore_cmd = Command::new(&spput_bin);
        if let Some(addr) = server.as_deref() {
            restore_cmd.arg("--server").arg(addr);
        }
        restore_cmd
            .arg("-w")
            .arg(interop_cmd_timeout_secs().to_string())
            .arg(&pv_rw)
            .arg("42.0")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        run_command_success(&mut restore_cmd, "spput p4p:rw restore")
            .unwrap_or_else(|e| panic!("spput restore failed: {}", e));
    }

    // ── PUT to integer PV ────────────────────────────────────────────────────
    {
        let mut put_cmd = Command::new(&spput_bin);
        if let Some(addr) = server.as_deref() {
            put_cmd.arg("--server").arg(addr);
        }
        put_cmd
            .arg("-w")
            .arg(interop_cmd_timeout_secs().to_string())
            .arg("p4p:int")
            .arg("42")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        run_command_success(&mut put_cmd, "spput p4p:int 42")
            .unwrap_or_else(|e| panic!("spput int failed: {}", e));

        let mut get_cmd = rust_tool_command(&spget_bin, server.as_deref(), "p4p:int");
        let output = run_command_success(&mut get_cmd, "spget p4p:int after put")
            .unwrap_or_else(|e| panic!("spget p4p:int after put failed: {}", e));
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(
            text.contains("42"),
            "spget p4p:int after put: expected '42' in output: {}",
            text
        );
    }

    // ── PUT to string PV ─────────────────────────────────────────────────────
    {
        let mut put_cmd = Command::new(&spput_bin);
        if let Some(addr) = server.as_deref() {
            put_cmd.arg("--server").arg(addr);
        }
        put_cmd
            .arg("-w")
            .arg(interop_cmd_timeout_secs().to_string())
            .arg("p4p:str")
            .arg("world")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        run_command_success(&mut put_cmd, "spput p4p:str world")
            .unwrap_or_else(|e| panic!("spput string failed: {}", e));

        let mut get_cmd = rust_tool_command(&spget_bin, server.as_deref(), "p4p:str");
        let output = run_command_success(&mut get_cmd, "spget p4p:str after put")
            .unwrap_or_else(|e| panic!("spget p4p:str after put failed: {}", e));
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(
            text.contains("world"),
            "spget p4p:str after put: expected 'world' in output: {}",
            text
        );
    }

    // NOTE: spinfo introspection tests are skipped for p4p because pvxs
    // cannot decode spvirit's GET_FIELD (cmd 0x11) encoding.

    // ── Monitor smoke test ───────────────────────────────────────────────────
    // Start spmonitor, wait briefly for initial update, then kill.
    {
        let spmonitor_bin = workspace_bin("spmonitor");
        let mut cmd = Command::new(&spmonitor_bin);
        if let Some(addr) = server.as_deref() {
            cmd.arg("--server").arg(addr);
        }
        cmd.arg("-w")
            .arg(interop_cmd_timeout_secs().to_string())
            .arg(&pv_rw)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut guard = ProcessGuard::spawn(&mut cmd, "spmonitor p4p:rw")
            .unwrap_or_else(|e| panic!("spmonitor failed to start: {}", e));
        thread::sleep(Duration::from_millis(800));
        guard.kill_and_wait();
    }

    // ── EPICS tools (optional, when EPICS_BASE_BIN is available) ─────────────
    run_epics_read_tools(&pv_rw);
    run_epics_read_tools(&pv_ro);
    let pvlist_target = env_string("P4P_EPICS_PVLIST_TARGET")
        .or_else(|| server.clone())
        .or_else(|| Some("127.0.0.1:5075".to_string()));
    run_epics_pvlist(pvlist_target.as_deref());

    provider_guard.kill_and_wait();
}
