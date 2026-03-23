use std::net::{TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// Locate a workspace binary by name.
pub fn workspace_bin(name: &str) -> String {
    let ext = if cfg!(windows) { ".exe" } else { "" };
    let test_exe = std::env::current_exe().expect("cannot locate test executable");
    test_exe
        .parent().unwrap()
        .parent().unwrap()
        .join(format!("{name}{ext}"))
        .to_string_lossy()
        .to_string()
}

pub fn env_enabled(var_name: &str) -> bool {
    std::env::var(var_name)
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            normalized == "1" || normalized == "true" || normalized == "yes"
        })
        .unwrap_or(false)
}

pub fn env_string(var_name: &str) -> Option<String> {
    let value = std::env::var(var_name).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub fn free_tcp_port() -> Option<u16> {
    TcpListener::bind("127.0.0.1:0")
        .ok()
        .and_then(|listener| listener.local_addr().ok())
        .map(|address| address.port())
}

pub fn free_udp_port() -> Option<u16> {
    UdpSocket::bind("127.0.0.1:0")
        .ok()
        .and_then(|socket| socket.local_addr().ok())
        .map(|address| address.port())
}

pub fn tool_path_from_base(env_var: &str, tool_name: &str) -> Option<PathBuf> {
    let base = env_string(env_var)?;
    let candidate = PathBuf::from(base).join(tool_name);
    if candidate.exists() {
        Some(candidate)
    } else {
        None
    }
}

pub fn run_command(command: &mut Command, label: &str) -> Result<Output, String> {
    command
        .output()
        .map_err(|error| format!("{}: command failed to start: {}", label, error))
}

pub fn run_command_with_timeout(
    command: &mut Command,
    label: &str,
    timeout: Duration,
) -> Result<Output, String> {
    let mut child = command
        .spawn()
        .map_err(|error| format!("{}: command failed to start: {}", label, error))?;
    let started_at = Instant::now();

    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                return child
                    .wait_with_output()
                    .map_err(|error| format!("{}: wait_with_output failed: {}", label, error));
            }
            Ok(None) => {
                if started_at.elapsed() >= timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!(
                        "{}: timed out after {:?}",
                        label,
                        timeout,
                    ));
                }
                thread::sleep(Duration::from_millis(50));
            }
            Err(error) => {
                let _ = child.kill();
                return Err(format!("{}: try_wait failed: {}", label, error));
            }
        }
    }
}

pub fn run_command_success(command: &mut Command, label: &str) -> Result<Output, String> {
    let output = run_command(command, label)?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(format!(
            "{}: exit={} stderr={} stdout={}",
            label,
            output
                .status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "signal".to_string()),
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout),
        ))
    }
}

pub fn run_command_success_with_timeout(
    command: &mut Command,
    label: &str,
    timeout: Duration,
) -> Result<Output, String> {
    let output = run_command_with_timeout(command, label, timeout)?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(format!(
            "{}: exit={} stderr={} stdout={}",
            label,
            output
                .status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "signal".to_string()),
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout),
        ))
    }
}

pub fn write_temp_db_file(db_prefix: &str, db_contents: &str) -> Result<PathBuf, String> {
    let path = std::env::temp_dir().join(format!("{}_{}.db", db_prefix, std::process::id()));
    std::fs::write(&path, db_contents).map_err(|error| format!("write temp db: {}", error))?;
    Ok(path)
}

pub struct ProcessGuard {
    child: Child,
}

impl ProcessGuard {
    pub fn spawn(command: &mut Command, label: &str) -> Result<Self, String> {
        let child = command
            .spawn()
            .map_err(|error| format!("{}: spawn failed: {}", label, error))?;
        Ok(Self { child })
    }

    pub fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }

    pub fn kill_and_wait(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        self.kill_and_wait();
    }
}

pub struct LocalServerFixture {
    pub db_path: PathBuf,
    pub tcp_port: u16,
    pub udp_port: u16,
    _server: ProcessGuard,
}

impl LocalServerFixture {
    pub fn spawn(db_contents: &str, extra_args: &[&str]) -> Result<Self, String> {
        let tcp_port = free_tcp_port().ok_or_else(|| "no free TCP port".to_string())?;
        let udp_port = free_udp_port().ok_or_else(|| "no free UDP port".to_string())?;
        let db_path = write_temp_db_file("pva_interop", db_contents)?;

        let server_bin = workspace_bin("spvirit_server");
        let mut command = Command::new(server_bin);
        command
            .arg("--db-file")
            .arg(&db_path)
            .arg("--listen-addr")
            .arg("127.0.0.1")
            .arg("--tcp-port")
            .arg(tcp_port.to_string())
            .arg("--udp-port")
            .arg(udp_port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        for arg in extra_args {
            command.arg(arg);
        }

        let server = ProcessGuard::spawn(&mut command, "spawn spvirit_server")?;
        thread::sleep(Duration::from_millis(400));

        Ok(Self {
            db_path,
            tcp_port,
            udp_port,
            _server: server,
        })
    }

    pub fn epics_env(&self) -> Vec<(String, String)> {
        vec![
            ("EPICS_PVA_ADDR_LIST".to_string(), "127.0.0.1".to_string()),
            ("EPICS_PVA_AUTO_ADDR_LIST".to_string(), "NO".to_string()),
            (
                "EPICS_PVA_BROADCAST_PORT".to_string(),
                self.udp_port.to_string(),
            ),
            ("EPICS_PVA_CONN_TMO".to_string(), "5".to_string()),
        ]
    }

    pub fn server_addr(&self) -> String {
        format!("127.0.0.1:{}", self.tcp_port)
    }
}

impl Drop for LocalServerFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.db_path);
    }
}

pub fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

pub fn path_exists(path: &Path) -> bool {
    path.exists()
}