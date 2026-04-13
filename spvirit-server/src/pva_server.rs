//! High-level PVAccess server — builder pattern for typed records.
//!
//! # Example
//!
//! ```rust,ignore
//! use spvirit_server::PvaServer;
//!
//! let server = PvaServer::builder()
//!     .ai("SIM:TEMPERATURE", 22.5)
//!     .ao("SIM:SETPOINT", 25.0)
//!     .bo("SIM:ENABLE", false)
//!     .build();
//!
//! server.run().await?;
//! ```

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use regex::Regex;
use tracing::info;

use spvirit_types::{NtScalar, NtScalarArray, ScalarArrayValue, ScalarValue};

use crate::db::{load_db, parse_db};
use crate::handler::PvListMode;
use crate::monitor::MonitorRegistry;
use crate::server::{run_pva_server_with_registry, PvaServerConfig};
use crate::simple_store::{LinkDef, OnPutCallback, ScanCallback, SimplePvStore};
use crate::types::{
    DbCommonState, OutputMode, RecordData, RecordInstance, RecordType,
};

// ─── PvaServerBuilder ────────────────────────────────────────────────────

/// Builder for [`PvaServer`].
///
/// ```rust,ignore
/// let server = PvaServer::builder()
///     .ai("TEMP:READBACK", 22.5)
///     .ao("TEMP:SETPOINT", 25.0)
///     .bo("HEATER:ON", false)
///     .port(5075)
///     .build();
/// ```
pub struct PvaServerBuilder {
    records: HashMap<String, RecordInstance>,
    on_put: HashMap<String, OnPutCallback>,
    scans: Vec<(String, Duration, ScanCallback)>,
    links: Vec<LinkDef>,
    tcp_port: u16,
    udp_port: u16,
    listen_ip: Option<IpAddr>,
    advertise_ip: Option<IpAddr>,
    compute_alarms: bool,
    beacon_period_secs: u64,
    conn_timeout: Duration,
    pvlist_mode: PvListMode,
    pvlist_max: usize,
    pvlist_allow_pattern: Option<Regex>,
}

impl PvaServerBuilder {
    fn new() -> Self {
        Self {
            records: HashMap::new(),
            on_put: HashMap::new(),
            scans: Vec::new(),
            links: Vec::new(),
            tcp_port: 5075,
            udp_port: 5076,
            listen_ip: None,
            advertise_ip: None,
            compute_alarms: false,
            beacon_period_secs: 15,
            conn_timeout: Duration::from_secs(64000),
            pvlist_mode: PvListMode::List,
            pvlist_max: 1024,
            pvlist_allow_pattern: None,
        }
    }

    // ─── Typed record constructors ───────────────────────────────────

    /// Add an `ai` (analog input, read-only) record.
    pub fn ai(mut self, name: impl Into<String>, initial: f64) -> Self {
        let name = name.into();
        self.records.insert(
            name.clone(),
            make_scalar_record(&name, RecordType::Ai, ScalarValue::F64(initial)),
        );
        self
    }

    /// Add an `ao` (analog output, writable) record.
    pub fn ao(mut self, name: impl Into<String>, initial: f64) -> Self {
        let name = name.into();
        self.records.insert(
            name.clone(),
            make_output_record(&name, RecordType::Ao, ScalarValue::F64(initial)),
        );
        self
    }

    /// Add a `bi` (binary input, read-only) record.
    pub fn bi(mut self, name: impl Into<String>, initial: bool) -> Self {
        let name = name.into();
        self.records.insert(
            name.clone(),
            make_scalar_record(&name, RecordType::Bi, ScalarValue::Bool(initial)),
        );
        self
    }

    /// Add a `bo` (binary output, writable) record.
    pub fn bo(mut self, name: impl Into<String>, initial: bool) -> Self {
        let name = name.into();
        self.records.insert(
            name.clone(),
            make_output_record(&name, RecordType::Bo, ScalarValue::Bool(initial)),
        );
        self
    }

    /// Add a `stringin` (string input, read-only) record.
    pub fn string_in(mut self, name: impl Into<String>, initial: impl Into<String>) -> Self {
        let name = name.into();
        self.records.insert(
            name.clone(),
            make_scalar_record(&name, RecordType::StringIn, ScalarValue::Str(initial.into())),
        );
        self
    }

    /// Add a `stringout` (string output, writable) record.
    pub fn string_out(mut self, name: impl Into<String>, initial: impl Into<String>) -> Self {
        let name = name.into();
        self.records.insert(
            name.clone(),
            make_output_record(&name, RecordType::StringOut, ScalarValue::Str(initial.into())),
        );
        self
    }

    /// Add a generic structure PV with the given initial value.
    ///
    /// Use this for QSRV group PVs and other composite PVs whose payload
    /// does not fit a canonical Normative Type. The structure can carry
    /// nested scalars, scalar arrays, and nested structures (see
    /// [`spvirit_types::NtStructure`] for the exact envelope).
    pub fn nt_structure(
        mut self,
        name: impl Into<String>,
        initial: spvirit_types::NtStructure,
    ) -> Self {
        let name = name.into();
        self.records.insert(
            name.clone(),
            RecordInstance {
                name: name.clone(),
                record_type: RecordType::NtStructure,
                common: DbCommonState::default(),
                data: RecordData::NtStructure { nt: initial },
                raw_fields: HashMap::new(),
            },
        );
        self
    }

    /// Add a `waveform` record (array) with the given initial data.
    pub fn waveform(
        mut self,
        name: impl Into<String>,
        data: ScalarArrayValue,
    ) -> Self {
        let name = name.into();
        let ftvl = data.type_label().trim_end_matches("[]").to_string();
        let nelm = data.len();
        self.records.insert(
            name.clone(),
            RecordInstance {
                name: name.clone(),
                record_type: RecordType::Waveform,
                common: DbCommonState::default(),
                data: RecordData::Waveform {
                    nt: NtScalarArray::from_value(data),
                    inp: None,
                    ftvl,
                    nelm,
                    nord: nelm,
                },
                raw_fields: HashMap::new(),
            },
        );
        self
    }

    // ─── .db file loading ────────────────────────────────────────────

    /// Load records from an EPICS `.db` file.
    pub fn db_file(mut self, path: impl AsRef<str>) -> Self {
        match load_db(path.as_ref()) {
            Ok(records) => {
                self.records.extend(records);
            }
            Err(e) => {
                tracing::error!("Failed to load db file '{}': {}", path.as_ref(), e);
            }
        }
        self
    }

    /// Parse records from an EPICS `.db` string.
    pub fn db_string(mut self, content: &str) -> Self {
        match parse_db(content) {
            Ok(records) => {
                self.records.extend(records);
            }
            Err(e) => {
                tracing::error!("Failed to parse db string: {}", e);
            }
        }
        self
    }

    // ─── Callbacks ───────────────────────────────────────────────────

    /// Register a callback invoked when a PUT is applied to the named PV.
    pub fn on_put<F>(mut self, name: impl Into<String>, callback: F) -> Self
    where
        F: Fn(&str, &spvirit_codec::spvd_decode::DecodedValue) + Send + Sync + 'static,
    {
        self.on_put.insert(name.into(), Arc::new(callback));
        self
    }

    /// Register a periodic scan callback that produces a new value for a PV.
    pub fn scan<F>(
        mut self,
        name: impl Into<String>,
        period: Duration,
        callback: F,
    ) -> Self
    where
        F: Fn(&str) -> ScalarValue + Send + Sync + 'static,
    {
        self.scans
            .push((name.into(), period, Arc::new(callback)));
        self
    }

    /// Link an output PV to one or more input PVs.
    ///
    /// Whenever any input PV changes (via `set_value`, protocol PUT, or
    /// another link), the `compute` callback is invoked with the current
    /// values of **all** inputs (in order) and the result is written to
    /// the output PV.
    ///
    /// ```rust,ignore
    /// .link("CALC:SUM", &["INPUT:A", "INPUT:B"], |values| {
    ///     let a = values[0].as_f64().unwrap_or(0.0);
    ///     let b = values[1].as_f64().unwrap_or(0.0);
    ///     ScalarValue::F64(a + b)
    /// })
    /// ```
    pub fn link<F>(
        mut self,
        output: impl Into<String>,
        inputs: &[&str],
        compute: F,
    ) -> Self
    where
        F: Fn(&[ScalarValue]) -> ScalarValue + Send + Sync + 'static,
    {
        self.links.push(LinkDef {
            output: output.into(),
            inputs: inputs.iter().map(|s| s.to_string()).collect(),
            compute: Arc::new(compute),
        });
        self
    }

    // ─── Configuration ───────────────────────────────────────────────

    /// Set the TCP port (default 5075).
    pub fn port(mut self, port: u16) -> Self {
        self.tcp_port = port;
        self
    }

    /// Set the UDP search port (default 5076).
    pub fn udp_port(mut self, port: u16) -> Self {
        self.udp_port = port;
        self
    }

    /// Set the IP address to listen on.
    pub fn listen_ip(mut self, ip: IpAddr) -> Self {
        self.listen_ip = Some(ip);
        self
    }

    /// Set the IP address to advertise in search responses.
    pub fn advertise_ip(mut self, ip: IpAddr) -> Self {
        self.advertise_ip = Some(ip);
        self
    }

    /// Enable alarm computation from limits.
    pub fn compute_alarms(mut self, enabled: bool) -> Self {
        self.compute_alarms = enabled;
        self
    }

    /// Set the beacon broadcast period in seconds (default 15).
    pub fn beacon_period(mut self, secs: u64) -> Self {
        self.beacon_period_secs = secs;
        self
    }

    /// Set the idle connection timeout (default ~18 hours).
    pub fn conn_timeout(mut self, timeout: Duration) -> Self {
        self.conn_timeout = timeout;
        self
    }

    /// Set the PV list mode (default [`PvListMode::List`]).
    pub fn pvlist_mode(mut self, mode: PvListMode) -> Self {
        self.pvlist_mode = mode;
        self
    }

    /// Set the maximum number of PV names in pvlist responses (default 1024).
    pub fn pvlist_max(mut self, max: usize) -> Self {
        self.pvlist_max = max;
        self
    }

    /// Set a regex filter for PV names exposed by pvlist.
    pub fn pvlist_allow_pattern(mut self, pattern: Regex) -> Self {
        self.pvlist_allow_pattern = Some(pattern);
        self
    }

    /// Build the [`PvaServer`].
    pub fn build(self) -> PvaServer {
        let store = Arc::new(SimplePvStore::new(
            self.records,
            self.on_put,
            self.links,
            self.compute_alarms,
        ));

        let mut config = PvaServerConfig::default();
        config.tcp_port = self.tcp_port;
        config.udp_port = self.udp_port;
        config.compute_alarms = self.compute_alarms;
        if let Some(ip) = self.listen_ip {
            config.listen_ip = ip;
        }
        config.advertise_ip = self.advertise_ip;
        config.beacon_period_secs = self.beacon_period_secs;
        config.conn_timeout = self.conn_timeout;
        config.pvlist_mode = self.pvlist_mode;
        config.pvlist_max = self.pvlist_max;
        config.pvlist_allow_pattern = self.pvlist_allow_pattern;

        PvaServer {
            store,
            config,
            scans: self.scans,
        }
    }
}

// ─── PvaServer ───────────────────────────────────────────────────────────

/// High-level PVAccess server.
///
/// Built via [`PvaServer::builder()`] with typed record constructors,
/// `.db_file()` loading, `.on_put()` / `.scan()` callbacks, and a
/// simple `.run()` to start serving.
///
/// ```rust,ignore
/// let server = PvaServer::builder()
///     .ai("SIM:TEMP", 22.5)
///     .ao("SIM:SP", 25.0)
///     .build();
///
/// // Read/write PVs from another task:
/// let store = server.store();
/// store.set_value("SIM:TEMP", ScalarValue::F64(23.1)).await;
///
/// server.run().await?;
/// ```
pub struct PvaServer {
    store: Arc<SimplePvStore>,
    config: PvaServerConfig,
    scans: Vec<(String, Duration, ScanCallback)>,
}

impl PvaServer {
    /// Create a builder for configuring a [`PvaServer`].
    pub fn builder() -> PvaServerBuilder {
        PvaServerBuilder::new()
    }

    /// Get a reference to the underlying store for runtime get/put.
    pub fn store(&self) -> &Arc<SimplePvStore> {
        &self.store
    }

    /// Start the PVA server (UDP search + TCP handler + beacon + scan tasks).
    ///
    /// This blocks until the server is shut down or an error occurs.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        // Create the monitor registry early so scan tasks can notify
        // PVAccess monitor clients when values change.
        let registry = Arc::new(MonitorRegistry::new());
        self.store.set_registry(registry.clone()).await;

        // Spawn scan tasks.
        for (name, period, callback) in &self.scans {
            let store = self.store.clone();
            let name = name.clone();
            let period = *period;
            let callback = callback.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(period);
                loop {
                    interval.tick().await;
                    let new_val = callback(&name);
                    store.set_value(&name, new_val).await;
                }
            });
        }

        let pv_count = self.store.pv_names().await.len();
        info!(
            "PvaServer starting: {} PVs on port {}",
            pv_count, self.config.tcp_port
        );

        run_pva_server_with_registry(self.store, self.config, registry).await
    }
}

// ─── Record construction helpers ─────────────────────────────────────────

fn make_scalar_record(
    name: &str,
    record_type: RecordType,
    value: ScalarValue,
) -> RecordInstance {
    let nt = NtScalar::from_value(value);
    let data = match record_type {
        RecordType::Ai => RecordData::Ai {
            nt,
            inp: None,
            siml: None,
            siol: None,
            simm: false,
        },
        RecordType::Bi => RecordData::Bi {
            nt,
            inp: None,
            znam: "Off".to_string(),
            onam: "On".to_string(),
            siml: None,
            siol: None,
            simm: false,
        },
        RecordType::StringIn => RecordData::StringIn {
            nt,
            inp: None,
            siml: None,
            siol: None,
            simm: false,
        },
        _ => panic!("make_scalar_record: unsupported type {record_type:?}"),
    };
    RecordInstance {
        name: name.to_string(),
        record_type,
        common: DbCommonState::default(),
        data,
        raw_fields: HashMap::new(),
    }
}

fn make_output_record(
    name: &str,
    record_type: RecordType,
    value: ScalarValue,
) -> RecordInstance {
    let nt = NtScalar::from_value(value);
    let data = match record_type {
        RecordType::Ao => RecordData::Ao {
            nt,
            out: None,
            dol: None,
            omsl: OutputMode::Supervisory,
            drvl: None,
            drvh: None,
            oroc: None,
            siml: None,
            siol: None,
            simm: false,
        },
        RecordType::Bo => RecordData::Bo {
            nt,
            out: None,
            dol: None,
            omsl: OutputMode::Supervisory,
            znam: "Off".to_string(),
            onam: "On".to_string(),
            siml: None,
            siol: None,
            simm: false,
        },
        RecordType::StringOut => RecordData::StringOut {
            nt,
            out: None,
            dol: None,
            omsl: OutputMode::Supervisory,
            siml: None,
            siol: None,
            simm: false,
        },
        _ => panic!("make_output_record: unsupported type {record_type:?}"),
    };
    RecordInstance {
        name: name.to_string(),
        record_type,
        common: DbCommonState::default(),
        data,
        raw_fields: HashMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_creates_records() {
        let server = PvaServer::builder()
            .ai("T:AI", 1.0)
            .ao("T:AO", 2.0)
            .bi("T:BI", true)
            .bo("T:BO", false)
            .string_in("T:SI", "hello")
            .string_out("T:SO", "world")
            .build();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let names = rt.block_on(server.store.pv_names());
        assert_eq!(names.len(), 6);
    }

    #[test]
    fn builder_defaults() {
        let server = PvaServer::builder().build();
        assert_eq!(server.config.tcp_port, 5075);
        assert_eq!(server.config.udp_port, 5076);
        assert!(!server.config.compute_alarms);
    }

    #[test]
    fn builder_port_override() {
        let server = PvaServer::builder().port(9075).udp_port(9076).build();
        assert_eq!(server.config.tcp_port, 9075);
        assert_eq!(server.config.udp_port, 9076);
    }

    #[test]
    fn builder_db_string() {
        let db = r#"
            record(ai, "TEST:VAL") {
                field(VAL, "3.14")
            }
        "#;
        let server = PvaServer::builder().db_string(db).build();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(rt.block_on(server.store.get_value("TEST:VAL")).is_some());
    }

    #[test]
    fn builder_waveform() {
        let data = ScalarArrayValue::F64(vec![1.0, 2.0, 3.0]);
        let server = PvaServer::builder()
            .waveform("T:WF", data)
            .build();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let names = rt.block_on(server.store.pv_names());
        assert!(names.contains(&"T:WF".to_string()));
    }

    #[test]
    fn builder_scan_callback() {
        let server = PvaServer::builder()
            .ai("SCAN:V", 0.0)
            .scan("SCAN:V", Duration::from_secs(1), |_name| {
                ScalarValue::F64(42.0)
            })
            .build();
        assert_eq!(server.scans.len(), 1);
    }

    #[test]
    fn builder_on_put_callback() {
        let server = PvaServer::builder()
            .ao("PUT:V", 0.0)
            .on_put("PUT:V", |_name, _val| {})
            .build();
        // on_put is stored in the SimplePvStore, not directly inspectable,
        // but the server built without panic.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(rt.block_on(server.store.get_value("PUT:V")).is_some());
    }

    #[test]
    fn store_runtime_get_set() {
        let server = PvaServer::builder()
            .ao("RT:V", 0.0)
            .build();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let store = server.store().clone();
        rt.block_on(async {
            assert_eq!(
                store.get_value("RT:V").await,
                Some(ScalarValue::F64(0.0))
            );
            store.set_value("RT:V", ScalarValue::F64(99.0)).await;
            assert_eq!(
                store.get_value("RT:V").await,
                Some(ScalarValue::F64(99.0))
            );
        });
    }

    #[test]
    fn link_propagates_on_set_value() {
        let server = PvaServer::builder()
            .ao("INPUT:A", 1.0)
            .ao("INPUT:B", 2.0)
            .ai("CALC:SUM", 0.0)
            .link("CALC:SUM", &["INPUT:A", "INPUT:B"], |values| {
                let a = match &values[0] { ScalarValue::F64(v) => *v, _ => 0.0 };
                let b = match &values[1] { ScalarValue::F64(v) => *v, _ => 0.0 };
                ScalarValue::F64(a + b)
            })
            .build();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let store = server.store().clone();
        rt.block_on(async {
            // Writing INPUT:A should recompute CALC:SUM = 10 + 2.
            store.set_value("INPUT:A", ScalarValue::F64(10.0)).await;
            assert_eq!(
                store.get_value("CALC:SUM").await,
                Some(ScalarValue::F64(12.0))
            );

            // Writing INPUT:B should recompute CALC:SUM = 10 + 5.
            store.set_value("INPUT:B", ScalarValue::F64(5.0)).await;
            assert_eq!(
                store.get_value("CALC:SUM").await,
                Some(ScalarValue::F64(15.0))
            );
        });
    }
}
