//! generic_structure — exposes a single `NtPayload::Structure` PV.
//!
//! Demonstrates that the 0.2.0 `NtStructure` extension works end-to-end
//! through `SimplePvStore` without needing the QSRV bridge:
//!
//!   1. The `BEAMLINE:GROUP` PV is registered as a generic structure with
//!      a temperature scalar, a current array, and a nested alarm
//!      structure.
//!   2. A background task mutates the values every second using
//!      `SimplePvStore::put_nt(...)`.
//!   3. Run `spget BEAMLINE:GROUP` (or `spmonitor`) against this server
//!      to see the wire-encoded structure decode on the client.
//!
//! ```text
//! cargo run -p spvirit-server --example generic_structure
//! cargo run -p spvirit-tools --bin spget -- BEAMLINE:GROUP
//! ```

use std::time::Duration;

use spvirit_server::PvaServer;
use spvirit_types::{NtField, NtPayload, NtStructure, ScalarArrayValue, ScalarValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let initial = build_structure(0);

    let server = PvaServer::builder()
        .nt_structure("BEAMLINE:GROUP", initial)
        .build();

    let store = server.store().clone();
    tokio::spawn(async move {
        let mut tick: u64 = 0;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            tick = tick.wrapping_add(1);
            store
                .put_nt("BEAMLINE:GROUP", NtPayload::Structure(build_structure(tick)))
                .await;
        }
    });

    server.run().await
}

fn build_structure(tick: u64) -> NtStructure {
    let mut alarm = NtStructure::new("alarm_t");
    let severity: i32 = if tick % 5 == 0 { 1 } else { 0 };
    alarm.push("severity", NtField::Scalar(ScalarValue::I32(severity)));
    alarm.push("status", NtField::Scalar(ScalarValue::I32(0)));
    alarm.push(
        "message",
        NtField::Scalar(ScalarValue::Str(if severity == 0 {
            String::new()
        } else {
            "minor".to_string()
        })),
    );

    let mut group = NtStructure::new("beamline:group/v1");
    let temp = 22.0_f64 + (tick as f64 * 0.5).sin();
    group.push("temperature", NtField::Scalar(ScalarValue::F64(temp)));
    group.push(
        "currents",
        NtField::ScalarArray(ScalarArrayValue::F64(
            (0..4).map(|i| (tick as f64 + i as f64) * 0.1).collect(),
        )),
    );
    group.push("alarm", NtField::Structure(alarm));
    group
}
