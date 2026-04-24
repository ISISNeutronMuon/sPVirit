//! # Aggregate / Computed Source
//!
//! A read-only source that publishes derived PVs computed from the
//! built-in store's raw values.  Demonstrates how a source can reference
//! the server's `SimplePvStore` to produce aggregated statistics.
//!
//! The server has three raw temperature PVs (`RAW:T1`, `RAW:T2`, `RAW:T3`)
//! updated by a background task.  The `AggregateSource` publishes:
//!
//! * `AGG:MEAN` — arithmetic mean of the three
//! * `AGG:MIN`  — minimum
//! * `AGG:MAX`  — maximum
//!
//! Because the aggregate source is registered at order 10 (after the
//! built-in store at order 0), `RAW:*` PVs are resolved by the built-in
//! store, while `AGG:*` PVs go to `AggregateSource`.
//!
//! Run with:
//! ```sh
//! cargo run --example aggregate_source
//! ```
//! Then try:
//! ```sh
//! cargo run --bin spget -- RAW:T1 RAW:T2 RAW:T3 AGG:MEAN AGG:MIN AGG:MAX
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use spvirit_codec::spvd_decode::{DecodedValue, FieldDesc, FieldType, StructureDesc, TypeCode};
use spvirit_server::pvstore::{PvInfo, Source};
use spvirit_server::{PvaServer, SimplePvStore};
use spvirit_types::{NtPayload, NtScalar, ScalarValue};
use tokio::sync::mpsc;

// ─── AggregateSource ─────────────────────────────────────────────────────────

/// A read-only source that computes aggregate statistics from raw input PVs.
struct AggregateSource {
    /// Reference to the built-in store holding the raw PVs.
    store: Arc<SimplePvStore>,
    /// Names of the input PVs to aggregate.
    inputs: Vec<String>,
    /// Published aggregate PV names.
    outputs: Vec<&'static str>,
}

impl AggregateSource {
    fn new(store: Arc<SimplePvStore>, inputs: Vec<String>) -> Self {
        Self {
            store,
            inputs,
            outputs: vec!["AGG:MEAN", "AGG:MIN", "AGG:MAX"],
        }
    }

    fn is_output(&self, name: &str) -> bool {
        self.outputs.contains(&name)
    }

    /// Read all input PVs and return their numeric values.
    async fn read_inputs(&self) -> Vec<f64> {
        let mut vals = Vec::new();
        for name in &self.inputs {
            if let Some(ScalarValue::F64(v)) = self.store.get_value(name).await {
                vals.push(v);
            }
        }
        vals
    }

    /// Compute the requested aggregate.
    async fn compute(&self, name: &str) -> Option<f64> {
        let vals = self.read_inputs().await;
        if vals.is_empty() {
            return Some(0.0);
        }
        match name {
            "AGG:MEAN" => Some(vals.iter().sum::<f64>() / vals.len() as f64),
            "AGG:MIN" => vals.iter().copied().reduce(f64::min),
            "AGG:MAX" => vals.iter().copied().reduce(f64::max),
            _ => None,
        }
    }
}

fn f64_desc() -> StructureDesc {
    StructureDesc {
        struct_id: Some("epics:nt/NTScalar:1.0".to_string()),
        fields: vec![FieldDesc {
            name: "value".to_string(),
            field_type: FieldType::Scalar(TypeCode::Float64),
        }],
    }
}

impl Source for AggregateSource {
    fn claim(&self, name: &str) -> Pin<Box<dyn Future<Output = Option<PvInfo>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            if self.is_output(&name) {
                Some(PvInfo {
                    descriptor: f64_desc(),
                    writable: false,
                })
            } else {
                None
            }
        })
    }

    fn get(&self, name: &str) -> Pin<Box<dyn Future<Output = Option<NtPayload>> + Send + '_>> {
        let name = name.to_string();
        Box::pin(async move {
            let val = self.compute(&name).await?;
            Some(NtPayload::Scalar(NtScalar::from_value(ScalarValue::F64(
                val,
            ))))
        })
    }

    fn put(
        &self,
        _name: &str,
        _value: &DecodedValue,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<(String, NtPayload)>, String>> + Send + '_>> {
        Box::pin(async { Err("aggregate PVs are read-only".to_string()) })
    }

    fn subscribe(
        &self,
        _name: &str,
    ) -> Pin<Box<dyn Future<Output = Option<mpsc::Receiver<NtPayload>>> + Send + '_>> {
        // Aggregate values are computed on GET; subscription is not supported.
        Box::pin(async { None })
    }

    fn names(&self) -> Pin<Box<dyn Future<Output = Vec<String>> + Send + '_>> {
        Box::pin(async move { self.outputs.iter().map(|s| s.to_string()).collect() })
    }
}

// ─── main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = PvaServer::builder()
        .ai("RAW:T1", 20.0)
        .ai("RAW:T2", 22.0)
        .ai("RAW:T3", 24.0)
        .build();

    let store = server.store().clone();
    let inputs = vec!["RAW:T1".into(), "RAW:T2".into(), "RAW:T3".into()];

    // Register the aggregate source *after* build so it can reference the store.
    let agg = Arc::new(AggregateSource::new(store.clone(), inputs));
    server.add_source("aggregates", 10, agg);

    // Background task: simulate temperature fluctuations
    tokio::spawn(async move {
        let mut tick = 0u64;
        loop {
            let phase = tick as f64 * 0.3;
            store
                .set_value("RAW:T1", ScalarValue::F64(20.0 + 2.0 * phase.sin()))
                .await;
            store
                .set_value("RAW:T2", ScalarValue::F64(22.0 + 1.5 * (phase * 1.3).sin()))
                .await;
            store
                .set_value("RAW:T3", ScalarValue::F64(24.0 + 3.0 * (phase * 0.7).sin()))
                .await;
            tick += 1;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    println!("Aggregate source server running on port 5075");
    println!("  Raw PVs:       RAW:T1, RAW:T2, RAW:T3");
    println!("  Aggregate PVs: AGG:MEAN, AGG:MIN, AGG:MAX");

    server.run().await
}
