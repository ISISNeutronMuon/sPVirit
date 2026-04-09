use spvirit_server::PvaServer;
use spvirit_types::ScalarValue;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    static TICK: AtomicU64 = AtomicU64::new(0);

    let server = PvaServer::builder()
        .ai("SIM:TEMPERATURE", 22.5)
        .scan("SIM:TEMPERATURE", Duration::from_secs(1), |_pv| {
            let t = TICK.fetch_add(1, Ordering::Relaxed) as f64;
            ScalarValue::F64(22.5 + (t * 0.1).sin())
        })
        .build();

    server.run().await
}
