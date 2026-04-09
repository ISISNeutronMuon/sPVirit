use spvirit_server::PvaServer;
use spvirit_types::{ScalarArrayValue, ScalarValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = PvaServer::builder()
        .waveform("SIM:SPECTRUM", ScalarArrayValue::F64(vec![0.0; 1024]))
        .build();

    let store = server.store().clone();

    tokio::spawn(async move {
        let mut tick = 0u64;
        loop {
            let first_val = ((tick as f64) * 0.01).sin();
            store
                .set_value("SIM:SPECTRUM", ScalarValue::F64(first_val))
                .await;
            tick += 1;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });

    server.run().await
}
