use spvirit_server::PvaServer;
use spvirit_types::ScalarValue;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = PvaServer::builder()
        .ai("SIM:TEMPERATURE", 22.5)
        .ao("SIM:SETPOINT", 25.0)
        .build();

    let store = server.store().clone();

    tokio::spawn(async move {
        loop {
            if let Some(sp) = store.get_value("SIM:SETPOINT").await {
                println!("Current setpoint: {sp:?}");
            }
            store
                .set_value("SIM:TEMPERATURE", ScalarValue::F64(23.1))
                .await;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    server.run().await
}
