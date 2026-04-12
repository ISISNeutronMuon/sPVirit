use spvirit_server::PvaServer;
use spvirit_types::{NtPayload, NtScalar, NtScalarArray, ScalarArrayValue, ScalarValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = PvaServer::builder()
        .ao("SIM:TEMP", 22.5)
        .waveform("SIM:SPECTRUM", ScalarArrayValue::F64(vec![0.0; 8]))
        .build();

    let store = server.store().clone();

    tokio::spawn(async move {
        let mut tick = 0u64;
        loop {
            let t = tick as f64;
            let temp = 22.0 + (t * 0.2).sin();

            // Lower-level NT write (scalar) with custom alarm logic.
            let temp_nt = make_temp_nt_with_custom_alarm(temp);
            store.put_nt("SIM:TEMP", NtPayload::Scalar(temp_nt)).await;

            // Lower-level NT write (array).
            let samples = (0..8)
                .map(|i| (t * 0.15 + i as f64 * 0.4).sin())
                .collect::<Vec<_>>();
            let array_nt = NtScalarArray::from_value(ScalarArrayValue::F64(samples));
            store
                .put_nt("SIM:SPECTRUM", NtPayload::ScalarArray(array_nt))
                .await;

            // Lower-level NT read.
            if let Some(snapshot) = store.get_nt("SIM:TEMP").await {
                println!("SIM:TEMP => {snapshot:?}");
            }

            tick += 1;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    server.run().await
}

fn make_temp_nt_with_custom_alarm(temp: f64) -> NtScalar {
    let mut nt = NtScalar::from_value(ScalarValue::F64(temp))
        .with_units("degC".to_string())
        .with_description("Simulated temperature with custom alarm mapping".to_string())
        .with_precision(2)
        .with_limits(20.5, 23.5);

    // Custom severity mapping:
    // 0 = NO_ALARM, 1 = MINOR, 2 = MAJOR
    // status is example-only numeric tagging.
    let (severity, status, message) = if temp >= 22.9 {
        (2, 3, "custom HIHI")
    } else if temp >= 22.7 {
        (1, 1, "custom HIGH")
    } else if temp <= 21.1 {
        (2, 5, "custom LOLO")
    } else if temp <= 21.3 {
        (1, 4, "custom LOW")
    } else {
        (0, 0, "custom OK")
    };

    nt.alarm_severity = severity;
    nt.alarm_status = status;
    nt.alarm_message = message.to_string();
    nt.display_description = format!("Temp={temp:.2}°C");
    nt
}