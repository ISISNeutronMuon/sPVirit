use spvirit_server::{PvStore, PvaServer};
use spvirit_types::{NtPayload, PvValue, ScalarArrayValue, ScalarValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = PvaServer::builder()
        .ai("SIM:TEMPERATURE", 22.5)
        .waveform("SIM:SPECTRUM", ScalarArrayValue::F64(vec![0.0; 16]))
        .mbbi(
            "SIM:STATE",
            vec![
                "Idle".to_string(),
                "Running".to_string(),
                "Error".to_string(),
            ],
            0,
        )
        .generic(
            "SIM:META",
            "demo:custom/Meta:1.0",
            vec![
                (
                    "author".to_string(),
                    PvValue::Scalar(ScalarValue::Str("spvirit".to_string())),
                ),
                (
                    "version".to_string(),
                    PvValue::Scalar(ScalarValue::I32(1)),
                ),
            ],
        )
        .build();

    let store = server.store().clone();

    tokio::spawn(async move {
        let mut tick = 0u64;
        loop {
            let phase = tick as f64 * 0.2;
            let spectrum = (0..16)
                .map(|i| (phase + i as f64 * 0.25).sin())
                .collect::<Vec<_>>();

            store
                .set_value("SIM:TEMPERATURE", ScalarValue::F64(22.0 + phase.sin()))
                .await;
            store
                .set_array_value("SIM:SPECTRUM", ScalarArrayValue::F64(spectrum))
                .await;

            // Cycle the enum state
            let state_idx = (tick % 3) as i32;
            store
                .put_nt(
                    "SIM:STATE",
                    NtPayload::Enum(spvirit_types::NtEnum::new(
                        state_idx,
                        vec![
                            "Idle".to_string(),
                            "Running".to_string(),
                            "Error".to_string(),
                        ],
                    )),
                )
                .await;

            for pv in &["SIM:TEMPERATURE", "SIM:SPECTRUM", "SIM:STATE", "SIM:META"] {
                if let Some(snapshot) = store.get_snapshot(pv).await {
                    print_snapshot(pv, &snapshot);
                }
            }

            tick += 1;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    server.run().await
}

fn print_snapshot(pv: &str, payload: &NtPayload) {
    match payload {
        NtPayload::Scalar(nt) => {
            println!(
                "{pv} snapshot: scalar value={:?} units={}",
                nt.value, nt.units
            );
        }
        NtPayload::ScalarArray(nt) => {
            println!(
                "{pv} snapshot: array len={} type={}",
                nt.value.len(),
                nt.value.type_label()
            );
        }
        NtPayload::Table(nt) => {
            println!("{pv} snapshot: table columns={}", nt.columns.len());
        }
        NtPayload::NdArray(nt) => {
            println!("{pv} snapshot: ndarray dims={}", nt.dimension.len());
        }
        NtPayload::Enum(nt) => {
            println!(
                "{pv} snapshot: enum index={} selected={:?}",
                nt.index,
                nt.selected()
            );
        }
        NtPayload::Generic { struct_id, fields } => {
            println!(
                "{pv} snapshot: generic struct_id={struct_id} fields={}",
                fields.len()
            );
        }
    }
}
