use std::collections::HashMap;
use spvirit_server::{DbCommonState, PvaServer, RecordData, RecordInstance, RecordType};
use spvirit_types::{NtScalar, ScalarValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let custom = RecordInstance {
        name: "CUSTOM:SENSOR".into(),
        record_type: RecordType::Ai,
        common: DbCommonState {
            desc: "My custom sensor".into(),
            ..DbCommonState::default()
        },
        data: RecordData::Ai {
            nt: NtScalar::from_value(ScalarValue::F64(21.0))
                .with_units("degC".into())
                .with_precision(2)
                .with_limits(-20.0, 100.0),
            inp: None,
            siml: None,
            siol: None,
            simm:true,
        },
        raw_fields: HashMap::new(),
    };

    let server = PvaServer::builder()
        .ao("CUSTOM:SETPOINT", 25.0)
        .build();

    server.store().insert("CUSTOM:SENSOR".into(), custom).await;

    server.run().await
}
