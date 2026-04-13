use std::collections::HashMap;

use spvirit_server::{
    DbCommonState, OutputMode, PvaServer, RecordData, RecordInstance, RecordType,
};
use spvirit_types::{
    NdCodec, NdDimension, NtNdArray, NtPayload, NtScalar, NtTable, NtTableColumn, ScalarArrayValue,
    ScalarValue,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = make_mode_enum_record();
    let table = make_table_record();
    let image = make_ndarray_record();

    let server = PvaServer::builder().build();
    let store = server.store().clone();

    // since we dont have a RecordBuilder for these exotic NT types yet, we insert them directly as RecordInstances.

    store.insert("SIM:MODE".into(), mode).await;
    store.insert("SIM:TBL".into(), table).await;
    store.insert("SIM:IMG".into(), image).await;

    tokio::spawn(async move {
        let mut tick = 0u64;
        loop {
            // Enum-like NTScalar using i32 + display choices.
            let mode_idx = (tick % 3) as i32;
            let mut mode_nt = NtScalar::from_value(ScalarValue::I32(mode_idx));
            mode_nt.display_form_index = mode_idx;
            mode_nt.display_form_choices = vec![
                "Standby".to_string(),
                "Acquire".to_string(),
                "Calibrate".to_string(),
            ];
            mode_nt.display_description =
                format!("Mode={}", mode_nt.display_form_choices[mode_idx as usize]);
            store.put_nt("SIM:MODE", NtPayload::Scalar(mode_nt)).await;

            // NTTable with two columns that change over time.
            let x = (0..8).map(|i| i as f64).collect::<Vec<_>>();
            let y = x
                .iter()
                .map(|v| (v * 0.7 + tick as f64 * 0.15).sin())
                .collect::<Vec<_>>();
            let table_nt = NtTable {
                labels: vec!["X".to_string(), "Y".to_string()],
                columns: vec![
                    NtTableColumn {
                        name: "x".to_string(),
                        values: ScalarArrayValue::F64(x),
                    },
                    NtTableColumn {
                        name: "y".to_string(),
                        values: ScalarArrayValue::F64(y),
                    },
                ],
                descriptor: Some("SIM table demo".to_string()),
                alarm: None,
                time_stamp: None,
            };
            store.put_nt("SIM:TBL", NtPayload::Table(table_nt)).await;

            // NTNDArray with a tiny 4x4 image.
            let pixels = (0..16)
                .map(|i| (((i + tick as i32) % 16) * 16) as u8)
                .collect::<Vec<_>>();
            let ndarray_nt = NtNdArray {
                value: ScalarArrayValue::U8(pixels),
                codec: NdCodec {
                    name: "none".to_string(),
                    parameters: HashMap::new(),
                },
                compressed_size: 16,
                uncompressed_size: 16,
                dimension: vec![
                    NdDimension {
                        size: 4,
                        offset: 0,
                        full_size: 4,
                        binning: 1,
                        reverse: false,
                    },
                    NdDimension {
                        size: 4,
                        offset: 0,
                        full_size: 4,
                        binning: 1,
                        reverse: false,
                    },
                ],
                unique_id: tick as i32,
                data_time_stamp: Default::default(),
                attribute: vec![],
                descriptor: Some("SIM 4x4 image".to_string()),
                alarm: None,
                time_stamp: None,
                display: None,
            };
            store
                .put_nt("SIM:IMG", NtPayload::NdArray(ndarray_nt))
                .await;

            if let Some(snapshot) = store.get_nt("SIM:MODE").await {
                println!("SIM:MODE => {snapshot:?}");
            }

            tick += 1;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    server.run().await
}

fn make_mode_enum_record() -> RecordInstance {
    let mut nt = NtScalar::from_value(ScalarValue::I32(0));
    nt.display_form_index = 0;
    nt.display_form_choices = vec![
        "Standby".to_string(),
        "Acquire".to_string(),
        "Calibrate".to_string(),
    ];
    nt.display_description = "Enum-like mode selector".to_string();

    RecordInstance {
        name: "SIM:MODE".to_string(),
        record_type: RecordType::Ao,
        common: DbCommonState::default(),
        data: RecordData::Ao {
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
        raw_fields: HashMap::new(),
    }
}

fn make_table_record() -> RecordInstance {
    RecordInstance {
        name: "SIM:TBL".to_string(),
        record_type: RecordType::NtTable,
        common: DbCommonState::default(),
        data: RecordData::NtTable {
            nt: NtTable {
                labels: vec!["X".to_string(), "Y".to_string()],
                columns: vec![
                    NtTableColumn {
                        name: "x".to_string(),
                        values: ScalarArrayValue::F64(vec![0.0; 8]),
                    },
                    NtTableColumn {
                        name: "y".to_string(),
                        values: ScalarArrayValue::F64(vec![0.0; 8]),
                    },
                ],
                descriptor: Some("SIM table demo".to_string()),
                alarm: None,
                time_stamp: None,
            },
            inp: None,
            out: None,
            omsl: OutputMode::Supervisory,
        },
        raw_fields: HashMap::new(),
    }
}

fn make_ndarray_record() -> RecordInstance {
    RecordInstance {
        name: "SIM:IMG".to_string(),
        record_type: RecordType::NtNdArray,
        common: DbCommonState::default(),
        data: RecordData::NtNdArray {
            nt: NtNdArray {
                value: ScalarArrayValue::U8(vec![0; 16]),
                codec: NdCodec {
                    name: "none".to_string(),
                    parameters: HashMap::new(),
                },
                compressed_size: 16,
                uncompressed_size: 16,
                dimension: vec![
                    NdDimension {
                        size: 4,
                        offset: 0,
                        full_size: 4,
                        binning: 1,
                        reverse: false,
                    },
                    NdDimension {
                        size: 4,
                        offset: 0,
                        full_size: 4,
                        binning: 1,
                        reverse: false,
                    },
                ],
                unique_id: 0,
                data_time_stamp: Default::default(),
                attribute: vec![],
                descriptor: Some("SIM 4x4 image".to_string()),
                alarm: None,
                time_stamp: None,
                display: None,
            },
            inp: None,
            out: None,
            omsl: OutputMode::Supervisory,
        },
        raw_fields: HashMap::new(),
    }
}
