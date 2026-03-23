use spvirit_tools::spvirit_server::db::parse_db;
use spvirit_tools::spvirit_server::types::{RecordData, RecordType, ScalarArrayValue};

#[test]
fn parse_array_family_record_types() {
    let input = r#"
        record(waveform, "SIM:WAVE") {
            field(FTVL, "DOUBLE")
            field(NELM, "4")
            field(NORD, "4")
            field(VAL, "1,2,3,4")
            field(INP, "SIM:SRC")
        }
        record(aai, "SIM:AAI") {
            field(FTVL, "LONG")
            field(NELM, "3")
            field(NORD, "3")
            field(VAL, "10 20 30")
            field(INP, "SIM:SRC")
        }
        record(aao, "SIM:AAO") {
            field(FTVL, "FLOAT")
            field(NELM, "2")
            field(NORD, "2")
            field(VAL, "1.5,2.5")
            field(OUT, "SIM:OUT")
            field(OMSL, "supervisory")
        }
        record(subArray, "SIM:SUB") {
            field(FTVL, "UCHAR")
            field(MALM, "8")
            field(NELM, "3")
            field(NORD, "3")
            field(INDX, "1")
            field(VAL, "1,2,3")
            field(INP, "SIM:WAVE")
        }
    "#;

    let map = parse_db(input).expect("parse");

    let wave = map.get("SIM:WAVE").expect("waveform record");
    assert_eq!(wave.record_type, RecordType::Waveform);
    assert!(wave.writable());
    match &wave.data {
        RecordData::Waveform { nt, nelm, nord, .. } => {
            assert_eq!(*nelm, 4);
            assert_eq!(*nord, 4);
            match &nt.value {
                ScalarArrayValue::F64(v) => assert_eq!(v, &vec![1.0, 2.0, 3.0, 4.0]),
                other => panic!("unexpected waveform value type: {:?}", other),
            }
        }
        other => panic!("unexpected waveform data: {:?}", other),
    }

    let aai = map.get("SIM:AAI").expect("aai record");
    assert_eq!(aai.record_type, RecordType::Aai);
    assert!(!aai.writable());

    let aao = map.get("SIM:AAO").expect("aao record");
    assert_eq!(aao.record_type, RecordType::Aao);
    assert!(aao.writable());

    let sub = map.get("SIM:SUB").expect("subArray record");
    assert_eq!(sub.record_type, RecordType::SubArray);
    assert!(!sub.writable());
}
