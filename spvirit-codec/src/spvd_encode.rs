//! PVD (pvData) Encoding Helpers
//!
//! Minimal encoder for NTScalar introspection and value updates.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::spvd_decode::{FieldDesc, FieldType, StructureDesc, TypeCode};

use spvirit_types::{
    NdDimension, NtAlarm, NtAttribute, NtDisplay, NtEnum, NtNdArray, NtPayload, NtScalar,
    NtScalarArray, NtTable, NtTableColumn, NtTimeStamp, PvValue, ScalarArrayValue, ScalarValue,
};

fn count_structure_fields(desc: &StructureDesc) -> usize {
    let mut count = 0;
    for field in &desc.fields {
        count += 1;
        if let FieldType::Structure(nested) = &field.field_type {
            count += count_structure_fields(nested);
        }
    }
    count
}

pub fn encode_size_pvd(size: usize, is_be: bool) -> Vec<u8> {
    crate::encode_common::encode_size(size, is_be)
}

pub fn encode_string_pvd(value: &str, is_be: bool) -> Vec<u8> {
    crate::encode_common::encode_string(value, is_be)
}

pub fn encode_structure_desc(desc: &StructureDesc, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    let struct_id = desc.struct_id.clone().unwrap_or_default();
    out.extend_from_slice(&encode_string_pvd(&struct_id, is_be));
    out.extend_from_slice(&encode_size_pvd(desc.fields.len(), is_be));
    for field in &desc.fields {
        out.extend_from_slice(&encode_field_desc(field, is_be));
    }
    out
}

fn encode_field_desc(field: &FieldDesc, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_string_pvd(&field.name, is_be));
    out.extend_from_slice(&encode_type_desc(&field.field_type, is_be));
    out
}

fn encode_type_desc(field_type: &FieldType, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    match field_type {
        FieldType::Structure(desc) => {
            out.push(0x80);
            out.extend_from_slice(&encode_structure_desc(desc, is_be));
        }
        FieldType::StructureArray(desc) => {
            out.push(0x88);
            out.push(0x80); // inner structure element tag
            out.extend_from_slice(&encode_structure_desc(desc, is_be));
        }
        FieldType::Union(fields) => {
            out.push(0x81);
            let desc = StructureDesc {
                struct_id: None,
                fields: fields.clone(),
            };
            out.extend_from_slice(&encode_structure_desc(&desc, is_be));
        }
        FieldType::UnionArray(fields) => {
            out.push(0x89);
            out.push(0x81); // inner union element tag
            let desc = StructureDesc {
                struct_id: None,
                fields: fields.clone(),
            };
            out.extend_from_slice(&encode_structure_desc(&desc, is_be));
        }
        FieldType::Variant => out.push(0x82),
        FieldType::VariantArray => out.push(0x8A),
        FieldType::BoundedString(bound) => {
            out.push(0x83);
            out.extend_from_slice(&encode_size_pvd(*bound as usize, is_be));
        }
        FieldType::String => out.push(0x60),
        FieldType::StringArray => out.push(0x68),
        FieldType::Scalar(tc) => out.push(*tc as u8),
        FieldType::ScalarArray(tc) => out.push((*tc as u8) | 0x08),
    }
    out
}

fn encode_scalar_value(value: &ScalarValue, is_be: bool) -> Vec<u8> {
    match value {
        ScalarValue::Bool(v) => vec![if *v { 1 } else { 0 }],
        ScalarValue::I8(v) => vec![*v as u8],
        ScalarValue::I16(v) => {
            if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            }
        }
        ScalarValue::I32(v) => {
            if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            }
        }
        ScalarValue::I64(v) => {
            if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            }
        }
        ScalarValue::U8(v) => vec![*v],
        ScalarValue::U16(v) => {
            if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            }
        }
        ScalarValue::U32(v) => {
            if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            }
        }
        ScalarValue::U64(v) => {
            if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            }
        }
        ScalarValue::F32(v) => {
            if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            }
        }
        ScalarValue::F64(v) => {
            if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            }
        }
        ScalarValue::Str(v) => encode_string_pvd(v, is_be),
    }
}

fn encode_alarm(nt: &NtScalar, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_i32(nt.alarm_severity, is_be));
    out.extend_from_slice(&encode_i32(nt.alarm_status, is_be));
    out.extend_from_slice(&encode_string_pvd(&nt.alarm_message, is_be));
    out
}

fn encode_bool(value: bool) -> Vec<u8> {
    vec![if value { 1 } else { 0 }]
}

fn encode_string_array(values: &[String], is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_size_pvd(values.len(), is_be));
    for v in values {
        out.extend_from_slice(&encode_string_pvd(v, is_be));
    }
    out
}

fn encode_enum(index: i32, choices: &[String], is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_i32(index, is_be));
    out.extend_from_slice(&encode_string_array(choices, is_be));
    out
}

fn encode_timestamp(_nt: &NtScalar, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let seconds_past_epoch = now.as_secs() as i64;
    let nanos = now.subsec_nanos() as i32;

    out.extend_from_slice(&encode_i64(seconds_past_epoch, is_be));
    out.extend_from_slice(&encode_i32(nanos, is_be));
    out.extend_from_slice(&encode_i32(0, is_be)); // userTag
    out
}

fn encode_display(nt: &NtScalar, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_f64(nt.display_low, is_be));
    out.extend_from_slice(&encode_f64(nt.display_high, is_be));
    out.extend_from_slice(&encode_string_pvd(&nt.display_description, is_be));
    out.extend_from_slice(&encode_string_pvd(&nt.units, is_be));
    out.extend_from_slice(&encode_i32(nt.display_precision, is_be));
    out.extend_from_slice(&encode_enum(
        nt.display_form_index,
        &nt.display_form_choices,
        is_be,
    ));
    out
}

fn encode_control(nt: &NtScalar, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_f64(nt.control_low, is_be));
    out.extend_from_slice(&encode_f64(nt.control_high, is_be));
    out.extend_from_slice(&encode_f64(nt.control_min_step, is_be));
    out
}

fn encode_value_alarm(nt: &NtScalar, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_bool(nt.value_alarm_active));
    out.extend_from_slice(&encode_f64(nt.value_alarm_low_alarm_limit, is_be));
    out.extend_from_slice(&encode_f64(nt.value_alarm_low_warning_limit, is_be));
    out.extend_from_slice(&encode_f64(nt.value_alarm_high_warning_limit, is_be));
    out.extend_from_slice(&encode_f64(nt.value_alarm_high_alarm_limit, is_be));
    out.extend_from_slice(&encode_i32(nt.value_alarm_low_alarm_severity, is_be));
    out.extend_from_slice(&encode_i32(nt.value_alarm_low_warning_severity, is_be));
    out.extend_from_slice(&encode_i32(nt.value_alarm_high_warning_severity, is_be));
    out.extend_from_slice(&encode_i32(nt.value_alarm_high_alarm_severity, is_be));
    out.push(nt.value_alarm_hysteresis);
    out
}

fn encode_i32(value: i32, is_be: bool) -> Vec<u8> {
    if is_be {
        value.to_be_bytes().to_vec()
    } else {
        value.to_le_bytes().to_vec()
    }
}

fn encode_i64(value: i64, is_be: bool) -> Vec<u8> {
    if is_be {
        value.to_be_bytes().to_vec()
    } else {
        value.to_le_bytes().to_vec()
    }
}

fn encode_f64(value: f64, is_be: bool) -> Vec<u8> {
    if is_be {
        value.to_be_bytes().to_vec()
    } else {
        value.to_le_bytes().to_vec()
    }
}

pub fn nt_scalar_desc(value: &ScalarValue) -> StructureDesc {
    let value_type = match value {
        ScalarValue::Bool(_) => FieldType::Scalar(TypeCode::Boolean),
        ScalarValue::I8(_) => FieldType::Scalar(TypeCode::Int8),
        ScalarValue::I16(_) => FieldType::Scalar(TypeCode::Int16),
        ScalarValue::I32(_) => FieldType::Scalar(TypeCode::Int32),
        ScalarValue::I64(_) => FieldType::Scalar(TypeCode::Int64),
        ScalarValue::U8(_) => FieldType::Scalar(TypeCode::UInt8),
        ScalarValue::U16(_) => FieldType::Scalar(TypeCode::UInt16),
        ScalarValue::U32(_) => FieldType::Scalar(TypeCode::UInt32),
        ScalarValue::U64(_) => FieldType::Scalar(TypeCode::UInt64),
        ScalarValue::F32(_) => FieldType::Scalar(TypeCode::Float32),
        ScalarValue::F64(_) => FieldType::Scalar(TypeCode::Float64),
        ScalarValue::Str(_) => FieldType::String,
    };

    StructureDesc {
        struct_id: Some("epics:nt/NTScalar:1.0".to_string()),
        fields: vec![
            FieldDesc {
                name: "value".to_string(),
                field_type: value_type,
            },
            FieldDesc {
                name: "alarm".to_string(),
                field_type: FieldType::Structure(StructureDesc {
                    struct_id: Some("alarm_t".to_string()),
                    fields: vec![
                        FieldDesc {
                            name: "severity".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                        FieldDesc {
                            name: "status".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                        FieldDesc {
                            name: "message".to_string(),
                            field_type: FieldType::String,
                        },
                    ],
                }),
            },
            FieldDesc {
                name: "timeStamp".to_string(),
                field_type: FieldType::Structure(StructureDesc {
                    struct_id: None,
                    fields: vec![
                        FieldDesc {
                            name: "secondsPastEpoch".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int64),
                        },
                        FieldDesc {
                            name: "nanoseconds".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                        FieldDesc {
                            name: "userTag".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                    ],
                }),
            },
            FieldDesc {
                name: "display".to_string(),
                field_type: FieldType::Structure(StructureDesc {
                    struct_id: None,
                    fields: vec![
                        FieldDesc {
                            name: "limitLow".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Float64),
                        },
                        FieldDesc {
                            name: "limitHigh".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Float64),
                        },
                        FieldDesc {
                            name: "description".to_string(),
                            field_type: FieldType::String,
                        },
                        FieldDesc {
                            name: "units".to_string(),
                            field_type: FieldType::String,
                        },
                        FieldDesc {
                            name: "precision".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                        FieldDesc {
                            name: "form".to_string(),
                            field_type: FieldType::Structure(StructureDesc {
                                struct_id: Some("enum_t".to_string()),
                                fields: vec![
                                    FieldDesc {
                                        name: "index".to_string(),
                                        field_type: FieldType::Scalar(TypeCode::Int32),
                                    },
                                    FieldDesc {
                                        name: "choices".to_string(),
                                        field_type: FieldType::StringArray,
                                    },
                                ],
                            }),
                        },
                    ],
                }),
            },
            FieldDesc {
                name: "control".to_string(),
                field_type: FieldType::Structure(StructureDesc {
                    struct_id: Some("control_t".to_string()),
                    fields: vec![
                        FieldDesc {
                            name: "limitLow".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Float64),
                        },
                        FieldDesc {
                            name: "limitHigh".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Float64),
                        },
                        FieldDesc {
                            name: "minStep".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Float64),
                        },
                    ],
                }),
            },
            FieldDesc {
                name: "valueAlarm".to_string(),
                field_type: FieldType::Structure(StructureDesc {
                    struct_id: Some("valueAlarm_t".to_string()),
                    fields: vec![
                        FieldDesc {
                            name: "active".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Boolean),
                        },
                        FieldDesc {
                            name: "lowAlarmLimit".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Float64),
                        },
                        FieldDesc {
                            name: "lowWarningLimit".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Float64),
                        },
                        FieldDesc {
                            name: "highWarningLimit".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Float64),
                        },
                        FieldDesc {
                            name: "highAlarmLimit".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Float64),
                        },
                        FieldDesc {
                            name: "lowAlarmSeverity".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                        FieldDesc {
                            name: "lowWarningSeverity".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                        FieldDesc {
                            name: "highWarningSeverity".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                        FieldDesc {
                            name: "highAlarmSeverity".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                        FieldDesc {
                            name: "hysteresis".to_string(),
                            field_type: FieldType::Scalar(TypeCode::UInt8),
                        },
                    ],
                }),
            },
        ],
    }
}

pub fn encode_nt_scalar_full(nt: &NtScalar, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_scalar_value(&nt.value, is_be));
    out.extend_from_slice(&encode_alarm(nt, is_be));
    out.extend_from_slice(&encode_timestamp(nt, is_be));
    out.extend_from_slice(&encode_display(nt, is_be));
    out.extend_from_slice(&encode_control(nt, is_be));
    out.extend_from_slice(&encode_value_alarm(nt, is_be));
    out
}

fn encode_structure_bitset(desc: &StructureDesc, is_be: bool) -> Vec<u8> {
    let total_bits = 1 + count_structure_fields(desc);
    let bitset_size = (total_bits + 7) / 8;
    let mut bitset = vec![0u8; bitset_size];
    for bit in 0..total_bits {
        let byte_idx = bit / 8;
        let bit_idx = bit % 8;
        bitset[byte_idx] |= 1 << bit_idx;
    }
    let mut out = Vec::new();
    out.extend_from_slice(&encode_size_pvd(bitset_size, is_be));
    out.extend_from_slice(&bitset);
    out
}

fn encode_structure_with_bitset(desc: &StructureDesc, nt: &NtScalar, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_structure_bitset(desc, is_be));
    out.extend_from_slice(&encode_nt_scalar_full(nt, is_be));
    out
}

pub fn encode_nt_scalar_bitset(nt: &NtScalar, is_be: bool) -> Vec<u8> {
    let desc = nt_scalar_desc(&nt.value);
    encode_structure_with_bitset(&desc, nt, is_be)
}

pub fn encode_nt_scalar_bitset_parts(nt: &NtScalar, is_be: bool) -> (Vec<u8>, Vec<u8>) {
    let desc = nt_scalar_desc(&nt.value);
    let bitset = encode_structure_bitset(&desc, is_be);
    let values = encode_nt_scalar_full(nt, is_be);
    (bitset, values)
}

fn alarm_desc() -> StructureDesc {
    StructureDesc {
        struct_id: Some("alarm_t".to_string()),
        fields: vec![
            FieldDesc {
                name: "severity".to_string(),
                field_type: FieldType::Scalar(TypeCode::Int32),
            },
            FieldDesc {
                name: "status".to_string(),
                field_type: FieldType::Scalar(TypeCode::Int32),
            },
            FieldDesc {
                name: "message".to_string(),
                field_type: FieldType::String,
            },
        ],
    }
}

fn timestamp_desc() -> StructureDesc {
    StructureDesc {
        struct_id: Some("time_t".to_string()),
        fields: vec![
            FieldDesc {
                name: "secondsPastEpoch".to_string(),
                field_type: FieldType::Scalar(TypeCode::Int64),
            },
            FieldDesc {
                name: "nanoseconds".to_string(),
                field_type: FieldType::Scalar(TypeCode::Int32),
            },
            FieldDesc {
                name: "userTag".to_string(),
                field_type: FieldType::Scalar(TypeCode::Int32),
            },
        ],
    }
}

fn display_desc() -> StructureDesc {
    StructureDesc {
        struct_id: Some("display_t".to_string()),
        fields: vec![
            FieldDesc {
                name: "limitLow".to_string(),
                field_type: FieldType::Scalar(TypeCode::Float64),
            },
            FieldDesc {
                name: "limitHigh".to_string(),
                field_type: FieldType::Scalar(TypeCode::Float64),
            },
            FieldDesc {
                name: "description".to_string(),
                field_type: FieldType::String,
            },
            FieldDesc {
                name: "units".to_string(),
                field_type: FieldType::String,
            },
            FieldDesc {
                name: "precision".to_string(),
                field_type: FieldType::Scalar(TypeCode::Int32),
            },
        ],
    }
}

fn scalar_array_field_type(value: &ScalarArrayValue) -> FieldType {
    match value {
        ScalarArrayValue::Bool(_) => FieldType::ScalarArray(TypeCode::Boolean),
        ScalarArrayValue::I8(_) => FieldType::ScalarArray(TypeCode::Int8),
        ScalarArrayValue::I16(_) => FieldType::ScalarArray(TypeCode::Int16),
        ScalarArrayValue::I32(_) => FieldType::ScalarArray(TypeCode::Int32),
        ScalarArrayValue::I64(_) => FieldType::ScalarArray(TypeCode::Int64),
        ScalarArrayValue::U8(_) => FieldType::ScalarArray(TypeCode::UInt8),
        ScalarArrayValue::U16(_) => FieldType::ScalarArray(TypeCode::UInt16),
        ScalarArrayValue::U32(_) => FieldType::ScalarArray(TypeCode::UInt32),
        ScalarArrayValue::U64(_) => FieldType::ScalarArray(TypeCode::UInt64),
        ScalarArrayValue::F32(_) => FieldType::ScalarArray(TypeCode::Float32),
        ScalarArrayValue::F64(_) => FieldType::ScalarArray(TypeCode::Float64),
        ScalarArrayValue::Str(_) => FieldType::StringArray,
    }
}

fn encode_scalar_array_value_pvd(value: &ScalarArrayValue, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    match value {
        ScalarArrayValue::Bool(v) => {
            out.extend_from_slice(&encode_size_pvd(v.len(), is_be));
            for i in v {
                out.push(if *i { 1 } else { 0 });
            }
        }
        ScalarArrayValue::I8(v) => {
            out.extend_from_slice(&encode_size_pvd(v.len(), is_be));
            for i in v {
                out.push(*i as u8);
            }
        }
        ScalarArrayValue::I16(v) => {
            out.extend_from_slice(&encode_size_pvd(v.len(), is_be));
            for i in v {
                let b = if is_be {
                    i.to_be_bytes()
                } else {
                    i.to_le_bytes()
                };
                out.extend_from_slice(&b);
            }
        }
        ScalarArrayValue::I32(v) => {
            out.extend_from_slice(&encode_size_pvd(v.len(), is_be));
            for i in v {
                out.extend_from_slice(&encode_i32(*i, is_be));
            }
        }
        ScalarArrayValue::I64(v) => {
            out.extend_from_slice(&encode_size_pvd(v.len(), is_be));
            for i in v {
                out.extend_from_slice(&encode_i64(*i, is_be));
            }
        }
        ScalarArrayValue::U8(v) => {
            out.extend_from_slice(&encode_size_pvd(v.len(), is_be));
            out.extend_from_slice(v);
        }
        ScalarArrayValue::U16(v) => {
            out.extend_from_slice(&encode_size_pvd(v.len(), is_be));
            for i in v {
                let b = if is_be {
                    i.to_be_bytes()
                } else {
                    i.to_le_bytes()
                };
                out.extend_from_slice(&b);
            }
        }
        ScalarArrayValue::U32(v) => {
            out.extend_from_slice(&encode_size_pvd(v.len(), is_be));
            for i in v {
                let b = if is_be {
                    i.to_be_bytes()
                } else {
                    i.to_le_bytes()
                };
                out.extend_from_slice(&b);
            }
        }
        ScalarArrayValue::U64(v) => {
            out.extend_from_slice(&encode_size_pvd(v.len(), is_be));
            for i in v {
                let b = if is_be {
                    i.to_be_bytes()
                } else {
                    i.to_le_bytes()
                };
                out.extend_from_slice(&b);
            }
        }
        ScalarArrayValue::F32(v) => {
            out.extend_from_slice(&encode_size_pvd(v.len(), is_be));
            for i in v {
                let b = if is_be {
                    i.to_be_bytes()
                } else {
                    i.to_le_bytes()
                };
                out.extend_from_slice(&b);
            }
        }
        ScalarArrayValue::F64(v) => {
            out.extend_from_slice(&encode_size_pvd(v.len(), is_be));
            for i in v {
                out.extend_from_slice(&encode_f64(*i, is_be));
            }
        }
        ScalarArrayValue::Str(v) => {
            out.extend_from_slice(&encode_string_array(v, is_be));
        }
    }
    out
}

fn encode_nt_alarm(alarm: &NtAlarm, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_i32(alarm.severity, is_be));
    out.extend_from_slice(&encode_i32(alarm.status, is_be));
    out.extend_from_slice(&encode_string_pvd(&alarm.message, is_be));
    out
}

fn encode_nt_timestamp(ts: &NtTimeStamp, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_i64(ts.seconds_past_epoch, is_be));
    out.extend_from_slice(&encode_i32(ts.nanoseconds, is_be));
    out.extend_from_slice(&encode_i32(ts.user_tag, is_be));
    out
}

fn encode_nt_display(display: &NtDisplay, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_f64(display.limit_low, is_be));
    out.extend_from_slice(&encode_f64(display.limit_high, is_be));
    out.extend_from_slice(&encode_string_pvd(&display.description, is_be));
    out.extend_from_slice(&encode_string_pvd(&display.units, is_be));
    out.extend_from_slice(&encode_i32(display.precision, is_be));
    out
}

pub fn nt_scalar_array_desc(value: &ScalarArrayValue) -> StructureDesc {
    StructureDesc {
        struct_id: Some("epics:nt/NTScalarArray:1.0".to_string()),
        fields: vec![
            FieldDesc {
                name: "value".to_string(),
                field_type: scalar_array_field_type(value),
            },
            FieldDesc {
                name: "alarm".to_string(),
                field_type: FieldType::Structure(alarm_desc()),
            },
            FieldDesc {
                name: "timeStamp".to_string(),
                field_type: FieldType::Structure(timestamp_desc()),
            },
            FieldDesc {
                name: "display".to_string(),
                field_type: FieldType::Structure(display_desc()),
            },
            FieldDesc {
                name: "control".to_string(),
                field_type: FieldType::Structure(StructureDesc {
                    struct_id: Some("control_t".to_string()),
                    fields: vec![
                        FieldDesc {
                            name: "limitLow".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Float64),
                        },
                        FieldDesc {
                            name: "limitHigh".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Float64),
                        },
                        FieldDesc {
                            name: "minStep".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Float64),
                        },
                    ],
                }),
            },
        ],
    }
}

pub fn encode_nt_scalar_array_full(nt: &NtScalarArray, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_scalar_array_value_pvd(&nt.value, is_be));
    out.extend_from_slice(&encode_nt_alarm(&nt.alarm, is_be));
    out.extend_from_slice(&encode_nt_timestamp(&nt.time_stamp, is_be));
    out.extend_from_slice(&encode_nt_display(&nt.display, is_be));
    out.extend_from_slice(&encode_f64(nt.control.limit_low, is_be));
    out.extend_from_slice(&encode_f64(nt.control.limit_high, is_be));
    out.extend_from_slice(&encode_f64(nt.control.min_step, is_be));
    out
}

pub fn nt_table_desc(nt: &NtTable) -> StructureDesc {
    let mut value_fields: Vec<FieldDesc> = Vec::new();
    for col in &nt.columns {
        value_fields.push(FieldDesc {
            name: col.name.clone(),
            field_type: scalar_array_field_type(&col.values),
        });
    }
    StructureDesc {
        struct_id: Some("epics:nt/NTTable:1.0".to_string()),
        fields: vec![
            FieldDesc {
                name: "labels".to_string(),
                field_type: FieldType::StringArray,
            },
            FieldDesc {
                name: "value".to_string(),
                field_type: FieldType::Structure(StructureDesc {
                    struct_id: None,
                    fields: value_fields,
                }),
            },
        ],
    }
}

pub fn encode_nt_table_full(nt: &NtTable, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_string_array(&nt.labels, is_be));
    for NtTableColumn { values, .. } in &nt.columns {
        out.extend_from_slice(&encode_scalar_array_value_pvd(values, is_be));
    }
    out
}

fn nt_ndarray_value_union_fields() -> Vec<FieldDesc> {
    vec![
        FieldDesc {
            name: "booleanValue".to_string(),
            field_type: FieldType::ScalarArray(TypeCode::Boolean),
        },
        FieldDesc {
            name: "byteValue".to_string(),
            field_type: FieldType::ScalarArray(TypeCode::Int8),
        },
        FieldDesc {
            name: "shortValue".to_string(),
            field_type: FieldType::ScalarArray(TypeCode::Int16),
        },
        FieldDesc {
            name: "intValue".to_string(),
            field_type: FieldType::ScalarArray(TypeCode::Int32),
        },
        FieldDesc {
            name: "longValue".to_string(),
            field_type: FieldType::ScalarArray(TypeCode::Int64),
        },
        FieldDesc {
            name: "ubyteValue".to_string(),
            field_type: FieldType::ScalarArray(TypeCode::UInt8),
        },
        FieldDesc {
            name: "ushortValue".to_string(),
            field_type: FieldType::ScalarArray(TypeCode::UInt16),
        },
        FieldDesc {
            name: "uintValue".to_string(),
            field_type: FieldType::ScalarArray(TypeCode::UInt32),
        },
        FieldDesc {
            name: "ulongValue".to_string(),
            field_type: FieldType::ScalarArray(TypeCode::UInt64),
        },
        FieldDesc {
            name: "floatValue".to_string(),
            field_type: FieldType::ScalarArray(TypeCode::Float32),
        },
        FieldDesc {
            name: "doubleValue".to_string(),
            field_type: FieldType::ScalarArray(TypeCode::Float64),
        },
        FieldDesc {
            name: "stringValue".to_string(),
            field_type: FieldType::StringArray,
        },
    ]
}

fn ndarray_union_index(value: &ScalarArrayValue) -> usize {
    match value {
        ScalarArrayValue::Bool(_) => 0,
        ScalarArrayValue::I8(_) => 1,
        ScalarArrayValue::I16(_) => 2,
        ScalarArrayValue::I32(_) => 3,
        ScalarArrayValue::I64(_) => 4,
        ScalarArrayValue::U8(_) => 5,
        ScalarArrayValue::U16(_) => 6,
        ScalarArrayValue::U32(_) => 7,
        ScalarArrayValue::U64(_) => 8,
        ScalarArrayValue::F32(_) => 9,
        ScalarArrayValue::F64(_) => 10,
        ScalarArrayValue::Str(_) => 11,
    }
}

fn encode_ndarray_union(value: &ScalarArrayValue, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_size_pvd(ndarray_union_index(value), is_be));
    out.extend_from_slice(&encode_scalar_array_value_pvd(value, is_be));
    out
}

fn encode_codec_parameters(
    parameters: &std::collections::HashMap<String, String>,
    is_be: bool,
) -> Vec<u8> {
    if parameters.is_empty() {
        return vec![0xFF];
    }
    let mut out = Vec::new();
    out.push(0x80);
    let mut fields = Vec::new();
    for key in parameters.keys() {
        fields.push(FieldDesc {
            name: key.clone(),
            field_type: FieldType::String,
        });
    }
    let desc = StructureDesc {
        struct_id: None,
        fields,
    };
    out.extend_from_slice(&encode_structure_desc(&desc, is_be));
    for value in parameters.values() {
        out.extend_from_slice(&encode_string_pvd(value, is_be));
    }
    out
}

pub fn nt_ndarray_desc(_nt: &NtNdArray) -> StructureDesc {
    StructureDesc {
        struct_id: Some("epics:nt/NTNDArray:1.0".to_string()),
        fields: vec![
            FieldDesc {
                name: "value".to_string(),
                field_type: FieldType::Union(nt_ndarray_value_union_fields()),
            },
            FieldDesc {
                name: "codec".to_string(),
                field_type: FieldType::Structure(StructureDesc {
                    struct_id: Some("codec_t".to_string()),
                    fields: vec![
                        FieldDesc {
                            name: "name".to_string(),
                            field_type: FieldType::String,
                        },
                        FieldDesc {
                            name: "parameters".to_string(),
                            field_type: FieldType::Variant,
                        },
                    ],
                }),
            },
            FieldDesc {
                name: "compressedSize".to_string(),
                field_type: FieldType::Scalar(TypeCode::Int64),
            },
            FieldDesc {
                name: "uncompressedSize".to_string(),
                field_type: FieldType::Scalar(TypeCode::Int64),
            },
            FieldDesc {
                name: "dimension".to_string(),
                field_type: FieldType::StructureArray(StructureDesc {
                    struct_id: Some("dimension_t".to_string()),
                    fields: vec![
                        FieldDesc {
                            name: "size".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                        FieldDesc {
                            name: "offset".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                        FieldDesc {
                            name: "fullSize".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                        FieldDesc {
                            name: "binning".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                        FieldDesc {
                            name: "reverse".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Boolean),
                        },
                    ],
                }),
            },
            FieldDesc {
                name: "uniqueId".to_string(),
                field_type: FieldType::Scalar(TypeCode::Int32),
            },
            FieldDesc {
                name: "dataTimeStamp".to_string(),
                field_type: FieldType::Structure(timestamp_desc()),
            },
            FieldDesc {
                name: "attribute".to_string(),
                field_type: FieldType::StructureArray(StructureDesc {
                    struct_id: Some("NTAttribute".to_string()),
                    fields: vec![
                        FieldDesc {
                            name: "name".to_string(),
                            field_type: FieldType::String,
                        },
                        FieldDesc {
                            name: "value".to_string(),
                            field_type: FieldType::Variant,
                        },
                        FieldDesc {
                            name: "descriptor".to_string(),
                            field_type: FieldType::String,
                        },
                        FieldDesc {
                            name: "sourceType".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                        FieldDesc {
                            name: "source".to_string(),
                            field_type: FieldType::String,
                        },
                    ],
                }),
            },
            FieldDesc {
                name: "descriptor".to_string(),
                field_type: FieldType::String,
            },
            FieldDesc {
                name: "alarm".to_string(),
                field_type: FieldType::Structure(alarm_desc()),
            },
            FieldDesc {
                name: "timeStamp".to_string(),
                field_type: FieldType::Structure(timestamp_desc()),
            },
            FieldDesc {
                name: "display".to_string(),
                field_type: FieldType::Structure(display_desc()),
            },
        ],
    }
}

fn encode_attribute_variant(attr: &NtAttribute, is_be: bool) -> Vec<u8> {
    match &attr.value {
        ScalarValue::Bool(v) => {
            let mut out = vec![TypeCode::Boolean as u8];
            out.push(if *v { 1 } else { 0 });
            out
        }
        ScalarValue::I8(v) => {
            let mut out = vec![TypeCode::Int8 as u8];
            out.push(*v as u8);
            out
        }
        ScalarValue::I16(v) => {
            let mut out = vec![TypeCode::Int16 as u8];
            out.extend_from_slice(&if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            });
            out
        }
        ScalarValue::I32(v) => {
            let mut out = vec![TypeCode::Int32 as u8];
            out.extend_from_slice(&encode_i32(*v, is_be));
            out
        }
        ScalarValue::I64(v) => {
            let mut out = vec![TypeCode::Int64 as u8];
            out.extend_from_slice(&encode_i64(*v, is_be));
            out
        }
        ScalarValue::U8(v) => {
            let mut out = vec![TypeCode::UInt8 as u8];
            out.push(*v);
            out
        }
        ScalarValue::U16(v) => {
            let mut out = vec![TypeCode::UInt16 as u8];
            out.extend_from_slice(&if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            });
            out
        }
        ScalarValue::U32(v) => {
            let mut out = vec![TypeCode::UInt32 as u8];
            out.extend_from_slice(&if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            });
            out
        }
        ScalarValue::U64(v) => {
            let mut out = vec![TypeCode::UInt64 as u8];
            out.extend_from_slice(&if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            });
            out
        }
        ScalarValue::F32(v) => {
            let mut out = vec![TypeCode::Float32 as u8];
            out.extend_from_slice(&if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            });
            out
        }
        ScalarValue::F64(v) => {
            let mut out = vec![TypeCode::Float64 as u8];
            out.extend_from_slice(&encode_f64(*v, is_be));
            out
        }
        ScalarValue::Str(v) => {
            let mut out = vec![TypeCode::String as u8];
            out.extend_from_slice(&encode_string_pvd(v, is_be));
            out
        }
    }
}

pub fn encode_nt_ndarray_full(nt: &NtNdArray, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_ndarray_union(&nt.value, is_be));
    out.extend_from_slice(&encode_string_pvd(&nt.codec.name, is_be));
    out.extend_from_slice(&encode_codec_parameters(&nt.codec.parameters, is_be));
    out.extend_from_slice(&encode_i64(nt.compressed_size, is_be));
    out.extend_from_slice(&encode_i64(nt.uncompressed_size, is_be));
    out.extend_from_slice(&encode_size_pvd(nt.dimension.len(), is_be));
    for NdDimension {
        size,
        offset,
        full_size,
        binning,
        reverse,
    } in &nt.dimension
    {
        out.push(1); // non-null element indicator
        out.extend_from_slice(&encode_i32(*size, is_be));
        out.extend_from_slice(&encode_i32(*offset, is_be));
        out.extend_from_slice(&encode_i32(*full_size, is_be));
        out.extend_from_slice(&encode_i32(*binning, is_be));
        out.push(if *reverse { 1 } else { 0 });
    }
    out.extend_from_slice(&encode_i32(nt.unique_id, is_be));
    out.extend_from_slice(&encode_nt_timestamp(&nt.data_time_stamp, is_be));
    out.extend_from_slice(&encode_size_pvd(nt.attribute.len(), is_be));
    for attr in &nt.attribute {
        out.push(1); // non-null element indicator
        out.extend_from_slice(&encode_string_pvd(&attr.name, is_be));
        out.extend_from_slice(&encode_attribute_variant(attr, is_be));
        out.extend_from_slice(&encode_string_pvd(&attr.descriptor, is_be));
        out.extend_from_slice(&encode_i32(attr.source_type, is_be));
        out.extend_from_slice(&encode_string_pvd(&attr.source, is_be));
    }
    out.extend_from_slice(&encode_string_pvd(
        nt.descriptor.as_deref().unwrap_or(""),
        is_be,
    ));
    out.extend_from_slice(&encode_nt_alarm(
        nt.alarm.as_ref().unwrap_or(&NtAlarm::default()),
        is_be,
    ));
    out.extend_from_slice(&encode_nt_timestamp(
        nt.time_stamp.as_ref().unwrap_or(&NtTimeStamp::default()),
        is_be,
    ));
    out.extend_from_slice(&encode_nt_display(
        nt.display.as_ref().unwrap_or(&NtDisplay::default()),
        is_be,
    ));
    out
}

// ---------------------------------------------------------------------------
// NTEnum descriptor & encoder
// ---------------------------------------------------------------------------

pub fn nt_enum_desc() -> StructureDesc {
    StructureDesc {
        struct_id: Some("epics:nt/NTEnum:1.0".to_string()),
        fields: vec![
            FieldDesc {
                name: "value".to_string(),
                field_type: FieldType::Structure(StructureDesc {
                    struct_id: Some("enum_t".to_string()),
                    fields: vec![
                        FieldDesc {
                            name: "index".to_string(),
                            field_type: FieldType::Scalar(TypeCode::Int32),
                        },
                        FieldDesc {
                            name: "choices".to_string(),
                            field_type: FieldType::StringArray,
                        },
                    ],
                }),
            },
            FieldDesc {
                name: "alarm".to_string(),
                field_type: FieldType::Structure(alarm_desc()),
            },
            FieldDesc {
                name: "timeStamp".to_string(),
                field_type: FieldType::Structure(timestamp_desc()),
            },
        ],
    }
}

pub fn encode_nt_enum_full(nt: &NtEnum, is_be: bool) -> Vec<u8> {
    let mut out = Vec::new();
    // value — enum_t { index, choices }
    out.extend_from_slice(&encode_enum(nt.index, &nt.choices, is_be));
    // alarm
    out.extend_from_slice(&encode_nt_alarm(&nt.alarm, is_be));
    // timeStamp
    out.extend_from_slice(&encode_nt_timestamp(&nt.time_stamp, is_be));
    out
}

// ---------------------------------------------------------------------------
// PvValue (generic recursive) descriptor & encoder
// ---------------------------------------------------------------------------

fn scalar_value_type_code(v: &ScalarValue) -> TypeCode {
    match v {
        ScalarValue::Bool(_) => TypeCode::Boolean,
        ScalarValue::I8(_) => TypeCode::Int8,
        ScalarValue::I16(_) => TypeCode::Int16,
        ScalarValue::I32(_) => TypeCode::Int32,
        ScalarValue::I64(_) => TypeCode::Int64,
        ScalarValue::U8(_) => TypeCode::UInt8,
        ScalarValue::U16(_) => TypeCode::UInt16,
        ScalarValue::U32(_) => TypeCode::UInt32,
        ScalarValue::U64(_) => TypeCode::UInt64,
        ScalarValue::F32(_) => TypeCode::Float32,
        ScalarValue::F64(_) => TypeCode::Float64,
        ScalarValue::Str(_) => TypeCode::String,
    }
}

/// Build a [`StructureDesc`] from a [`PvValue::Structure`].
pub fn pv_value_desc(struct_id: &str, fields: &[(String, PvValue)]) -> StructureDesc {
    StructureDesc {
        struct_id: if struct_id.is_empty() {
            None
        } else {
            Some(struct_id.to_string())
        },
        fields: fields
            .iter()
            .map(|(name, val)| FieldDesc {
                name: name.clone(),
                field_type: pv_value_field_type(val),
            })
            .collect(),
    }
}

fn pv_value_field_type(val: &PvValue) -> FieldType {
    match val {
        PvValue::Scalar(sv) => {
            if matches!(sv, ScalarValue::Str(_)) {
                FieldType::String
            } else {
                FieldType::Scalar(scalar_value_type_code(sv))
            }
        }
        PvValue::ScalarArray(sa) => scalar_array_field_type(sa),
        PvValue::Structure { struct_id, fields } => {
            FieldType::Structure(pv_value_desc(struct_id, fields))
        }
    }
}

/// Encode a [`PvValue`] tree to PVA wire bytes (values only, no descriptor).
pub fn encode_pv_value(val: &PvValue, is_be: bool) -> Vec<u8> {
    match val {
        PvValue::Scalar(sv) => encode_scalar_value(sv, is_be),
        PvValue::ScalarArray(sa) => encode_scalar_array_value_pvd(sa, is_be),
        PvValue::Structure { fields, .. } => {
            let mut out = Vec::new();
            for (_, v) in fields {
                out.extend_from_slice(&encode_pv_value(v, is_be));
            }
            out
        }
    }
}

pub fn nt_payload_desc(payload: &NtPayload) -> StructureDesc {
    match payload {
        NtPayload::Scalar(nt) => nt_scalar_desc(&nt.value),
        NtPayload::ScalarArray(nt) => nt_scalar_array_desc(&nt.value),
        NtPayload::Table(nt) => nt_table_desc(nt),
        NtPayload::NdArray(nt) => nt_ndarray_desc(nt),
        NtPayload::Enum(_) => nt_enum_desc(),
        NtPayload::Generic { struct_id, fields } => pv_value_desc(struct_id, fields),
    }
}

pub fn encode_nt_payload_full(payload: &NtPayload, is_be: bool) -> Vec<u8> {
    match payload {
        NtPayload::Scalar(nt) => encode_nt_scalar_full(nt, is_be),
        NtPayload::ScalarArray(nt) => encode_nt_scalar_array_full(nt, is_be),
        NtPayload::Table(nt) => encode_nt_table_full(nt, is_be),
        NtPayload::NdArray(nt) => encode_nt_ndarray_full(nt, is_be),
        NtPayload::Enum(nt) => encode_nt_enum_full(nt, is_be),
        NtPayload::Generic { fields, .. } => {
            let mut out = Vec::new();
            for (_, v) in fields {
                out.extend_from_slice(&encode_pv_value(v, is_be));
            }
            out
        }
    }
}

pub fn encode_nt_payload_bitset(payload: &NtPayload, is_be: bool) -> Vec<u8> {
    let desc = nt_payload_desc(payload);
    let mut out = Vec::new();
    out.extend_from_slice(&encode_structure_bitset(&desc, is_be));
    out.extend_from_slice(&encode_nt_payload_full(payload, is_be));
    out
}

pub fn encode_nt_payload_bitset_parts(payload: &NtPayload, is_be: bool) -> (Vec<u8>, Vec<u8>) {
    let desc = nt_payload_desc(payload);
    (
        encode_structure_bitset(&desc, is_be),
        encode_nt_payload_full(payload, is_be),
    )
}

// ---------------------------------------------------------------------------
// Generic DecodedValue → wire bytes encoder
// ---------------------------------------------------------------------------

use crate::spvd_decode::DecodedValue;

/// Encode a `DecodedValue` back to PVA wire bytes.
pub fn encode_decoded_value(val: &DecodedValue, is_be: bool) -> Vec<u8> {
    match val {
        DecodedValue::Null => Vec::new(),
        DecodedValue::Boolean(v) => vec![if *v { 1 } else { 0 }],
        DecodedValue::Int8(v) => vec![*v as u8],
        DecodedValue::Int16(v) => {
            if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            }
        }
        DecodedValue::Int32(v) => encode_i32(*v, is_be),
        DecodedValue::Int64(v) => encode_i64(*v, is_be),
        DecodedValue::UInt8(v) => vec![*v],
        DecodedValue::UInt16(v) => {
            if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            }
        }
        DecodedValue::UInt32(v) => {
            if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            }
        }
        DecodedValue::UInt64(v) => {
            if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            }
        }
        DecodedValue::Float32(v) => {
            if is_be {
                v.to_be_bytes().to_vec()
            } else {
                v.to_le_bytes().to_vec()
            }
        }
        DecodedValue::Float64(v) => encode_f64(*v, is_be),
        DecodedValue::String(v) => encode_string_pvd(v, is_be),
        DecodedValue::Array(arr) => {
            let mut out = encode_size_pvd(arr.len(), is_be);
            for item in arr {
                out.extend_from_slice(&encode_decoded_value(item, is_be));
            }
            out
        }
        DecodedValue::Structure(fields) => {
            let mut out = Vec::new();
            for (_name, value) in fields {
                out.extend_from_slice(&encode_decoded_value(value, is_be));
            }
            out
        }
        DecodedValue::Raw(data) => data.clone(),
    }
}

// ---------------------------------------------------------------------------
// pvRequest parsing & descriptor filtering
// ---------------------------------------------------------------------------

/// Parse a pvRequest structure from the INIT body bytes and return the list
/// of requested top-level field names.
///
/// Returns `None` if the body is empty or cannot be parsed, which should be
/// treated as "return all fields" (no filtering).
pub fn decode_pv_request_fields(body: &[u8], is_be: bool) -> Option<Vec<String>> {
    if body.is_empty() {
        return None;
    }
    let decoder = crate::spvd_decode::PvdDecoder::new(is_be);
    let desc = decoder.parse_introspection(body)?;
    // Find the "field" sub-structure.
    for field in &desc.fields {
        if field.name == "field" {
            if let FieldType::Structure(ref inner) = field.field_type {
                if inner.fields.is_empty() {
                    // Empty "field {}" means all fields.
                    return None;
                }
                let names: Vec<String> = inner.fields.iter().map(|f| f.name.clone()).collect();
                return Some(names);
            }
        }
    }
    None
}

/// Filter a [`StructureDesc`] to include only the listed top-level field
/// names.  Unknown names are silently ignored.  If `requested` is empty the
/// original descriptor is returned unchanged.
pub fn filter_structure_desc(desc: &StructureDesc, requested: &[String]) -> StructureDesc {
    if requested.is_empty() {
        return desc.clone();
    }
    StructureDesc {
        struct_id: desc.struct_id.clone(),
        fields: desc
            .fields
            .iter()
            .filter(|f| requested.iter().any(|r| r == &f.name))
            .cloned()
            .collect(),
    }
}

/// Encode only the fields of an [`NtPayload`] whose names appear in
/// `desc`.  The bitset and value bytes are computed against the filtered
/// descriptor so that a client that received the filtered INIT descriptor
/// will decode them correctly.
pub fn encode_nt_payload_filtered(
    payload: &NtPayload,
    filtered_desc: &StructureDesc,
    is_be: bool,
) -> (Vec<u8>, Vec<u8>) {
    let requested: Vec<&str> = filtered_desc
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    let full_desc = nt_payload_desc(payload);
    let full_fields = &full_desc.fields;

    // Map each field in the full descriptor to its encoded bytes.
    let field_bytes: Vec<(&str, Vec<u8>)> = encode_nt_payload_fields(payload, full_fields, is_be);

    // Build the filtered values and a full-set bitset over the filtered desc.
    let mut values = Vec::new();
    for (name, bytes) in &field_bytes {
        if requested.iter().any(|r| *r == *name) {
            values.extend_from_slice(bytes);
        }
    }

    let bitset = encode_structure_bitset(filtered_desc, is_be);
    (bitset, values)
}

/// Helper: encode each top-level field of an NtPayload separately, returning
/// `(field_name, encoded_bytes)` pairs in descriptor order.
fn encode_nt_table_field(nt: &NtTable, name: &str, is_be: bool) -> Vec<u8> {
    match name {
        "labels" => encode_string_array(&nt.labels, is_be),
        "value" => {
            let mut out = Vec::new();
            for NtTableColumn { values, .. } in &nt.columns {
                out.extend_from_slice(&encode_scalar_array_value_pvd(values, is_be));
            }
            out
        }
        _ => Vec::new(),
    }
}

fn encode_nt_ndarray_field(nt: &NtNdArray, name: &str, is_be: bool) -> Vec<u8> {
    match name {
        "value" => encode_ndarray_union(&nt.value, is_be),
        "codec" => {
            let mut out = Vec::new();
            out.extend_from_slice(&encode_string_pvd(&nt.codec.name, is_be));
            out.extend_from_slice(&encode_codec_parameters(&nt.codec.parameters, is_be));
            out
        }
        "compressedSize" => encode_i64(nt.compressed_size, is_be),
        "uncompressedSize" => encode_i64(nt.uncompressed_size, is_be),
        "dimension" => {
            let mut out = encode_size_pvd(nt.dimension.len(), is_be);
            for d in &nt.dimension {
                out.push(1);
                out.extend_from_slice(&encode_i32(d.size, is_be));
                out.extend_from_slice(&encode_i32(d.offset, is_be));
                out.extend_from_slice(&encode_i32(d.full_size, is_be));
                out.extend_from_slice(&encode_i32(d.binning, is_be));
                out.push(if d.reverse { 1 } else { 0 });
            }
            out
        }
        "uniqueId" => encode_i32(nt.unique_id, is_be),
        "dataTimeStamp" => encode_nt_timestamp(&nt.data_time_stamp, is_be),
        "attribute" => {
            let mut out = encode_size_pvd(nt.attribute.len(), is_be);
            for attr in &nt.attribute {
                out.push(1);
                out.extend_from_slice(&encode_string_pvd(&attr.name, is_be));
                out.extend_from_slice(&encode_attribute_variant(attr, is_be));
                out.extend_from_slice(&encode_string_pvd(&attr.descriptor, is_be));
                out.extend_from_slice(&encode_i32(attr.source_type, is_be));
                out.extend_from_slice(&encode_string_pvd(&attr.source, is_be));
            }
            out
        }
        "descriptor" => encode_string_pvd(nt.descriptor.as_deref().unwrap_or(""), is_be),
        "alarm" => encode_nt_alarm(nt.alarm.as_ref().unwrap_or(&NtAlarm::default()), is_be),
        "timeStamp" => encode_nt_timestamp(
            nt.time_stamp.as_ref().unwrap_or(&NtTimeStamp::default()),
            is_be,
        ),
        "display" => encode_nt_display(nt.display.as_ref().unwrap_or(&NtDisplay::default()), is_be),
        _ => Vec::new(),
    }
}

fn encode_nt_payload_fields<'a>(
    payload: &'a NtPayload,
    full_fields: &'a [FieldDesc],
    is_be: bool,
) -> Vec<(&'a str, Vec<u8>)> {
    // NTScalar field encoders
    fn scalar_field(nt: &NtScalar, name: &str, is_be: bool) -> Vec<u8> {
        match name {
            "value" => encode_scalar_value(&nt.value, is_be),
            "alarm" => encode_alarm(nt, is_be),
            "timeStamp" => encode_timestamp(nt, is_be),
            "display" => encode_display(nt, is_be),
            "control" => encode_control(nt, is_be),
            "valueAlarm" => encode_value_alarm(nt, is_be),
            _ => Vec::new(),
        }
    }

    fn scalar_array_field(nt: &NtScalarArray, name: &str, is_be: bool) -> Vec<u8> {
        match name {
            "value" => encode_scalar_array_value_pvd(&nt.value, is_be),
            "alarm" => encode_nt_alarm(&nt.alarm, is_be),
            "timeStamp" => encode_nt_timestamp(&nt.time_stamp, is_be),
            "display" => encode_nt_display(&nt.display, is_be),
            "control" => {
                let mut out = Vec::new();
                out.extend_from_slice(&encode_f64(nt.control.limit_low, is_be));
                out.extend_from_slice(&encode_f64(nt.control.limit_high, is_be));
                out.extend_from_slice(&encode_f64(nt.control.min_step, is_be));
                out
            }
            _ => Vec::new(),
        }
    }

    fn enum_field(nt: &NtEnum, name: &str, is_be: bool) -> Vec<u8> {
        match name {
            "value" => encode_enum(nt.index, &nt.choices, is_be),
            "alarm" => encode_nt_alarm(&nt.alarm, is_be),
            "timeStamp" => encode_nt_timestamp(&nt.time_stamp, is_be),
            _ => Vec::new(),
        }
    }

    full_fields
        .iter()
        .map(|f| {
            let name = f.name.as_str();
            let bytes = match payload {
                NtPayload::Scalar(nt) => scalar_field(nt, name, is_be),
                NtPayload::ScalarArray(nt) => scalar_array_field(nt, name, is_be),
                NtPayload::Table(nt) => encode_nt_table_field(nt, name, is_be),
                NtPayload::NdArray(nt) => encode_nt_ndarray_field(nt, name, is_be),
                NtPayload::Enum(nt) => enum_field(nt, name, is_be),
                NtPayload::Generic { fields, .. } => {
                    if let Some((_, v)) = fields.iter().find(|(n, _)| n == name) {
                        encode_pv_value(v, is_be)
                    } else {
                        Vec::new()
                    }
                }
            };
            (name, bytes)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// pvRequest builder
// ---------------------------------------------------------------------------

/// Build a pvRequest structure for the given top-level field names.
///
/// Produces the byte sequence that a client sends inside an INIT request to
/// select which fields to subscribe to, e.g.
/// `encode_pv_request(&["value", "alarm", "timeStamp"], false)` produces the
/// equivalent of `field(value,alarm,timeStamp)`.
///
/// The output is the *full* type-described pvRequest structure: a `0xFD` /
/// `0x80` tag followed by the structure descriptor and empty-struct field values.
pub fn encode_pv_request(fields: &[&str], is_be: bool) -> Vec<u8> {
    // Build inner "field" structure descriptor: each requested field is an
    // empty sub-structure (no fields).
    let inner_fields: Vec<FieldDesc> = fields
        .iter()
        .map(|name| FieldDesc {
            name: name.to_string(),
            field_type: FieldType::Structure(StructureDesc {
                struct_id: None,
                fields: Vec::new(),
            }),
        })
        .collect();

    let field_desc = StructureDesc {
        struct_id: None,
        fields: inner_fields,
    };

    let pv_request_desc = StructureDesc {
        struct_id: None,
        fields: vec![FieldDesc {
            name: "field".to_string(),
            field_type: FieldType::Structure(field_desc),
        }],
    };

    let mut out = Vec::new();
    out.push(0x80); // structure tag
    out.extend_from_slice(&encode_structure_desc(&pv_request_desc, is_be));
    // Values: the field structure and all its children are empty structs, so
    // there are no value bytes to write.
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spvd_decode::PvdDecoder;

    #[test]
    fn nt_scalar_roundtrip() {
        let nt = NtScalar::from_value(ScalarValue::F64(12.5));
        let desc = nt_scalar_desc(&nt.value);
        let desc_bytes = encode_structure_desc(&desc, false);
        let mut pvd = Vec::new();
        pvd.push(0x80);
        pvd.extend_from_slice(&desc_bytes);
        pvd.extend_from_slice(&encode_nt_scalar_full(&nt, false));

        let decoder = PvdDecoder::new(false);
        let parsed_desc = decoder.parse_introspection(&pvd).expect("desc");
        let (_, consumed) = decoder
            .decode_structure(&pvd[1 + desc_bytes.len()..], &parsed_desc)
            .expect("value");
        assert!(consumed > 0);
    }

    #[test]
    fn nt_ndarray_roundtrip() {
        use spvirit_types::{
            NdCodec, NdDimension, NtAlarm, NtNdArray, NtTimeStamp, ScalarArrayValue,
        };
        use std::collections::HashMap;

        let nt = NtNdArray {
            value: ScalarArrayValue::U8(vec![1, 2, 3, 4]),
            codec: NdCodec {
                name: String::new(),
                parameters: HashMap::new(),
            },
            compressed_size: 4,
            uncompressed_size: 4,
            dimension: vec![NdDimension {
                size: 2,
                offset: 0,
                full_size: 2,
                binning: 1,
                reverse: false,
            }],
            unique_id: 42,
            data_time_stamp: NtTimeStamp {
                seconds_past_epoch: 1000,
                nanoseconds: 500,
                user_tag: 0,
            },
            attribute: Vec::new(),
            descriptor: Some("test".to_string()),
            alarm: Some(NtAlarm::default()),
            time_stamp: Some(NtTimeStamp::default()),
            display: None,
        };

        let desc = nt_ndarray_desc(&nt);
        let desc_bytes = encode_structure_desc(&desc, false);
        let data_bytes = encode_nt_ndarray_full(&nt, false);

        // Build complete PVD: type_tag + desc + data
        let mut pvd = Vec::new();
        pvd.push(0x80);
        pvd.extend_from_slice(&desc_bytes);
        pvd.extend_from_slice(&data_bytes);

        let decoder = PvdDecoder::new(false);
        let parsed_desc = decoder
            .parse_introspection(&pvd)
            .expect("desc parse failed");
        let data_start = 1 + desc_bytes.len();
        let (_decoded, consumed) = decoder
            .decode_structure(&pvd[data_start..], &parsed_desc)
            .expect("data decode failed");
        assert!(consumed > 0, "consumed should be > 0");
        assert_eq!(
            consumed,
            data_bytes.len(),
            "consumed should match data_bytes.len()"
        );
    }
}
