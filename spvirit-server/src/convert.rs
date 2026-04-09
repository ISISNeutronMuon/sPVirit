//! Type conversions from [`DecodedValue`] to Rust scalars and `ScalarValue`.

use spvirit_codec::spvd_decode::DecodedValue;
use spvirit_types::{ScalarArrayValue, ScalarValue};

pub fn decoded_to_bool(val: &DecodedValue) -> Option<bool> {
    match val {
        DecodedValue::Boolean(v) => Some(*v),
        DecodedValue::Int8(v) => Some(*v != 0),
        DecodedValue::Int16(v) => Some(*v != 0),
        DecodedValue::Int32(v) => Some(*v != 0),
        DecodedValue::Int64(v) => Some(*v != 0),
        DecodedValue::UInt8(v) => Some(*v != 0),
        DecodedValue::UInt16(v) => Some(*v != 0),
        DecodedValue::UInt32(v) => Some(*v != 0),
        DecodedValue::UInt64(v) => Some(*v != 0),
        DecodedValue::Float32(v) => Some(*v != 0.0),
        DecodedValue::Float64(v) => Some(*v != 0.0),
        _ => None,
    }
}

pub fn decoded_to_i8(val: &DecodedValue) -> Option<i8> {
    decoded_to_i64(val).and_then(|v| i8::try_from(v).ok())
}

pub fn decoded_to_i16(val: &DecodedValue) -> Option<i16> {
    decoded_to_i64(val).and_then(|v| i16::try_from(v).ok())
}

pub fn decoded_to_i32(val: &DecodedValue) -> Option<i32> {
    match val {
        DecodedValue::Int8(v) => Some(*v as i32),
        DecodedValue::Int16(v) => Some(*v as i32),
        DecodedValue::Int32(v) => Some(*v),
        DecodedValue::Int64(v) => Some(*v as i32),
        DecodedValue::UInt8(v) => Some(*v as i32),
        DecodedValue::UInt16(v) => Some(*v as i32),
        DecodedValue::UInt32(v) => Some(*v as i32),
        DecodedValue::UInt64(v) => Some(*v as i32),
        DecodedValue::Boolean(v) => Some(if *v { 1 } else { 0 }),
        DecodedValue::Float32(v) => Some(*v as i32),
        DecodedValue::Float64(v) => Some(*v as i32),
        _ => None,
    }
}

pub fn decoded_to_i64(val: &DecodedValue) -> Option<i64> {
    match val {
        DecodedValue::Int8(v) => Some(*v as i64),
        DecodedValue::Int16(v) => Some(*v as i64),
        DecodedValue::Int32(v) => Some(*v as i64),
        DecodedValue::Int64(v) => Some(*v),
        DecodedValue::UInt8(v) => Some(*v as i64),
        DecodedValue::UInt16(v) => Some(*v as i64),
        DecodedValue::UInt32(v) => Some(*v as i64),
        DecodedValue::UInt64(v) => i64::try_from(*v).ok(),
        DecodedValue::Boolean(v) => Some(if *v { 1 } else { 0 }),
        DecodedValue::Float32(v) => Some(*v as i64),
        DecodedValue::Float64(v) => Some(*v as i64),
        _ => None,
    }
}

pub fn decoded_to_u8(val: &DecodedValue) -> Option<u8> {
    decoded_to_u64(val).and_then(|v| u8::try_from(v).ok())
}

pub fn decoded_to_u16(val: &DecodedValue) -> Option<u16> {
    decoded_to_u64(val).and_then(|v| u16::try_from(v).ok())
}

pub fn decoded_to_u32(val: &DecodedValue) -> Option<u32> {
    decoded_to_u64(val).and_then(|v| u32::try_from(v).ok())
}

pub fn decoded_to_u64(val: &DecodedValue) -> Option<u64> {
    match val {
        DecodedValue::Int8(v) => (*v >= 0).then_some(*v as u64),
        DecodedValue::Int16(v) => (*v >= 0).then_some(*v as u64),
        DecodedValue::Int32(v) => (*v >= 0).then_some(*v as u64),
        DecodedValue::Int64(v) => (*v >= 0).then_some(*v as u64),
        DecodedValue::UInt8(v) => Some(*v as u64),
        DecodedValue::UInt16(v) => Some(*v as u64),
        DecodedValue::UInt32(v) => Some(*v as u64),
        DecodedValue::UInt64(v) => Some(*v),
        DecodedValue::Boolean(v) => Some(if *v { 1 } else { 0 }),
        DecodedValue::Float32(v) => (*v >= 0.0).then_some(*v as u64),
        DecodedValue::Float64(v) => (*v >= 0.0).then_some(*v as u64),
        _ => None,
    }
}

pub fn decoded_to_f32(val: &DecodedValue) -> Option<f32> {
    decoded_to_f64(val).map(|v| v as f32)
}

pub fn decoded_to_f64(val: &DecodedValue) -> Option<f64> {
    match val {
        DecodedValue::Float64(v) => Some(*v),
        DecodedValue::Float32(v) => Some(*v as f64),
        DecodedValue::Int8(v) => Some(*v as f64),
        DecodedValue::Int16(v) => Some(*v as f64),
        DecodedValue::Int32(v) => Some(*v as f64),
        DecodedValue::Int64(v) => Some(*v as f64),
        DecodedValue::UInt8(v) => Some(*v as f64),
        DecodedValue::UInt16(v) => Some(*v as f64),
        DecodedValue::UInt32(v) => Some(*v as f64),
        DecodedValue::UInt64(v) => Some(*v as f64),
        DecodedValue::Boolean(v) => Some(if *v { 1.0 } else { 0.0 }),
        _ => None,
    }
}

pub fn decoded_to_string(val: &DecodedValue) -> Option<String> {
    match val {
        DecodedValue::String(s) => Some(s.clone()),
        _ => None,
    }
}

pub fn decoded_to_scalar_value(val: &DecodedValue) -> ScalarValue {
    if let Some(b) = decoded_to_bool(val) {
        return ScalarValue::Bool(b);
    }
    if let Some(i) = decoded_to_i32(val) {
        return ScalarValue::I32(i);
    }
    if let Some(f) = decoded_to_f64(val) {
        return ScalarValue::F64(f);
    }
    if let Some(s) = decoded_to_string(val) {
        return ScalarValue::Str(s);
    }
    ScalarValue::I32(0)
}

pub fn decoded_to_scalar_array(
    val: &DecodedValue,
    template: &ScalarArrayValue,
) -> Option<ScalarArrayValue> {
    let DecodedValue::Array(items) = val else {
        return None;
    };
    match template {
        ScalarArrayValue::Bool(_) => Some(ScalarArrayValue::Bool(
            items.iter().filter_map(decoded_to_bool).collect(),
        )),
        ScalarArrayValue::I8(_) => Some(ScalarArrayValue::I8(
            items.iter().filter_map(decoded_to_i8).collect(),
        )),
        ScalarArrayValue::I16(_) => Some(ScalarArrayValue::I16(
            items.iter().filter_map(decoded_to_i16).collect(),
        )),
        ScalarArrayValue::I32(_) => Some(ScalarArrayValue::I32(
            items.iter().filter_map(decoded_to_i32).collect(),
        )),
        ScalarArrayValue::I64(_) => Some(ScalarArrayValue::I64(
            items.iter().filter_map(decoded_to_i64).collect(),
        )),
        ScalarArrayValue::U8(_) => Some(ScalarArrayValue::U8(
            items.iter().filter_map(decoded_to_u8).collect(),
        )),
        ScalarArrayValue::U16(_) => Some(ScalarArrayValue::U16(
            items.iter().filter_map(decoded_to_u16).collect(),
        )),
        ScalarArrayValue::U32(_) => Some(ScalarArrayValue::U32(
            items.iter().filter_map(decoded_to_u32).collect(),
        )),
        ScalarArrayValue::U64(_) => Some(ScalarArrayValue::U64(
            items.iter().filter_map(decoded_to_u64).collect(),
        )),
        ScalarArrayValue::F32(_) => Some(ScalarArrayValue::F32(
            items.iter().filter_map(decoded_to_f32).collect(),
        )),
        ScalarArrayValue::F64(_) => Some(ScalarArrayValue::F64(
            items.iter().filter_map(decoded_to_f64).collect(),
        )),
        ScalarArrayValue::Str(_) => Some(ScalarArrayValue::Str(
            items.iter().filter_map(decoded_to_string).collect(),
        )),
    }
}

/// Decode an `NtAlarm` from a decoded PVA alarm structure.
pub fn decode_nt_alarm(val: &DecodedValue) -> Option<spvirit_types::NtAlarm> {
    let DecodedValue::Structure(fields) = val else {
        return None;
    };
    let severity = fields.iter().find(|(n, _)| n == "severity").and_then(|(_, v)| decoded_to_i32(v)).unwrap_or(0);
    let status = fields.iter().find(|(n, _)| n == "status").and_then(|(_, v)| decoded_to_i32(v)).unwrap_or(0);
    let message = fields.iter().find(|(n, _)| n == "message").and_then(|(_, v)| decoded_to_string(v)).unwrap_or_default();
    Some(spvirit_types::NtAlarm { severity, status, message })
}

/// Decode an `NtTimeStamp` from a decoded PVA timestamp structure.
pub fn decode_nt_timestamp(val: &DecodedValue) -> Option<spvirit_types::NtTimeStamp> {
    let DecodedValue::Structure(fields) = val else {
        return None;
    };
    let seconds = fields.iter().find(|(n, _)| n == "secondsPastEpoch").and_then(|(_, v)| decoded_to_i64(v)).unwrap_or(0);
    let nanos = fields.iter().find(|(n, _)| n == "nanoseconds").and_then(|(_, v)| decoded_to_i32(v)).unwrap_or(0);
    let user_tag = fields.iter().find(|(n, _)| n == "userTag").and_then(|(_, v)| decoded_to_i32(v)).unwrap_or(0);
    Some(spvirit_types::NtTimeStamp { seconds_past_epoch: seconds, nanoseconds: nanos, user_tag })
}

/// Decode an `NtDisplay` from a decoded PVA display structure.
pub fn decode_nt_display(val: &DecodedValue) -> Option<spvirit_types::NtDisplay> {
    let DecodedValue::Structure(fields) = val else {
        return None;
    };
    let limit_low = fields.iter().find(|(n, _)| n == "limitLow").and_then(|(_, v)| decoded_to_f64(v)).unwrap_or(0.0);
    let limit_high = fields.iter().find(|(n, _)| n == "limitHigh").and_then(|(_, v)| decoded_to_f64(v)).unwrap_or(0.0);
    let description = fields.iter().find(|(n, _)| n == "description").and_then(|(_, v)| decoded_to_string(v)).unwrap_or_default();
    let units = fields.iter().find(|(n, _)| n == "units").and_then(|(_, v)| decoded_to_string(v)).unwrap_or_default();
    let precision = fields.iter().find(|(n, _)| n == "precision").and_then(|(_, v)| decoded_to_i32(v)).unwrap_or(0);
    Some(spvirit_types::NtDisplay { limit_low, limit_high, description, units, precision })
}
