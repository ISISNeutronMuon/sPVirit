//! Functions that apply decoded PUT values to Normative Type payloads.

use spvirit_codec::spvd_decode::DecodedValue;
use spvirit_types::*;

use crate::convert::*;

/// Apply a scalar value update from a decoded PUT body to an `NtScalar`.
pub fn apply_value_update(nt: &mut NtScalar, val: &DecodedValue, compute_alarms: bool) -> bool {
    if let DecodedValue::Structure(fields) = val {
        if let Some((_, inner)) = fields.iter().find(|(name, _)| name == "value") {
            return apply_value_update(nt, inner, compute_alarms);
        }
    }
    match &mut nt.value {
        ScalarValue::Bool(current) => {
            if let Some(v) = decoded_to_bool(val) {
                *current = v;
                if compute_alarms {
                    nt.update_alarm_from_value();
                }
                return true;
            }
        }
        ScalarValue::I32(current) => {
            if let Some(v) = decoded_to_i32(val) {
                *current = v;
                if compute_alarms {
                    nt.update_alarm_from_value();
                }
                return true;
            }
        }
        ScalarValue::F64(current) => {
            if let Some(v) = decoded_to_f64(val) {
                *current = v;
                if compute_alarms {
                    nt.update_alarm_from_value();
                }
                return true;
            }
        }
        ScalarValue::Str(current) => {
            if let Some(v) = decoded_to_string(val) {
                *current = v;
                if compute_alarms {
                    nt.update_alarm_from_value();
                }
                return true;
            }
        }
        _ => {
            if let Some(v) = decoded_to_f64(val) {
                match &mut nt.value {
                    ScalarValue::I8(c) => { *c = v as i8; }
                    ScalarValue::I16(c) => { *c = v as i16; }
                    ScalarValue::I64(c) => { *c = v as i64; }
                    ScalarValue::U8(c) => { *c = v as u8; }
                    ScalarValue::U16(c) => { *c = v as u16; }
                    ScalarValue::U32(c) => { *c = v as u32; }
                    ScalarValue::U64(c) => { *c = v as u64; }
                    ScalarValue::F32(c) => { *c = v as f32; }
                    _ => return false,
                }
                if compute_alarms {
                    nt.update_alarm_from_value();
                }
                return true;
            }
        }
    }
    false
}

/// Apply an alarm structure update to an `NtScalar`.
pub fn apply_alarm_update(nt: &mut NtScalar, val: &DecodedValue) -> bool {
    let DecodedValue::Structure(fields) = val else {
        return false;
    };
    let mut changed = false;
    for (name, v) in fields {
        match name.as_str() {
            "severity" => {
                if let Some(i) = decoded_to_i32(v) {
                    nt.alarm_severity = i;
                    changed = true;
                }
            }
            "status" => {
                if let Some(i) = decoded_to_i32(v) {
                    nt.alarm_status = i;
                    changed = true;
                }
            }
            "message" => {
                if let Some(s) = decoded_to_string(v) {
                    nt.alarm_message = s;
                    changed = true;
                }
            }
            _ => {}
        }
    }
    changed
}

/// Apply a display structure update to an `NtScalar`.
pub fn apply_display_update(nt: &mut NtScalar, val: &DecodedValue) -> bool {
    let DecodedValue::Structure(fields) = val else {
        return false;
    };
    let mut changed = false;
    for (name, v) in fields {
        match name.as_str() {
            "low" | "limitLow" => {
                if let Some(f) = decoded_to_f64(v) {
                    nt.display_low = f;
                    changed = true;
                }
            }
            "high" | "limitHigh" => {
                if let Some(f) = decoded_to_f64(v) {
                    nt.display_high = f;
                    changed = true;
                }
            }
            "description" => {
                if let Some(s) = decoded_to_string(v) {
                    nt.display_description = s;
                    changed = true;
                }
            }
            "units" => {
                if let Some(s) = decoded_to_string(v) {
                    nt.units = s;
                    changed = true;
                }
            }
            "precision" => {
                if let Some(i) = decoded_to_i32(v) {
                    nt.display_precision = i;
                    changed = true;
                }
            }
            "form" => {
                if let DecodedValue::Structure(form_fields) = v {
                    let mut updated = false;
                    for (fname, fval) in form_fields {
                        match fname.as_str() {
                            "index" => {
                                if let Some(i) = decoded_to_i32(fval) {
                                    nt.display_form_index = i;
                                    updated = true;
                                }
                            }
                            "choices" => {
                                if let DecodedValue::Array(items) = fval {
                                    let mut choices = Vec::new();
                                    for item in items {
                                        if let DecodedValue::String(s) = item {
                                            choices.push(s.clone());
                                        }
                                    }
                                    if !choices.is_empty() {
                                        nt.display_form_choices = choices;
                                        updated = true;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    if updated {
                        changed = true;
                    }
                }
            }
            _ => {}
        }
    }
    changed
}

/// Apply a control structure update to an `NtScalar`.
pub fn apply_control_update(nt: &mut NtScalar, val: &DecodedValue) -> bool {
    let DecodedValue::Structure(fields) = val else {
        return false;
    };
    let mut changed = false;
    for (name, v) in fields {
        match name.as_str() {
            "low" | "limitLow" => {
                if let Some(f) = decoded_to_f64(v) {
                    nt.control_low = f;
                    changed = true;
                }
            }
            "high" | "limitHigh" => {
                if let Some(f) = decoded_to_f64(v) {
                    nt.control_high = f;
                    changed = true;
                }
            }
            "minStep" => {
                if let Some(f) = decoded_to_f64(v) {
                    nt.control_min_step = f;
                    changed = true;
                }
            }
            _ => {}
        }
    }
    changed
}

/// Apply a scalar-array PUT update to an `NtScalarArray`.
pub fn apply_scalar_array_put(
    nt: &mut NtScalarArray,
    nord: &mut usize,
    value: &DecodedValue,
) -> bool {
    let field_value = match value {
        DecodedValue::Structure(fields) => fields
            .iter()
            .find(|(name, _)| name == "value")
            .map(|(_, v)| v)
            .unwrap_or(value),
        _ => value,
    };
    if let Some(next) = decoded_to_scalar_array(field_value, &nt.value) {
        let changed = nt.value != next;
        if changed {
            *nord = next.len();
            nt.value = next;
        }
        return changed;
    }
    false
}

/// Apply a table PUT update to an `NtTable`.
pub fn apply_table_put(nt: &mut NtTable, value: &DecodedValue) -> bool {
    let DecodedValue::Structure(fields) = value else {
        return false;
    };
    let mut changed = false;
    for (name, field_value) in fields {
        match name.as_str() {
            "labels" => {
                if let DecodedValue::Array(items) = field_value {
                    let labels: Vec<String> = items.iter().filter_map(decoded_to_string).collect();
                    if !labels.is_empty() && nt.labels != labels {
                        nt.labels = labels;
                        changed = true;
                    }
                }
            }
            "value" => {
                if let DecodedValue::Structure(cols) = field_value {
                    for (col_name, col_value) in cols {
                        if let Some(col) = nt.columns.iter_mut().find(|c| c.name == *col_name) {
                            if let Some(next) = decoded_to_scalar_array(col_value, &col.values) {
                                if col.values != next {
                                    col.values = next;
                                    changed = true;
                                }
                            }
                        }
                    }
                }
            }
            "descriptor" => {
                if let Some(s) = decoded_to_string(field_value) {
                    let next = if s.is_empty() { None } else { Some(s) };
                    if nt.descriptor != next {
                        nt.descriptor = next;
                        changed = true;
                    }
                }
            }
            "alarm" => {
                if let Some(alarm) = decode_nt_alarm(field_value) {
                    if nt.alarm.as_ref() != Some(&alarm) {
                        nt.alarm = Some(alarm);
                        changed = true;
                    }
                }
            }
            "timeStamp" => {
                if let Some(ts) = decode_nt_timestamp(field_value) {
                    if nt.time_stamp.as_ref() != Some(&ts) {
                        nt.time_stamp = Some(ts);
                        changed = true;
                    }
                }
            }
            _ => {}
        }
    }
    changed
}

/// Apply an NdArray PUT update to an `NtNdArray`.
pub fn apply_ndarray_put(nt: &mut NtNdArray, value: &DecodedValue) -> bool {
    let DecodedValue::Structure(fields) = value else {
        return false;
    };
    let mut changed = false;
    for (name, field_value) in fields {
        match name.as_str() {
            "value" => {
                if let Some(next) = decoded_to_scalar_array(field_value, &nt.value) {
                    if nt.value != next {
                        nt.value = next;
                        changed = true;
                    }
                }
            }
            "compressedSize" => {
                if let Some(v) = decoded_to_i64(field_value) {
                    if nt.compressed_size != v {
                        nt.compressed_size = v;
                        changed = true;
                    }
                }
            }
            "uncompressedSize" => {
                if let Some(v) = decoded_to_i64(field_value) {
                    if nt.uncompressed_size != v {
                        nt.uncompressed_size = v;
                        changed = true;
                    }
                }
            }
            "uniqueId" => {
                if let Some(v) = decoded_to_i32(field_value) {
                    if nt.unique_id != v {
                        nt.unique_id = v;
                        changed = true;
                    }
                }
            }
            "codec" => {
                if let DecodedValue::Structure(codec_fields) = field_value {
                    for (cname, cval) in codec_fields {
                        if cname == "name" {
                            if let Some(s) = decoded_to_string(cval) {
                                if nt.codec.name != s {
                                    nt.codec.name = s;
                                    changed = true;
                                }
                            }
                        }
                    }
                }
            }
            "dimension" => {
                if let DecodedValue::Array(items) = field_value {
                    let dims: Vec<NdDimension> = items
                        .iter()
                        .filter_map(|item| {
                            if let DecodedValue::Structure(fs) = item {
                                Some(NdDimension {
                                    size: fs.iter().find(|(n, _)| n == "size").and_then(|(_, v)| decoded_to_i32(v)).unwrap_or(0),
                                    offset: fs.iter().find(|(n, _)| n == "offset").and_then(|(_, v)| decoded_to_i32(v)).unwrap_or(0),
                                    full_size: fs.iter().find(|(n, _)| n == "fullSize").and_then(|(_, v)| decoded_to_i32(v)).unwrap_or(0),
                                    binning: fs.iter().find(|(n, _)| n == "binning").and_then(|(_, v)| decoded_to_i32(v)).unwrap_or(1),
                                    reverse: fs.iter().find(|(n, _)| n == "reverse").and_then(|(_, v)| decoded_to_bool(v)).unwrap_or(false),
                                })
                            } else {
                                None
                            }
                        })
                        .collect();
                    if !dims.is_empty() && nt.dimension != dims {
                        nt.dimension = dims;
                        changed = true;
                    }
                }
            }
            "descriptor" => {
                if let Some(s) = decoded_to_string(field_value) {
                    let next = if s.is_empty() { None } else { Some(s) };
                    if nt.descriptor != next {
                        nt.descriptor = next;
                        changed = true;
                    }
                }
            }
            "alarm" => {
                if let Some(alarm) = decode_nt_alarm(field_value) {
                    if nt.alarm.as_ref() != Some(&alarm) {
                        nt.alarm = Some(alarm);
                        changed = true;
                    }
                }
            }
            "timeStamp" => {
                if let Some(ts) = decode_nt_timestamp(field_value) {
                    if nt.time_stamp.as_ref() != Some(&ts) {
                        nt.time_stamp = Some(ts);
                        changed = true;
                    }
                }
            }
            "dataTimeStamp" => {
                if let Some(ts) = decode_nt_timestamp(field_value) {
                    if nt.data_time_stamp != ts {
                        nt.data_time_stamp = ts;
                        changed = true;
                    }
                }
            }
            "display" => {
                if let Some(display) = decode_nt_display(field_value) {
                    if nt.display.as_ref() != Some(&display) {
                        nt.display = Some(display);
                        changed = true;
                    }
                }
            }
            "attribute" => {
                if let DecodedValue::Array(items) = field_value {
                    let attrs: Vec<NtAttribute> = items
                        .iter()
                        .filter_map(|item| {
                            if let DecodedValue::Structure(fs) = item {
                                let attr_name = fs.iter().find(|(n, _)| n == "name").and_then(|(_, v)| decoded_to_string(v)).unwrap_or_default();
                                let attr_value = fs.iter().find(|(n, _)| n == "value").map(|(_, v)| decoded_to_scalar_value(v)).unwrap_or(ScalarValue::I32(0));
                                let descriptor = fs.iter().find(|(n, _)| n == "descriptor").and_then(|(_, v)| decoded_to_string(v)).unwrap_or_default();
                                let source_type = fs.iter().find(|(n, _)| n == "sourceType").and_then(|(_, v)| decoded_to_i32(v)).unwrap_or(0);
                                let source = fs.iter().find(|(n, _)| n == "source").and_then(|(_, v)| decoded_to_string(v)).unwrap_or_default();
                                Some(NtAttribute {
                                    name: attr_name,
                                    value: attr_value,
                                    descriptor,
                                    source_type,
                                    source,
                                })
                            } else {
                                None
                            }
                        })
                        .collect();
                    if !attrs.is_empty() && nt.attribute != attrs {
                        nt.attribute = attrs;
                        changed = true;
                    }
                }
            }
            _ => {}
        }
    }
    changed
}
