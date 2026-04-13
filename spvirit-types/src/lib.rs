//! Shared Normative Type (NT) data model types.
//!
//! These types represent the PVAccess Normative Types used across the
//! codec, client tools, server, and packet capture subsystems.

use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Str(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarArrayValue {
    Bool(Vec<bool>),
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Str(Vec<String>),
}

impl ScalarArrayValue {
    pub fn len(&self) -> usize {
        match self {
            Self::Bool(v) => v.len(),
            Self::I8(v) => v.len(),
            Self::I16(v) => v.len(),
            Self::I32(v) => v.len(),
            Self::I64(v) => v.len(),
            Self::U8(v) => v.len(),
            Self::U16(v) => v.len(),
            Self::U32(v) => v.len(),
            Self::U64(v) => v.len(),
            Self::F32(v) => v.len(),
            Self::F64(v) => v.len(),
            Self::Str(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn element_size_bytes(&self) -> usize {
        match self {
            Self::Bool(_) => 1,
            Self::I8(_) => 1,
            Self::I16(_) => 2,
            Self::I32(_) => 4,
            Self::I64(_) => 8,
            Self::U8(_) => 1,
            Self::U16(_) => 2,
            Self::U32(_) => 4,
            Self::U64(_) => 8,
            Self::F32(_) => 4,
            Self::F64(_) => 8,
            Self::Str(v) => v.iter().map(|s| s.len()).sum(),
        }
    }

    pub fn type_label(&self) -> &'static str {
        match self {
            Self::Bool(_) => "boolean[]",
            Self::I8(_) => "byte[]",
            Self::I16(_) => "short[]",
            Self::I32(_) => "int[]",
            Self::I64(_) => "long[]",
            Self::U8(_) => "ubyte[]",
            Self::U16(_) => "ushort[]",
            Self::U32(_) => "uint[]",
            Self::U64(_) => "ulong[]",
            Self::F32(_) => "float[]",
            Self::F64(_) => "double[]",
            Self::Str(_) => "string[]",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NtAlarm {
    pub severity: i32,
    pub status: i32,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NtTimeStamp {
    pub seconds_past_epoch: i64,
    pub nanoseconds: i32,
    pub user_tag: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NtDisplay {
    pub limit_low: f64,
    pub limit_high: f64,
    pub description: String,
    pub units: String,
    pub precision: i32,
}

impl Default for NtDisplay {
    fn default() -> Self {
        Self {
            limit_low: 0.0,
            limit_high: 0.0,
            description: String::new(),
            units: String::new(),
            precision: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NtControl {
    pub limit_low: f64,
    pub limit_high: f64,
    pub min_step: f64,
}

impl Default for NtControl {
    fn default() -> Self {
        Self {
            limit_low: 0.0,
            limit_high: 0.0,
            min_step: 0.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NtScalar {
    pub value: ScalarValue,
    pub alarm_severity: i32,
    pub alarm_status: i32,
    pub alarm_message: String,
    pub alarm_low: Option<f64>,
    pub alarm_high: Option<f64>,
    pub alarm_lolo: Option<f64>,
    pub alarm_hihi: Option<f64>,
    pub display_low: f64,
    pub display_high: f64,
    pub display_description: String,
    pub display_precision: i32,
    pub display_form_index: i32,
    pub display_form_choices: Vec<String>,
    pub control_low: f64,
    pub control_high: f64,
    pub control_min_step: f64,
    pub units: String,
    pub value_alarm_active: bool,
    pub value_alarm_low_alarm_limit: f64,
    pub value_alarm_low_warning_limit: f64,
    pub value_alarm_high_warning_limit: f64,
    pub value_alarm_high_alarm_limit: f64,
    pub value_alarm_low_alarm_severity: i32,
    pub value_alarm_low_warning_severity: i32,
    pub value_alarm_high_warning_severity: i32,
    pub value_alarm_high_alarm_severity: i32,
    pub value_alarm_hysteresis: u8,
}

impl NtScalar {
    pub fn from_value(value: ScalarValue) -> Self {
        Self {
            value,
            alarm_severity: 0,
            alarm_status: 0,
            alarm_message: String::new(),
            alarm_low: None,
            alarm_high: None,
            alarm_lolo: None,
            alarm_hihi: None,
            display_low: 0.0,
            display_high: 0.0,
            display_description: String::new(),
            display_precision: 0,
            display_form_index: 0,
            display_form_choices: default_form_choices(),
            control_low: 0.0,
            control_high: 0.0,
            control_min_step: 0.0,
            units: String::new(),
            value_alarm_active: false,
            value_alarm_low_alarm_limit: 0.0,
            value_alarm_low_warning_limit: 0.0,
            value_alarm_high_warning_limit: 0.0,
            value_alarm_high_alarm_limit: 0.0,
            value_alarm_low_alarm_severity: 0,
            value_alarm_low_warning_severity: 0,
            value_alarm_high_warning_severity: 0,
            value_alarm_high_alarm_severity: 0,
            value_alarm_hysteresis: 0,
        }
    }

    pub fn with_limits(mut self, low: f64, high: f64) -> Self {
        self.display_low = low;
        self.display_high = high;
        self.control_low = low;
        self.control_high = high;
        self
    }

    pub fn with_units(mut self, units: String) -> Self {
        self.units = units;
        self
    }

    pub fn with_description(mut self, description: String) -> Self {
        self.display_description = description;
        self
    }

    pub fn with_precision(mut self, precision: i32) -> Self {
        self.display_precision = precision;
        self
    }

    pub fn with_alarm_limits(
        mut self,
        low: Option<f64>,
        high: Option<f64>,
        lolo: Option<f64>,
        hihi: Option<f64>,
    ) -> Self {
        self.alarm_low = low;
        self.alarm_high = high;
        self.alarm_lolo = lolo;
        self.alarm_hihi = hihi;
        if let Some(v) = low {
            self.value_alarm_low_warning_limit = v;
        }
        if let Some(v) = high {
            self.value_alarm_high_warning_limit = v;
        }
        if let Some(v) = lolo {
            self.value_alarm_low_alarm_limit = v;
        }
        if let Some(v) = hihi {
            self.value_alarm_high_alarm_limit = v;
        }
        self
    }

    pub fn update_alarm_from_value(&mut self) {
        let val = match self.value {
            ScalarValue::F64(v) => v,
            ScalarValue::F32(v) => v as f64,
            ScalarValue::I8(v) => v as f64,
            ScalarValue::I16(v) => v as f64,
            ScalarValue::I32(v) => v as f64,
            ScalarValue::I64(v) => v as f64,
            ScalarValue::U8(v) => v as f64,
            ScalarValue::U16(v) => v as f64,
            ScalarValue::U32(v) => v as f64,
            ScalarValue::U64(v) => v as f64,
            _ => {
                self.alarm_severity = 0;
                self.alarm_status = 0;
                self.alarm_message.clear();
                return;
            }
        };

        let mut severity = 0;
        let mut message = String::new();

        if let Some(hihi) = self.alarm_hihi {
            if val >= hihi {
                severity = 2;
                message = "HIHI".to_string();
            }
        }
        if severity == 0 {
            if let Some(high) = self.alarm_high {
                if val >= high {
                    severity = 1;
                    message = "HIGH".to_string();
                }
            }
        }
        if severity == 0 {
            if let Some(lolo) = self.alarm_lolo {
                if val <= lolo {
                    severity = 2;
                    message = "LOLO".to_string();
                }
            }
        }
        if severity == 0 {
            if let Some(low) = self.alarm_low {
                if val <= low {
                    severity = 1;
                    message = "LOW".to_string();
                }
            }
        }

        if severity == 0 {
            self.alarm_severity = 0;
            self.alarm_status = 0;
            self.alarm_message.clear();
        } else {
            self.alarm_severity = severity;
            self.alarm_status = 1;
            self.alarm_message = message;
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NtScalarArray {
    pub value: ScalarArrayValue,
    pub alarm: NtAlarm,
    pub time_stamp: NtTimeStamp,
    pub display: NtDisplay,
    pub control: NtControl,
}

impl NtScalarArray {
    pub fn from_value(value: ScalarArrayValue) -> Self {
        Self {
            value,
            alarm: NtAlarm::default(),
            time_stamp: NtTimeStamp::default(),
            display: NtDisplay::default(),
            control: NtControl::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NtTableColumn {
    pub name: String,
    pub values: ScalarArrayValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NtTable {
    pub labels: Vec<String>,
    pub columns: Vec<NtTableColumn>,
    pub descriptor: Option<String>,
    pub alarm: Option<NtAlarm>,
    pub time_stamp: Option<NtTimeStamp>,
}

impl NtTable {
    pub fn validate(&self) -> Result<(), String> {
        let mut expected_len: Option<usize> = None;
        for col in &self.columns {
            let len = col.values.len();
            if let Some(expected) = expected_len {
                if expected != len {
                    return Err(format!(
                        "table column '{}' length {} does not match expected {}",
                        col.name, len, expected
                    ));
                }
            } else {
                expected_len = Some(len);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NdCodec {
    pub name: String,
    pub parameters: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NdDimension {
    pub size: i32,
    pub offset: i32,
    pub full_size: i32,
    pub binning: i32,
    pub reverse: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NtAttribute {
    pub name: String,
    pub value: ScalarValue,
    pub descriptor: String,
    pub source_type: i32,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NtNdArray {
    pub value: ScalarArrayValue,
    pub codec: NdCodec,
    pub compressed_size: i64,
    pub uncompressed_size: i64,
    pub dimension: Vec<NdDimension>,
    pub unique_id: i32,
    pub data_time_stamp: NtTimeStamp,
    pub attribute: Vec<NtAttribute>,
    pub descriptor: Option<String>,
    pub alarm: Option<NtAlarm>,
    pub time_stamp: Option<NtTimeStamp>,
    pub display: Option<NtDisplay>,
}

impl NtNdArray {
    pub fn validate(&self) -> Result<(), String> {
        if self
            .attribute
            .iter()
            .any(|a| a.descriptor.trim().is_empty())
        {
            return Err("ntndarray attribute descriptor must be set".to_string());
        }
        let element_size = self.value.element_size_bytes().max(1) as i64;
        let logical_elements = self
            .dimension
            .iter()
            .map(|d| d.size.max(0) as i64)
            .product::<i64>()
            .max(0);
        let expected_uncompressed = logical_elements.saturating_mul(element_size);
        if self.uncompressed_size > 0 && self.uncompressed_size != expected_uncompressed {
            return Err(format!(
                "uncompressed_size {} does not match dimension*element_size {}",
                self.uncompressed_size, expected_uncompressed
            ));
        }
        if self.compressed_size > 0 && self.compressed_size > self.uncompressed_size {
            return Err(format!(
                "compressed_size {} cannot exceed uncompressed_size {}",
                self.compressed_size, self.uncompressed_size
            ));
        }
        Ok(())
    }
}

/// Top-level Normative Type payload returned by [`crate`] and consumed by
/// the spvirit codec / server.
///
/// `#[non_exhaustive]` means callers must include a wildcard arm in
/// `match` expressions; this lets new variants be added in future minor
/// releases without breaking downstream code. Adding the [`NtPayload::Structure`]
/// variant in 0.2.0 was already a breaking change for code that did not
/// have a wildcard arm.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum NtPayload {
    Scalar(NtScalar),
    ScalarArray(NtScalarArray),
    Table(NtTable),
    NdArray(NtNdArray),
    /// Generic nested structure — used for QSRV group PVs and other
    /// payloads whose shape does not match a canonical Normative Type.
    /// The contained [`NtStructure`] supports nested scalars, scalar
    /// arrays, and nested structures (no unions, struct-arrays, or
    /// variants — see [`NtStructure`] for details).
    Structure(NtStructure),
}

/// A nested PV value built out of scalars, scalar arrays and (recursively)
/// other [`NtStructure`]s. Used to express PV payloads that don't fit a
/// canonical Normative Type variant — primarily QSRV group PVs.
///
/// **Expressiveness limits.** Each field is an [`NtField`], which today only
/// carries `Scalar`, `ScalarArray`, or nested `Structure`. The richer
/// pvData kinds — `union`, `union[]`, `structure[]`, `variant`,
/// `variant[]`, `bounded_string` — are intentionally *not* supported.
/// They are rare in PVA data (QSRV groups never produce them) and adding
/// them would require codec, server-side put-routing, and PvStore changes
/// that no current consumer asks for. If a use case appears, prefer
/// extending [`NtField`] in a follow-up release rather than working
/// around it on the call site.
///
/// `struct_id` is the PVA `structure ID` string sent on the wire (e.g.
/// `"epics:nt/NTScalar:1.0"` for canonical NT shapes, an arbitrary
/// label like `"my:group/v1"` for QSRV groups, or `None` for an
/// anonymous nested structure).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NtStructure {
    pub struct_id: Option<String>,
    pub fields: Vec<(String, NtField)>,
}

impl NtStructure {
    pub fn new(struct_id: impl Into<String>) -> Self {
        Self {
            struct_id: Some(struct_id.into()),
            fields: Vec::new(),
        }
    }

    pub fn anonymous() -> Self {
        Self {
            struct_id: None,
            fields: Vec::new(),
        }
    }

    pub fn push(&mut self, name: impl Into<String>, value: NtField) {
        self.fields.push((name.into(), value));
    }

    pub fn field(&self, name: &str) -> Option<&NtField> {
        self.fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }
}

/// One field inside a generic [`NtStructure`].
///
/// Limited to scalars, scalar arrays, and nested structures — see the
/// expressiveness note on [`NtStructure`] for what is intentionally
/// excluded.
#[derive(Debug, Clone, PartialEq)]
pub enum NtField {
    Scalar(ScalarValue),
    ScalarArray(ScalarArrayValue),
    Structure(NtStructure),
}

pub(crate) fn default_form_choices() -> Vec<String> {
    vec![
        "Default".to_string(),
        "String".to_string(),
        "Binary".to_string(),
        "Decimal".to_string(),
        "Hex".to_string(),
        "Exponential".to_string(),
        "Engineering".to_string(),
    ]
}

#[cfg(test)]
mod nt_structure_tests {
    use super::*;

    #[test]
    fn nt_structure_build_and_lookup() {
        let mut nested = NtStructure::new("alarm_t");
        nested.push("severity", NtField::Scalar(ScalarValue::I32(2)));
        nested.push("message", NtField::Scalar(ScalarValue::Str("HIHI".into())));

        let mut outer = NtStructure::new("my:group/v1");
        outer.push("value", NtField::Scalar(ScalarValue::F64(1.5)));
        outer.push("alarm", NtField::Structure(nested));

        match outer.field("value") {
            Some(NtField::Scalar(ScalarValue::F64(v))) => assert_eq!(*v, 1.5),
            _ => panic!("expected scalar value"),
        }
        match outer.field("alarm") {
            Some(NtField::Structure(s)) => {
                assert_eq!(s.struct_id.as_deref(), Some("alarm_t"));
                assert_eq!(s.fields.len(), 2);
            }
            _ => panic!("expected nested structure"),
        }
    }

    #[test]
    fn nt_payload_structure_variant() {
        let mut s = NtStructure::new("test:group/1");
        s.push("x", NtField::Scalar(ScalarValue::I32(42)));
        let payload = NtPayload::Structure(s);
        match payload {
            NtPayload::Structure(s) => assert_eq!(s.fields.len(), 1),
            _ => panic!("expected Structure variant"),
        }
    }

    #[test]
    fn nt_structure_anonymous_has_no_id() {
        let s = NtStructure::anonymous();
        assert!(s.struct_id.is_none());
        assert!(s.fields.is_empty());
    }

    #[test]
    fn nt_field_nested_arrays() {
        let mut s = NtStructure::anonymous();
        s.push(
            "xs",
            NtField::ScalarArray(ScalarArrayValue::F64(vec![1.0, 2.0, 3.0])),
        );
        match s.field("xs") {
            Some(NtField::ScalarArray(ScalarArrayValue::F64(v))) => {
                assert_eq!(v.len(), 3);
            }
            _ => panic!("expected array"),
        }
    }
}
