//! Server-specific record and IOC types.
//!
//! Shared Normative Type definitions (ScalarValue, NtScalar, NtPayload, etc.)
//! live in the `pva-types` crate and are re-exported here for convenience.

use std::collections::HashMap;
use std::time::Duration;

pub use pva_types::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordType {
    Ai,
    Ao,
    Bi,
    Bo,
    StringIn,
    StringOut,
    Waveform,
    Aai,
    Aao,
    SubArray,
    NtTable,
    NtNdArray,
}

impl RecordType {
    pub fn from_db_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "ai" => Some(Self::Ai),
            "ao" => Some(Self::Ao),
            "bi" => Some(Self::Bi),
            "bo" => Some(Self::Bo),
            "stringin" => Some(Self::StringIn),
            "stringout" => Some(Self::StringOut),
            "waveform" => Some(Self::Waveform),
            "aai" => Some(Self::Aai),
            "aao" => Some(Self::Aao),
            "subarray" => Some(Self::SubArray),
            _ => None,
        }
    }

    pub fn is_output(&self) -> bool {
        matches!(self, Self::Ao | Self::Bo | Self::StringOut | Self::Aao)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanMode {
    Passive,
    Periodic(Duration),
    Event(String),
    IoEvent(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LinkExpr {
    Constant(ScalarValue),
    DbLink {
        target: String,
        process_passive: bool,
        maximize_severity: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputMode {
    Supervisory,
    ClosedLoop,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DbCommonState {
    pub desc: String,
    pub scan: ScanMode,
    pub pini: bool,
    pub phas: i32,
    pub pact: bool,
    pub disa: bool,
    pub sdis: Option<LinkExpr>,
    pub diss: i32,
    pub flnk: Option<LinkExpr>,
}

impl Default for DbCommonState {
    fn default() -> Self {
        Self {
            desc: String::new(),
            scan: ScanMode::Passive,
            pini: false,
            phas: 0,
            pact: false,
            disa: false,
            sdis: None,
            diss: 0,
            flnk: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RecordData {
    Ai {
        nt: NtScalar,
        inp: Option<LinkExpr>,
        siml: Option<LinkExpr>,
        siol: Option<LinkExpr>,
        simm: bool,
    },
    Ao {
        nt: NtScalar,
        out: Option<LinkExpr>,
        dol: Option<LinkExpr>,
        omsl: OutputMode,
        drvl: Option<f64>,
        drvh: Option<f64>,
        oroc: Option<f64>,
        siml: Option<LinkExpr>,
        siol: Option<LinkExpr>,
        simm: bool,
    },
    Bi {
        nt: NtScalar,
        inp: Option<LinkExpr>,
        znam: String,
        onam: String,
        siml: Option<LinkExpr>,
        siol: Option<LinkExpr>,
        simm: bool,
    },
    Bo {
        nt: NtScalar,
        out: Option<LinkExpr>,
        dol: Option<LinkExpr>,
        omsl: OutputMode,
        znam: String,
        onam: String,
        siml: Option<LinkExpr>,
        siol: Option<LinkExpr>,
        simm: bool,
    },
    StringIn {
        nt: NtScalar,
        inp: Option<LinkExpr>,
        siml: Option<LinkExpr>,
        siol: Option<LinkExpr>,
        simm: bool,
    },
    StringOut {
        nt: NtScalar,
        out: Option<LinkExpr>,
        dol: Option<LinkExpr>,
        omsl: OutputMode,
        siml: Option<LinkExpr>,
        siol: Option<LinkExpr>,
        simm: bool,
    },
    Waveform {
        nt: NtScalarArray,
        inp: Option<LinkExpr>,
        ftvl: String,
        nelm: usize,
        nord: usize,
    },
    Aai {
        nt: NtScalarArray,
        inp: Option<LinkExpr>,
        ftvl: String,
        nelm: usize,
        nord: usize,
    },
    Aao {
        nt: NtScalarArray,
        out: Option<LinkExpr>,
        dol: Option<LinkExpr>,
        omsl: OutputMode,
        ftvl: String,
        nelm: usize,
        nord: usize,
    },
    SubArray {
        nt: NtScalarArray,
        inp: Option<LinkExpr>,
        ftvl: String,
        malm: usize,
        nelm: usize,
        nord: usize,
        indx: usize,
    },
    NtTable {
        nt: NtTable,
        inp: Option<LinkExpr>,
        out: Option<LinkExpr>,
        omsl: OutputMode,
    },
    NtNdArray {
        nt: NtNdArray,
        inp: Option<LinkExpr>,
        out: Option<LinkExpr>,
        omsl: OutputMode,
    },
}

impl RecordData {
    pub fn nt(&self) -> &NtScalar {
        match self {
            Self::Ai { nt, .. } => nt,
            Self::Ao { nt, .. } => nt,
            Self::Bi { nt, .. } => nt,
            Self::Bo { nt, .. } => nt,
            Self::StringIn { nt, .. } => nt,
            Self::StringOut { nt, .. } => nt,
            _ => panic!("record variant does not expose NtScalar"),
        }
    }

    pub fn nt_mut(&mut self) -> &mut NtScalar {
        match self {
            Self::Ai { nt, .. } => nt,
            Self::Ao { nt, .. } => nt,
            Self::Bi { nt, .. } => nt,
            Self::Bo { nt, .. } => nt,
            Self::StringIn { nt, .. } => nt,
            Self::StringOut { nt, .. } => nt,
            _ => panic!("record variant does not expose NtScalar"),
        }
    }

    pub fn payload(&self) -> NtPayload {
        match self {
            Self::Ai { nt, .. }
            | Self::Ao { nt, .. }
            | Self::Bi { nt, .. }
            | Self::Bo { nt, .. }
            | Self::StringIn { nt, .. }
            | Self::StringOut { nt, .. } => NtPayload::Scalar(nt.clone()),
            Self::Waveform { nt, .. }
            | Self::Aai { nt, .. }
            | Self::Aao { nt, .. }
            | Self::SubArray { nt, .. } => NtPayload::ScalarArray(nt.clone()),
            Self::NtTable { nt, .. } => NtPayload::Table(nt.clone()),
            Self::NtNdArray { nt, .. } => NtPayload::NdArray(nt.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordInstance {
    pub name: String,
    pub record_type: RecordType,
    pub common: DbCommonState,
    pub data: RecordData,
    pub raw_fields: HashMap<String, String>,
}

impl RecordInstance {
    pub fn writable(&self) -> bool {
        if self.record_type.is_output() {
            return true;
        }
        match &self.data {
            RecordData::Ai { simm: true, .. } => true,
            RecordData::Waveform { .. } => true,
            RecordData::NtTable { .. } => true,
            RecordData::NtNdArray { .. } => true,
            _ => false,
        }
    }

    pub fn to_ntpayload(&self) -> NtPayload {
        self.data.payload()
    }

    pub fn to_ntscalar(&self) -> NtScalar {
        match self.to_ntpayload() {
            NtPayload::Scalar(nt) => nt,
            NtPayload::ScalarArray(nt) => {
                let mut scalar = NtScalar::from_value(ScalarValue::I32(nt.value.len() as i32));
                scalar.display_description = "Array length".to_string();
                scalar
            }
            NtPayload::Table(nt) => {
                let mut scalar = NtScalar::from_value(ScalarValue::I32(nt.columns.len() as i32));
                scalar.display_description = "Table columns".to_string();
                scalar
            }
            NtPayload::NdArray(nt) => {
                let mut scalar = NtScalar::from_value(ScalarValue::I32(nt.dimension.len() as i32));
                scalar.display_description = "NDArray dimensions".to_string();
                scalar
            }
        }
    }

    pub fn nt_mut(&mut self) -> &mut NtScalar {
        self.data.nt_mut()
    }

    pub fn current_value(&self) -> ScalarValue {
        match self.to_ntpayload() {
            NtPayload::Scalar(nt) => nt.value,
            NtPayload::ScalarArray(nt) => ScalarValue::I32(nt.value.len() as i32),
            NtPayload::Table(nt) => ScalarValue::I32(nt.columns.len() as i32),
            NtPayload::NdArray(nt) => ScalarValue::I32(nt.dimension.len() as i32),
        }
    }

    pub fn set_scalar_value(&mut self, value: ScalarValue, compute_alarms: bool) -> bool {
        let nt = match &mut self.data {
            RecordData::Ai { nt, .. }
            | RecordData::Ao { nt, .. }
            | RecordData::Bi { nt, .. }
            | RecordData::Bo { nt, .. }
            | RecordData::StringIn { nt, .. }
            | RecordData::StringOut { nt, .. } => nt,
            _ => return false,
        };

        let changed = match (&mut nt.value, value) {
            (ScalarValue::Bool(current), ScalarValue::Bool(v)) => {
                if *current == v {
                    false
                } else {
                    *current = v;
                    true
                }
            }
            (ScalarValue::I32(current), ScalarValue::I32(v)) => {
                if *current == v {
                    false
                } else {
                    *current = v;
                    true
                }
            }
            (ScalarValue::F64(current), ScalarValue::F64(v)) => {
                if (*current - v).abs() < f64::EPSILON {
                    false
                } else {
                    *current = v;
                    true
                }
            }
            (ScalarValue::Str(current), ScalarValue::Str(v)) => {
                if *current == v {
                    false
                } else {
                    *current = v;
                    true
                }
            }
            (ScalarValue::Bool(current), ScalarValue::I32(v)) => {
                let next = v != 0;
                if *current == next {
                    false
                } else {
                    *current = next;
                    true
                }
            }
            (ScalarValue::Bool(current), ScalarValue::F64(v)) => {
                let next = v != 0.0;
                if *current == next {
                    false
                } else {
                    *current = next;
                    true
                }
            }
            (ScalarValue::I32(current), ScalarValue::Bool(v)) => {
                let next = if v { 1 } else { 0 };
                if *current == next {
                    false
                } else {
                    *current = next;
                    true
                }
            }
            (ScalarValue::I32(current), ScalarValue::F64(v)) => {
                let next = v as i32;
                if *current == next {
                    false
                } else {
                    *current = next;
                    true
                }
            }
            (ScalarValue::F64(current), ScalarValue::Bool(v)) => {
                let next = if v { 1.0 } else { 0.0 };
                if (*current - next).abs() < f64::EPSILON {
                    false
                } else {
                    *current = next;
                    true
                }
            }
            (ScalarValue::F64(current), ScalarValue::I32(v)) => {
                let next = v as f64;
                if (*current - next).abs() < f64::EPSILON {
                    false
                } else {
                    *current = next;
                    true
                }
            }
            (ScalarValue::Str(current), ScalarValue::Bool(v)) => {
                let next = if v { "1" } else { "0" }.to_string();
                if *current == next {
                    false
                } else {
                    *current = next;
                    true
                }
            }
            (ScalarValue::Str(current), ScalarValue::I32(v)) => {
                let next = v.to_string();
                if *current == next {
                    false
                } else {
                    *current = next;
                    true
                }
            }
            (ScalarValue::Str(current), ScalarValue::F64(v)) => {
                let next = v.to_string();
                if *current == next {
                    false
                } else {
                    *current = next;
                    true
                }
            }
            (ScalarValue::Bool(current), ScalarValue::Str(v)) => {
                let next = parse_bool_like(&v).unwrap_or(*current);
                if *current == next {
                    false
                } else {
                    *current = next;
                    true
                }
            }
            (ScalarValue::I32(current), ScalarValue::Str(v)) => {
                let next = v.parse::<i32>().unwrap_or(*current);
                if *current == next {
                    false
                } else {
                    *current = next;
                    true
                }
            }
            (ScalarValue::F64(current), ScalarValue::Str(v)) => {
                let next = v.parse::<f64>().unwrap_or(*current);
                if (*current - next).abs() < f64::EPSILON {
                    false
                } else {
                    *current = next;
                    true
                }
            }
        };
        if changed && compute_alarms {
            nt.update_alarm_from_value();
        }
        changed
    }
}

fn parse_bool_like(input: &str) -> Option<bool> {
    match input.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
