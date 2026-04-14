//! Conversion between Rust PVAccess value types and Python objects.

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};
use serde_json::Value as JsonValue;
use spvirit_codec::spvd_decode::DecodedValue;
use spvirit_types::{ScalarArrayValue, ScalarValue};

// ─── DecodedValue → Python ───────────────────────────────────────────────────

pub fn decoded_to_py(py: Python<'_>, v: &DecodedValue) -> PyObject {
    match v {
        DecodedValue::Null => py.None(),
        DecodedValue::Boolean(b) => PyBool::new(py, *b).to_owned().into_any().unbind(),
        DecodedValue::Int8(n) => n.into_pyobject(py).expect("i8").into_any().unbind(),
        DecodedValue::Int16(n) => n.into_pyobject(py).expect("i16").into_any().unbind(),
        DecodedValue::Int32(n) => n.into_pyobject(py).expect("i32").into_any().unbind(),
        DecodedValue::Int64(n) => n.into_pyobject(py).expect("i64").into_any().unbind(),
        DecodedValue::UInt8(n) => n.into_pyobject(py).expect("u8").into_any().unbind(),
        DecodedValue::UInt16(n) => n.into_pyobject(py).expect("u16").into_any().unbind(),
        DecodedValue::UInt32(n) => n.into_pyobject(py).expect("u32").into_any().unbind(),
        DecodedValue::UInt64(n) => n.into_pyobject(py).expect("u64").into_any().unbind(),
        DecodedValue::Float32(f) => PyFloat::new(py, *f as f64).into_any().unbind(),
        DecodedValue::Float64(f) => PyFloat::new(py, *f).into_any().unbind(),
        DecodedValue::String(s) => PyString::new(py, s).into_any().unbind(),
        DecodedValue::Raw(data) => PyBytes::new(py, data).into_any().unbind(),
        DecodedValue::Array(arr) => {
            let items: Vec<PyObject> = arr.iter().map(|item| decoded_to_py(py, item)).collect();
            PyList::new(py, &items).expect("list").into_any().unbind()
        }
        DecodedValue::Structure(fields) => {
            let dict = PyDict::new(py);
            for (name, val) in fields {
                dict.set_item(name, decoded_to_py(py, val)).expect("dict set");
            }
            dict.into_any().unbind()
        }
    }
}

// ─── ScalarValue → Python ────────────────────────────────────────────────────

pub fn scalar_to_py(py: Python<'_>, v: &ScalarValue) -> PyObject {
    match v {
        ScalarValue::Bool(b) => PyBool::new(py, *b).to_owned().into_any().unbind(),
        ScalarValue::I8(n) => n.into_pyobject(py).expect("i8").into_any().unbind(),
        ScalarValue::I16(n) => n.into_pyobject(py).expect("i16").into_any().unbind(),
        ScalarValue::I32(n) => n.into_pyobject(py).expect("i32").into_any().unbind(),
        ScalarValue::I64(n) => n.into_pyobject(py).expect("i64").into_any().unbind(),
        ScalarValue::U8(n) => n.into_pyobject(py).expect("u8").into_any().unbind(),
        ScalarValue::U16(n) => n.into_pyobject(py).expect("u16").into_any().unbind(),
        ScalarValue::U32(n) => n.into_pyobject(py).expect("u32").into_any().unbind(),
        ScalarValue::U64(n) => n.into_pyobject(py).expect("u64").into_any().unbind(),
        ScalarValue::F32(f) => PyFloat::new(py, *f as f64).into_any().unbind(),
        ScalarValue::F64(f) => PyFloat::new(py, *f).into_any().unbind(),
        ScalarValue::Str(s) => PyString::new(py, s).into_any().unbind(),
    }
}

// ─── ScalarArrayValue → Python ───────────────────────────────────────────────

pub fn scalar_array_to_py(py: Python<'_>, v: &ScalarArrayValue) -> PyObject {
    match v {
        ScalarArrayValue::U8(a) => PyBytes::new(py, a).into_any().unbind(),
        ScalarArrayValue::Bool(a) => {
            let items: Vec<PyObject> = a.iter().map(|x| PyBool::new(py, *x).to_owned().into_any().unbind()).collect();
            PyList::new(py, &items).expect("list").into_any().unbind()
        }
        ScalarArrayValue::I8(a) => int_list_i8(py, a),
        ScalarArrayValue::I16(a) => int_list_i16(py, a),
        ScalarArrayValue::I32(a) => int_list_i32(py, a),
        ScalarArrayValue::I64(a) => int_list_i64(py, a),
        ScalarArrayValue::U16(a) => int_list_u16(py, a),
        ScalarArrayValue::U32(a) => int_list_u32(py, a),
        ScalarArrayValue::U64(a) => int_list_u64(py, a),
        ScalarArrayValue::F32(a) => {
            let items: Vec<PyObject> = a.iter().map(|x| PyFloat::new(py, *x as f64).into_any().unbind()).collect();
            PyList::new(py, &items).expect("list").into_any().unbind()
        }
        ScalarArrayValue::F64(a) => {
            let items: Vec<PyObject> = a.iter().map(|x| PyFloat::new(py, *x).into_any().unbind()).collect();
            PyList::new(py, &items).expect("list").into_any().unbind()
        }
        ScalarArrayValue::Str(a) => {
            let items: Vec<PyObject> = a.iter().map(|x| PyString::new(py, x).into_any().unbind()).collect();
            PyList::new(py, &items).expect("list").into_any().unbind()
        }
    }
}

fn int_list_i8(py: Python<'_>, a: &[i8]) -> PyObject {
    let items: Vec<PyObject> = a.iter().map(|x| x.into_pyobject(py).expect("i8").into_any().unbind()).collect();
    PyList::new(py, &items).expect("list").into_any().unbind()
}
fn int_list_i16(py: Python<'_>, a: &[i16]) -> PyObject {
    let items: Vec<PyObject> = a.iter().map(|x| x.into_pyobject(py).expect("i16").into_any().unbind()).collect();
    PyList::new(py, &items).expect("list").into_any().unbind()
}
fn int_list_i32(py: Python<'_>, a: &[i32]) -> PyObject {
    let items: Vec<PyObject> = a.iter().map(|x| x.into_pyobject(py).expect("i32").into_any().unbind()).collect();
    PyList::new(py, &items).expect("list").into_any().unbind()
}
fn int_list_i64(py: Python<'_>, a: &[i64]) -> PyObject {
    let items: Vec<PyObject> = a.iter().map(|x| x.into_pyobject(py).expect("i64").into_any().unbind()).collect();
    PyList::new(py, &items).expect("list").into_any().unbind()
}
fn int_list_u16(py: Python<'_>, a: &[u16]) -> PyObject {
    let items: Vec<PyObject> = a.iter().map(|x| x.into_pyobject(py).expect("u16").into_any().unbind()).collect();
    PyList::new(py, &items).expect("list").into_any().unbind()
}
fn int_list_u32(py: Python<'_>, a: &[u32]) -> PyObject {
    let items: Vec<PyObject> = a.iter().map(|x| x.into_pyobject(py).expect("u32").into_any().unbind()).collect();
    PyList::new(py, &items).expect("list").into_any().unbind()
}
fn int_list_u64(py: Python<'_>, a: &[u64]) -> PyObject {
    let items: Vec<PyObject> = a.iter().map(|x| x.into_pyobject(py).expect("u64").into_any().unbind()).collect();
    PyList::new(py, &items).expect("list").into_any().unbind()
}

// ─── Python → serde_json::Value ──────────────────────────────────────────────

pub fn py_to_json(obj: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    if obj.is_none() {
        Ok(JsonValue::Null)
    } else if let Ok(b) = obj.downcast::<PyBool>() {
        Ok(JsonValue::Bool(b.is_true()))
    } else if obj.is_instance_of::<PyInt>() {
        let v: i64 = obj.extract()?;
        Ok(JsonValue::Number(v.into()))
    } else if obj.is_instance_of::<PyFloat>() {
        let v: f64 = obj.extract()?;
        match serde_json::Number::from_f64(v) {
            Some(n) => Ok(JsonValue::Number(n)),
            None => Err(pyo3::exceptions::PyValueError::new_err("float is NaN or Inf")),
        }
    } else if obj.is_instance_of::<PyString>() {
        let v: String = obj.extract()?;
        Ok(JsonValue::String(v))
    } else if let Ok(list) = obj.downcast::<PyList>() {
        let mut arr = Vec::with_capacity(list.len());
        for item in list.iter() {
            arr.push(py_to_json(&item)?);
        }
        Ok(JsonValue::Array(arr))
    } else if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            map.insert(key, py_to_json(&v)?);
        }
        Ok(JsonValue::Object(map))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "cannot convert {} to PV value",
            obj.get_type().name()?
        )))
    }
}

// ─── Python → ScalarValue ────────────────────────────────────────────────────

pub fn py_to_scalar(obj: &Bound<'_, PyAny>) -> PyResult<ScalarValue> {
    if let Ok(b) = obj.downcast::<PyBool>() {
        Ok(ScalarValue::Bool(b.is_true()))
    } else if obj.is_instance_of::<PyInt>() {
        let v: i64 = obj.extract()?;
        Ok(ScalarValue::I64(v))
    } else if obj.is_instance_of::<PyFloat>() {
        let v: f64 = obj.extract()?;
        Ok(ScalarValue::F64(v))
    } else if obj.is_instance_of::<PyString>() {
        let v: String = obj.extract()?;
        Ok(ScalarValue::Str(v))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "cannot convert {} to ScalarValue",
            obj.get_type().name()?
        )))
    }
}

// ─── Python list → ScalarArrayValue ──────────────────────────────────────────

pub fn py_to_scalar_array(obj: &Bound<'_, PyAny>) -> PyResult<ScalarArrayValue> {
    if let Ok(bytes) = obj.downcast::<PyBytes>() {
        return Ok(ScalarArrayValue::U8(bytes.as_bytes().to_vec()));
    }
    let list = obj.downcast::<PyList>().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err("expected list or bytes for array value")
    })?;
    if list.is_empty() {
        return Ok(ScalarArrayValue::F64(Vec::new()));
    }
    let first = list.get_item(0)?;
    if first.downcast::<PyBool>().is_ok() {
        let v: Vec<bool> = list.extract()?;
        Ok(ScalarArrayValue::Bool(v))
    } else if first.is_instance_of::<PyInt>() {
        let v: Vec<i64> = list.extract()?;
        Ok(ScalarArrayValue::I64(v))
    } else if first.is_instance_of::<PyFloat>() {
        let v: Vec<f64> = list.extract()?;
        Ok(ScalarArrayValue::F64(v))
    } else if first.is_instance_of::<PyString>() {
        let v: Vec<String> = list.extract()?;
        Ok(ScalarArrayValue::Str(v))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(
            "array elements must be bool, int, float, or str",
        ))
    }
}
