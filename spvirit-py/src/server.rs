//! Python server wrappers — sync-only for phase 1.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;

use spvirit_codec::spvd_decode::DecodedValue;
use spvirit_server::pva_server::PvaServer;
use spvirit_server::SimplePvStore;
use spvirit_types::{ScalarArrayValue, ScalarValue};

use crate::convert::{decoded_to_py, py_to_scalar, py_to_scalar_array, scalar_to_py};
use crate::nt::{nt_payload_to_py, py_to_nt_payload};
use crate::runtime::RUNTIME;

// ─── ServerBuilder ───────────────────────────────────────────────────────────

#[pyclass(name = "ServerBuilder")]
pub struct PyServerBuilder {
    builder: Option<spvirit_server::PvaServerBuilder>,
}

#[pymethods]
impl PyServerBuilder {
    #[new]
    fn new() -> Self {
        Self {
            builder: Some(PvaServer::builder()),
        }
    }

    fn ai(mut slf: PyRefMut<'_, Self>, name: String, initial: f64) -> PyRefMut<'_, Self> {
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.ai(name, initial));
        slf
    }

    fn ao(mut slf: PyRefMut<'_, Self>, name: String, initial: f64) -> PyRefMut<'_, Self> {
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.ao(name, initial));
        slf
    }

    fn bi(mut slf: PyRefMut<'_, Self>, name: String, initial: bool) -> PyRefMut<'_, Self> {
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.bi(name, initial));
        slf
    }

    fn bo(mut slf: PyRefMut<'_, Self>, name: String, initial: bool) -> PyRefMut<'_, Self> {
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.bo(name, initial));
        slf
    }

    fn string_in(mut slf: PyRefMut<'_, Self>, name: String, initial: String) -> PyRefMut<'_, Self> {
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.string_in(name, initial));
        slf
    }

    fn string_out(mut slf: PyRefMut<'_, Self>, name: String, initial: String) -> PyRefMut<'_, Self> {
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.string_out(name, initial));
        slf
    }

    fn waveform<'py>(mut slf: PyRefMut<'py, Self>, name: String, data: &Bound<'py, PyAny>) -> PyResult<PyRefMut<'py, Self>> {
        let arr = py_to_scalar_array(data)?;
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.waveform(name, arr));
        Ok(slf)
    }

    fn aai<'py>(mut slf: PyRefMut<'py, Self>, name: String, data: &Bound<'py, PyAny>) -> PyResult<PyRefMut<'py, Self>> {
        let arr = py_to_scalar_array(data)?;
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.aai(name, arr));
        Ok(slf)
    }

    fn aao<'py>(mut slf: PyRefMut<'py, Self>, name: String, data: &Bound<'py, PyAny>) -> PyResult<PyRefMut<'py, Self>> {
        let arr = py_to_scalar_array(data)?;
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.aao(name, arr));
        Ok(slf)
    }

    #[pyo3(signature = (name, data, indx=0, nelm=None))]
    fn sub_array<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
        data: &Bound<'py, PyAny>,
        indx: usize,
        nelm: Option<usize>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let arr = py_to_scalar_array(data)?;
        let n = nelm.unwrap_or(arr.len());
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.sub_array(name, arr, indx, n));
        Ok(slf)
    }

    fn nt_table<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
        columns: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let dict = columns.downcast::<pyo3::types::PyDict>().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err("columns must be a dict of {name: list}")
        })?;
        let mut cols: Vec<(String, ScalarArrayValue)> = Vec::new();
        for (key, val) in dict.iter() {
            let col_name: String = key.extract()?;
            let col_data = py_to_scalar_array(&val)?;
            cols.push((col_name, col_data));
        }
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.nt_table(name, cols));
        Ok(slf)
    }

    fn nt_ndarray<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
        data: &Bound<'py, PyAny>,
        dims: Vec<(i32, i32)>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let arr = py_to_scalar_array(data)?;
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.nt_ndarray(name, arr, dims));
        Ok(slf)
    }

    fn db_file(mut slf: PyRefMut<'_, Self>, path: String) -> PyRefMut<'_, Self> {
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.db_file(path));
        slf
    }

    fn db_string(mut slf: PyRefMut<'_, Self>, content: String) -> PyRefMut<'_, Self> {
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.db_string(&content));
        slf
    }

    fn on_put(mut slf: PyRefMut<'_, Self>, name: String, callback: PyObject) -> PyRefMut<'_, Self> {
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.on_put(name, move |pv_name: &str, decoded: &DecodedValue| {
            Python::with_gil(|py| {
                let py_val = decoded_to_py(py, decoded);
                if let Err(e) = callback.call1(py, (pv_name, py_val)) {
                    tracing::error!("on_put callback error: {e}");
                }
            });
        }));
        slf
    }

    fn scan(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        period_secs: f64,
        callback: PyObject,
    ) -> PyRefMut<'_, Self> {
        let b = slf.builder.take().expect("builder consumed");
        let dur = Duration::from_secs_f64(period_secs);
        slf.builder = Some(b.scan(name, dur, move |pv_name: &str| {
            Python::with_gil(|py| {
                match callback.call1(py, (pv_name,)) {
                    Ok(ret) => {
                        py_to_scalar(ret.bind(py)).unwrap_or(ScalarValue::F64(0.0))
                    }
                    Err(e) => {
                        tracing::error!("scan callback error: {e}");
                        ScalarValue::F64(0.0)
                    }
                }
            })
        }));
        slf
    }

    fn port(mut slf: PyRefMut<'_, Self>, port: u16) -> PyRefMut<'_, Self> {
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.port(port));
        slf
    }

    fn udp_port(mut slf: PyRefMut<'_, Self>, port: u16) -> PyRefMut<'_, Self> {
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.udp_port(port));
        slf
    }

    fn listen_ip(mut slf: PyRefMut<'_, Self>, ip: String) -> PyResult<PyRefMut<'_, Self>> {
        let ip_addr: IpAddr = ip
            .parse()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("invalid IP: {e}")))?;
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.listen_ip(ip_addr));
        Ok(slf)
    }

    fn advertise_ip(mut slf: PyRefMut<'_, Self>, ip: String) -> PyResult<PyRefMut<'_, Self>> {
        let ip_addr: IpAddr = ip
            .parse()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("invalid IP: {e}")))?;
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.advertise_ip(ip_addr));
        Ok(slf)
    }

    fn compute_alarms(mut slf: PyRefMut<'_, Self>, enabled: bool) -> PyRefMut<'_, Self> {
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.compute_alarms(enabled));
        slf
    }

    fn beacon_period(mut slf: PyRefMut<'_, Self>, secs: u64) -> PyRefMut<'_, Self> {
        let b = slf.builder.take().expect("builder consumed");
        slf.builder = Some(b.beacon_period(secs));
        slf
    }

    /// Build and return a `Server` that can be started.
    fn build(&mut self) -> PyResult<PyServer> {
        let b = self
            .builder
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("builder already consumed"))?;
        let server = b.build();
        let store = server.store().clone();
        Ok(PyServer {
            server: Some(server),
            store: Some(store),
        })
    }
}

// ─── Server ──────────────────────────────────────────────────────────────────

#[pyclass(name = "Server")]
pub struct PyServer {
    server: Option<PvaServer>,
    store: Option<Arc<SimplePvStore>>,
}

#[pymethods]
impl PyServer {
    /// Get a handle to the PV store for runtime get/set.
    fn store(&self) -> PyResult<PyStore> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("server already consumed"))?
            .clone();
        Ok(PyStore { inner: store })
    }

    /// Run the server (blocking). This does not return until the server stops.
    fn run(&mut self, py: Python<'_>) -> PyResult<()> {
        let server = self
            .server
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("server already consumed"))?;
        py.allow_threads(|| {
            RUNTIME
                .block_on(server.run())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Start the server in a background thread and return the store handle.
    fn start_background(&mut self) -> PyResult<PyStore> {
        let server = self
            .server
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("server already consumed"))?;
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("server already consumed"))?
            .clone();

        std::thread::spawn(move || {
            if let Err(e) = RUNTIME.block_on(server.run()) {
                tracing::error!("background server error: {e}");
            }
        });

        Ok(PyStore { inner: store })
    }
}

// ─── Store ───────────────────────────────────────────────────────────────────

#[pyclass(name = "Store")]
pub struct PyStore {
    inner: Arc<SimplePvStore>,
}

#[pymethods]
impl PyStore {
    /// Get the current scalar value of a PV (returns None if not found).
    fn get_value(&self, py: Python<'_>, name: String) -> PyResult<PyObject> {
        let store = self.inner.clone();
        let val = py.allow_threads(|| RUNTIME.block_on(store.get_value(&name)));
        Ok(match val {
            Some(v) => scalar_to_py(py, &v),
            None => py.None(),
        })
    }

    /// Get the full NT payload for a PV (returns NtScalar, NtScalarArray, etc.).
    fn get_nt(&self, py: Python<'_>, name: String) -> PyResult<PyObject> {
        let store = self.inner.clone();
        let val = py.allow_threads(|| RUNTIME.block_on(store.get_nt(&name)));
        Ok(match val {
            Some(payload) => nt_payload_to_py(py, payload),
            None => py.None(),
        })
    }

    /// Set a scalar value on a PV. Returns True if the PV exists.
    fn set_value(&self, py: Python<'_>, name: String, value: &Bound<'_, PyAny>) -> PyResult<bool> {
        let sv = py_to_scalar(value)?;
        let store = self.inner.clone();
        Ok(py.allow_threads(|| RUNTIME.block_on(store.set_value(&name, sv))))
    }

    /// Set an array value on a PV. Returns True if the PV exists.
    fn set_array_value(
        &self,
        py: Python<'_>,
        name: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let arr = py_to_scalar_array(value)?;
        let store = self.inner.clone();
        Ok(py.allow_threads(|| RUNTIME.block_on(store.set_array_value(&name, arr))))
    }

    /// Write a full NT payload (NtScalar, NtScalarArray, etc.) to a PV.
    /// Returns True if the PV exists.
    fn put_nt(&self, py: Python<'_>, name: String, nt: &Bound<'_, PyAny>) -> PyResult<bool> {
        let payload = py_to_nt_payload(nt)?;
        let store = self.inner.clone();
        Ok(py.allow_threads(|| RUNTIME.block_on(store.put_nt(&name, payload))))
    }

    /// List all PV names in the store.
    fn pv_names(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let store = self.inner.clone();
        Ok(py.allow_threads(|| RUNTIME.block_on(store.pv_names())))
    }
}
