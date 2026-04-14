//! spvirit — Python bindings for PVAccess client and server.

use pyo3::prelude::*;

mod convert;
mod errors;
mod runtime;

pub mod client;
pub mod nt;
pub mod server;

#[pymodule]
fn spvirit(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Error types
    errors::register(m)?;

    // Client classes
    m.add_class::<client::PyClient>()?;
    m.add_class::<client::PyClientBuilder>()?;
    m.add_class::<client::PyGetResult>()?;
    m.add_class::<client::PyMonitorEvent>()?;
    m.add_class::<client::PyDiscoveredServer>()?;

    // Server classes
    m.add_class::<server::PyServerBuilder>()?;
    m.add_class::<server::PyServer>()?;
    m.add_class::<server::PyStore>()?;

    // NT classes
    m.add_class::<nt::PyAlarm>()?;
    m.add_class::<nt::PyTimeStamp>()?;
    m.add_class::<nt::PyDisplay>()?;
    m.add_class::<nt::PyControl>()?;
    m.add_class::<nt::PyNtScalar>()?;
    m.add_class::<nt::PyNtScalarArray>()?;
    m.add_class::<nt::PyNtTable>()?;
    m.add_class::<nt::PyNtNdArray>()?;

    // Module-level functions
    m.add_function(wrap_pyfunction!(client::py_discover_servers, m)?)?;

    Ok(())
}
