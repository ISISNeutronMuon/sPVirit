//! Python exception hierarchy mirroring `PvGetError`.
//!
//! ```text
//! SpviritError(Exception)
//! ├── TimeoutError
//! ├── SearchError
//! ├── ProtocolError
//! ├── DecodeError
//! └── IoError
//! ```

use pyo3::prelude::*;
use pyo3::create_exception;
use spvirit_client::PvGetError;

create_exception!(spvirit, SpviritError, pyo3::exceptions::PyException);
create_exception!(spvirit, TimeoutError, SpviritError);
create_exception!(spvirit, SearchError, SpviritError);
create_exception!(spvirit, ProtocolError, SpviritError);
create_exception!(spvirit, DecodeError, SpviritError);
create_exception!(spvirit, IoError, SpviritError);

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("SpviritError", m.py().get_type::<SpviritError>())?;
    m.add("TimeoutError", m.py().get_type::<TimeoutError>())?;
    m.add("SearchError", m.py().get_type::<SearchError>())?;
    m.add("ProtocolError", m.py().get_type::<ProtocolError>())?;
    m.add("DecodeError", m.py().get_type::<DecodeError>())?;
    m.add("IoError", m.py().get_type::<IoError>())?;
    Ok(())
}

/// Convert a `PvGetError` into the matching Python exception.
pub fn to_py_err(e: PvGetError) -> PyErr {
    match e {
        PvGetError::Io(e) => IoError::new_err(e.to_string()),
        PvGetError::Timeout(ctx) => TimeoutError::new_err(ctx.to_string()),
        PvGetError::Search(ctx) => SearchError::new_err(ctx.to_string()),
        PvGetError::Protocol(ctx) => ProtocolError::new_err(ctx),
        PvGetError::Decode(ctx) => DecodeError::new_err(ctx),
    }
}
