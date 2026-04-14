//! Shared Tokio runtime for blocking (sync) wrappers.
//!
//! A single multi-thread runtime is lazily created and shared across the
//! entire Python process.  Async Python methods use `pyo3_async_runtimes`
//! while sync helpers call `RUNTIME.block_on(...)`.

use std::sync::LazyLock;
use tokio::runtime::Runtime;

pub static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime for spvirit-py")
});
