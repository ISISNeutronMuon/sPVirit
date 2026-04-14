//! The [`PvStore`] trait — an abstraction over any PV data source.
//!
//! Protocol handlers are generic over `PvStore`, allowing different backends
//! (spvirit `RecordInstance` store, epics-rs `PvDatabase`, etc.) to plug into
//! the same PVA server protocol machinery.

use spvirit_codec::spvd_decode::{DecodedValue, StructureDesc};
use spvirit_types::NtPayload;
use tokio::sync::mpsc;

/// Abstraction over a PV data store that the PVA server protocol handler calls
/// to resolve names, read values, write values, and subscribe to updates.
///
/// Implementors can be backed by an in-memory record database, the epics-rs
/// `PvDatabase`, or any other data source.
pub trait PvStore: Send + Sync + 'static {
    /// Check whether a PV name exists in this store.
    fn has_pv(&self, name: &str) -> impl Future<Output = bool> + Send;

    /// Get the current value of a PV as an `NtPayload` snapshot.
    fn get_snapshot(&self, name: &str) -> impl Future<Output = Option<NtPayload>> + Send;

    /// Get the structure descriptor for a PV (used by GET init and GET_FIELD).
    fn get_descriptor(&self, name: &str) -> impl Future<Output = Option<StructureDesc>> + Send;

    /// Apply a PUT value to a PV.  Returns the list of (pv_name, updated_payload)
    /// pairs for all PVs that changed as a result (e.g. forward-link processing
    /// may cause multiple PVs to update).
    fn put_value(
        &self,
        name: &str,
        value: &DecodedValue,
    ) -> impl Future<Output = Result<Vec<(String, NtPayload)>, String>> + Send;

    /// Check whether a PV is writable.
    fn is_writable(&self, name: &str) -> impl Future<Output = bool> + Send;

    /// List all PV names available in this store.
    fn list_pvs(&self) -> impl Future<Output = Vec<String>> + Send;

    /// Subscribe to value-change notifications on a PV.  Returns a receiver
    /// that yields `NtPayload` snapshots whenever the PV is updated.
    /// Returns `None` if the PV does not exist.
    fn subscribe(
        &self,
        name: &str,
    ) -> impl Future<Output = Option<mpsc::Receiver<NtPayload>>> + Send;
}
