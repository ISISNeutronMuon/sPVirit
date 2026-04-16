//! The [`Source`] trait — an object-safe abstraction over any PV data source,
//! and [`SourceRegistry`] — a dynamic, priority-ordered collection of sources.
//!
//! Protocol handlers use `SourceRegistry` to resolve PV names across multiple
//! registered sources, allowing different backends (in-memory records, hardware
//! drivers, proxies, etc.) to coexist in a single PVA server. basically what pvxs does with its provider registry.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use spvirit_codec::spvd_decode::{DecodedValue, StructureDesc};
use spvirit_types::NtPayload;
use tokio::sync::{RwLock, mpsc};
use tracing::debug;

// ---------------------------------------------------------------------------
// PvInfo — metadata returned by Source::claim
// ---------------------------------------------------------------------------

/// Metadata about a PV as reported by the source that owns it.
#[derive(Debug, Clone)]
pub struct PvInfo {
    /// Structure descriptor for the PV.
    pub descriptor: StructureDesc,
    /// Whether the PV accepts PUT operations.
    pub writable: bool,
}

// ---------------------------------------------------------------------------
// Source — the object-safe provider trait
// ---------------------------------------------------------------------------

/// Object-safe trait for a PV data provider.
///
/// A source is responsible for a set of PV names. The server's
/// [`SourceRegistry`] iterates sources in priority order to find the first
/// that *claims* a given name.
///
/// # Implementing a custom source
///
/// ```rust,ignore
/// use spvirit_server::pvstore::{Source, PvInfo};
///
/// struct MySource { /* ... */ }
///
/// impl Source for MySource {
///     fn claim(&self, name: &str) -> Pin<Box<dyn Future<Output = Option<PvInfo>> + Send + '_>> {
///         Box::pin(async move { /* ... */ })
///     }
///     // ...other methods...
/// }
/// ```
pub trait Source: Send + Sync {
    /// Check whether this source owns `name` and, if so, return its metadata.
    ///
    /// Return `None` to let the registry try the next source.
    fn claim(&self, name: &str) -> Pin<Box<dyn Future<Output = Option<PvInfo>> + Send + '_>>;

    /// Read the current value of a PV.
    ///
    /// Only called for PVs this source has previously claimed.
    fn get(&self, name: &str) -> Pin<Box<dyn Future<Output = Option<NtPayload>> + Send + '_>>;

    /// Apply a PUT value to a PV.
    ///
    /// Returns the list of `(pv_name, updated_payload)` pairs for all PVs
    /// that changed as a result (e.g. forward-link propagation).
    fn put(
        &self,
        name: &str,
        value: &DecodedValue,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<(String, NtPayload)>, String>> + Send + '_>>;

    /// Subscribe to value-change notifications on a PV.
    ///
    /// Returns `None` if the PV does not support subscription.
    fn subscribe(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = Option<mpsc::Receiver<NtPayload>>> + Send + '_>>;

    /// Execute an RPC call on a channel.
    ///
    /// `name` is the channel name, `args` is the decoded request structure.
    /// Returns the response payload on success.
    ///
    /// The default implementation returns an error — override it in sources
    /// that provide RPC endpoints.
    fn rpc(
        &self,
        _name: &str,
        _args: &DecodedValue,
    ) -> Pin<Box<dyn Future<Output = Result<NtPayload, String>> + Send + '_>> {
        Box::pin(async { Err("RPC not supported".to_string()) })
    }

    /// List all PV names provided by this source.
    fn names(&self) -> Pin<Box<dyn Future<Output = Vec<String>> + Send + '_>>;
}

// ---------------------------------------------------------------------------
// SourceEntry — one registered source with its priority
// ---------------------------------------------------------------------------

struct SourceEntry {
    /// Human-readable label for debugging / logging.
    #[allow(dead_code)]
    label: String,
    /// Lower values are checked first.
    order: i32,
    /// The actual source implementation.
    source: Arc<dyn Source>,
}

// ---------------------------------------------------------------------------
// SourceRegistry — ordered collection of sources
// ---------------------------------------------------------------------------

/// A dynamic, priority-ordered registry of [`Source`] providers.
///
/// PV name resolution iterates sources from lowest `order` to highest and
/// delegates to the first source that claims the name.
pub struct SourceRegistry {
    sources: RwLock<Vec<SourceEntry>>,
}

impl SourceRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            sources: RwLock::new(Vec::new()),
        }
    }

    /// Register a new source at the given priority.
    ///
    /// Lower `order` values are queried first.
    pub async fn add(&self, label: impl Into<String>, order: i32, source: Arc<dyn Source>) {
        let label = label.into();
        debug!("SourceRegistry: adding source '{}' at order {}", label, order);
        let mut sources = self.sources.write().await;
        sources.push(SourceEntry {
            label,
            order,
            source,
        });
        sources.sort_by_key(|e| e.order);
    }

    /// Remove all sources with the given label.
    pub async fn remove(&self, label: &str) {
        debug!("SourceRegistry: removing source '{}'", label);
        let mut sources = self.sources.write().await;
        sources.retain(|e| e.label != label);
    }

    // ── Delegating operations ────────────────────────────────────────

    /// Find the first source that claims `name` and return its metadata.
    pub async fn claim(&self, name: &str) -> Option<PvInfo> {
        let sources = self.sources.read().await;
        for entry in sources.iter() {
            if let Some(info) = entry.source.claim(name).await {
                return Some(info);
            }
        }
        None
    }

    /// Check whether any source claims the given PV name.
    pub async fn has_pv(&self, name: &str) -> bool {
        self.claim(name).await.is_some()
    }

    /// Get the value from the first source that claims the PV.
    pub async fn get(&self, name: &str) -> Option<NtPayload> {
        let sources = self.sources.read().await;
        for entry in sources.iter() {
            if entry.source.claim(name).await.is_some() {
                return entry.source.get(name).await;
            }
        }
        None
    }

    /// Get the structure descriptor from the first source that claims the PV.
    pub async fn get_descriptor(&self, name: &str) -> Option<StructureDesc> {
        self.claim(name).await.map(|info| info.descriptor)
    }

    /// Check if the PV is writable (via the first claiming source).
    pub async fn is_writable(&self, name: &str) -> bool {
        self.claim(name).await.is_some_and(|info| info.writable)
    }

    /// Delegate a PUT to the first source that claims the PV.
    pub async fn put(
        &self,
        name: &str,
        value: &DecodedValue,
    ) -> Result<Vec<(String, NtPayload)>, String> {
        let sources = self.sources.read().await;
        for entry in sources.iter() {
            if entry.source.claim(name).await.is_some() {
                return entry.source.put(name, value).await;
            }
        }
        Err(format!("PV '{}' not found", name))
    }

    /// Subscribe via the first source that claims the PV.
    pub async fn subscribe(&self, name: &str) -> Option<mpsc::Receiver<NtPayload>> {
        let sources = self.sources.read().await;
        for entry in sources.iter() {
            if entry.source.claim(name).await.is_some() {
                return entry.source.subscribe(name).await;
            }
        }
        None
    }

    /// Execute an RPC call via the first source that claims the channel.
    pub async fn rpc(&self, name: &str, args: &DecodedValue) -> Result<NtPayload, String> {
        let sources = self.sources.read().await;
        for entry in sources.iter() {
            if entry.source.claim(name).await.is_some() {
                return entry.source.rpc(name, args).await;
            }
        }
        Err(format!("RPC channel '{}' not found", name))
    }

    /// Collect all PV names from every registered source.
    pub async fn names(&self) -> Vec<String> {
        let sources = self.sources.read().await;
        let mut seen = HashSet::new();
        let mut all = Vec::new();
        for entry in sources.iter() {
            for name in entry.source.names().await {
                if seen.insert(name.clone()) {
                    all.push(name);
                }
            }
        }
        all.sort();
        all
    }
}

impl Default for SourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}
