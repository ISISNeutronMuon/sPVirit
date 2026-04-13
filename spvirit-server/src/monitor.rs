//! Monitor subscription management for the PVA server.
//!
//! Tracks per-PV subscriber lists and dispatches monitor update messages.

use std::collections::HashMap;

use tokio::sync::{mpsc, Mutex};
use tracing::debug;

use spvirit_codec::spvirit_encode::{
    encode_monitor_data_response_filtered, encode_monitor_data_response_payload,
};
use spvirit_types::NtPayload;

use crate::state::MonitorSub;

/// Active connection channels and monitor subscriptions managed by the server.
pub struct MonitorRegistry {
    /// PV name → list of active monitor subscriptions.
    pub monitors: Mutex<HashMap<String, Vec<MonitorSub>>>,
    /// Connection id → message sender.
    pub conns: Mutex<HashMap<u64, mpsc::Sender<Vec<u8>>>>,
}

impl MonitorRegistry {
    pub fn new() -> Self {
        Self {
            monitors: Mutex::new(HashMap::new()),
            conns: Mutex::new(HashMap::new()),
        }
    }

    /// Send a raw message to a connection.
    pub async fn send_msg(&self, conn_id: u64, msg: Vec<u8>) {
        let conns = self.conns.lock().await;
        if let Some(tx) = conns.get(&conn_id) {
            let _ = tx.send(msg).await;
        }
    }

    /// Broadcast a monitor update for `pv_name` to all running subscribers.
    pub async fn notify_monitors(&self, pv_name: &str, payload: &NtPayload) {
        let mut to_send: Vec<(u64, Vec<u8>)> = Vec::new();
        {
            let mut monitors = self.monitors.lock().await;
            if let Some(list) = monitors.get_mut(pv_name) {
                for sub in list.iter_mut() {
                    if !sub.running {
                        continue;
                    }
                    if sub.pipeline_enabled && sub.nfree == 0 {
                        continue;
                    }
                    let subcmd = 0x00;
                    if sub.pipeline_enabled && sub.nfree > 0 {
                        sub.nfree -= 1;
                    }
                    let msg = if let Some(ref desc) = sub.filtered_desc {
                        encode_monitor_data_response_filtered(
                            sub.ioid, subcmd, payload, desc, sub.version, sub.is_be,
                        )
                    } else {
                        encode_monitor_data_response_payload(
                            sub.ioid, subcmd, payload, sub.version, sub.is_be,
                        )
                    };
                    to_send.push((sub.conn_id, msg));
                }
            }
        }

        for (conn_id, msg) in to_send {
            self.send_msg(conn_id, msg).await;
            debug!("Monitor update pv='{}' conn={}", pv_name, conn_id);
        }
    }

    /// Send a monitor update to a specific subscriber.
    pub async fn send_monitor_update_for(
        &self,
        pv_name: &str,
        conn_id: u64,
        ioid: u32,
        payload: &NtPayload,
    ) {
        let mut to_send: Option<(u64, Vec<u8>)> = None;
        {
            let mut monitors = self.monitors.lock().await;
            if let Some(list) = monitors.get_mut(pv_name) {
                if let Some(sub) = list
                    .iter_mut()
                    .find(|s| s.conn_id == conn_id && s.ioid == ioid)
                {
                    if !sub.running {
                        return;
                    }
                    if sub.pipeline_enabled && sub.nfree == 0 {
                        return;
                    }
                    let subcmd = 0x00;
                    if sub.pipeline_enabled && sub.nfree > 0 {
                        sub.nfree -= 1;
                    }
                    let msg = if let Some(ref desc) = sub.filtered_desc {
                        encode_monitor_data_response_filtered(
                            sub.ioid, subcmd, payload, desc, sub.version, sub.is_be,
                        )
                    } else {
                        encode_monitor_data_response_payload(
                            sub.ioid, subcmd, payload, sub.version, sub.is_be,
                        )
                    };
                    to_send = Some((sub.conn_id, msg));
                }
            }
        }

        if let Some((conn_id, msg)) = to_send {
            self.send_msg(conn_id, msg).await;
        }
    }

    /// Update a monitor subscription's running/pipeline state.
    pub async fn update_monitor_subscription(
        &self,
        conn_id: u64,
        ioid: u32,
        pv_name: &str,
        running: bool,
        nfree: Option<u32>,
        pipeline_enabled: Option<bool>,
    ) -> bool {
        let mut monitors = self.monitors.lock().await;
        if let Some(list) = monitors.get_mut(pv_name) {
            if let Some(sub) = list
                .iter_mut()
                .find(|s| s.conn_id == conn_id && s.ioid == ioid)
            {
                sub.running = running;
                if let Some(v) = nfree {
                    sub.nfree = v;
                }
                if let Some(enabled) = pipeline_enabled {
                    if enabled {
                        sub.pipeline_enabled = true;
                    }
                }
                return true;
            }
        }
        false
    }

    /// Remove a monitor subscription.
    pub async fn remove_monitor_subscription(
        &self,
        conn_id: u64,
        ioid: u32,
        pv_name: &str,
    ) {
        let mut monitors = self.monitors.lock().await;
        if let Some(list) = monitors.get_mut(pv_name) {
            list.retain(|s| s.conn_id != conn_id || s.ioid != ioid);
        }
    }

    /// Remove all subscriptions and connection entries for a given connection.
    pub async fn cleanup_connection(&self, conn_id: u64) {
        {
            let mut monitors = self.monitors.lock().await;
            for list in monitors.values_mut() {
                list.retain(|s| s.conn_id != conn_id);
            }
        }
        {
            let mut conns = self.conns.lock().await;
            conns.remove(&conn_id);
        }
    }
}
