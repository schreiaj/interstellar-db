//! Region subscriptions, fed by the store's change tap.
//!
//! The hub consumes [`ObservationEvent`]s from [`SpatioTemporalStore::watch`]
//! and fans each one out to every subscription whose sphere contains it — so
//! subscribers hear about *every* observation that lands on this node,
//! whether it arrived by a local write or by anti-entropy sync from a peer.
//!
//! Each subscription carries a `max_age_secs` guard: sync can replay hours of
//! history when a partitioned peer reconnects, and a live consumer (e.g. a
//! tracker) usually wants only fresh observations, not the flood. `0` means
//! unfiltered.
//!
//! [`SpatioTemporalStore::watch`]: crate::SpatioTemporalStore::watch

use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::{broadcast, mpsc};

use crate::store::ObservationEvent;

struct Subscription {
    center_x: f64,
    center_y: f64,
    center_z: f64,
    /// Pre-computed radius² to avoid a sqrt per event.
    r2: f64,
    /// Only deliver observations at most this old (by their own timestamp)
    /// at arrival time; 0 disables the filter.
    max_age_secs: u64,
    tx: mpsc::Sender<ObservationEvent>,
}

#[derive(Default)]
pub struct SubscriptionHub {
    subs: RwLock<Vec<Subscription>>,
}

impl SubscriptionHub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a subscription; the returned receiver yields every matching
    /// event until dropped. Delivery is lossy: if the receiver's buffer is
    /// full the event is skipped, never queued unboundedly.
    pub fn attach(
        &self,
        center_x: f64,
        center_y: f64,
        center_z: f64,
        radius: f64,
        max_age_secs: u64,
        buffer: usize,
    ) -> mpsc::Receiver<ObservationEvent> {
        let (tx, rx) = mpsc::channel(buffer);
        self.subs.write().unwrap().push(Subscription {
            center_x,
            center_y,
            center_z,
            r2: radius * radius,
            max_age_secs,
            tx,
        });
        rx
    }

    /// Drive the hub from a store's watch channel until the store is dropped.
    /// Spawn this once per node: `tokio::spawn(hub.clone().run(store.watch()))`.
    pub async fn run(
        self: std::sync::Arc<Self>,
        mut events: broadcast::Receiver<ObservationEvent>,
    ) {
        loop {
            match events.recv().await {
                Ok(event) => self.dispatch(&event),
                // Fell behind the tap's ring buffer: the overflow is lost,
                // which matches the store's lossy notification semantics.
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    fn dispatch(&self, event: &ObservationEvent) {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Fan out under a read lock so dispatch never serializes writers.
        let any_closed = {
            let subs = self.subs.read().unwrap();
            let mut any_closed = false;
            for sub in subs.iter() {
                if sub.tx.is_closed() {
                    any_closed = true;
                    continue;
                }
                if sub.max_age_secs > 0
                    && now_secs.saturating_sub(event.timestamp_secs) > sub.max_age_secs
                {
                    continue;
                }
                let dx = event.x - sub.center_x;
                let dy = event.y - sub.center_y;
                let dz = event.z - sub.center_z;
                if dx * dx + dy * dy + dz * dz <= sub.r2 {
                    let _ = sub.tx.try_send(event.clone());
                }
            }
            any_closed
        };

        if any_closed {
            self.subs.write().unwrap().retain(|sub| !sub.tx.is_closed());
        }
    }
}
