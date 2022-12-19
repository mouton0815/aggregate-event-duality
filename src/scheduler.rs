use log::{debug, info, warn};
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use tokio::time;
use crate::aggregator::MutexAggregator;

// Must be async as required by tokio::select!
async fn repeat(aggregator: &MutexAggregator, period: Duration, mut rx: Receiver<()>) {
    let mut interval = time::interval(period);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let mut aggregator = aggregator.lock().unwrap();
                if let Err(e) = aggregator.delete_events(period) {
                    warn!("Deletion task failed {:?}, leave scheduler", e);
                    break;
                }
            },
            _ = rx.recv() => {
                debug!("Termination signal received, leave scheduler");
                break;
            }
        }
    }
}

pub fn spawn_scheduler(aggregator: &MutexAggregator, rx: Receiver<()>, period: Duration) -> JoinHandle<()> {
    info!("Spawn scheduler");
    let aggregator = aggregator.clone();
    tokio::spawn(async move {
        repeat(&aggregator, period, rx).await;
    })
}