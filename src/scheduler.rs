use log::{debug, info, warn};
use std::time::{Duration, SystemTime};
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use tokio::time;
use crate::aggregator::MutexAggregator;

// Function spawns worker and waits for end of execution
async fn spawn_worker(aggregator: MutexAggregator, timestamp: SystemTime) -> bool {
    debug!("Spawn worker");
    let handle = tokio::spawn(async move {
        let mut aggregator = aggregator.lock().unwrap();
        match aggregator.delete_events_before(&timestamp) {
            Ok(_) => true,
            Err(e) => {
                warn!("Deletion task failed {:?}", e);
                false
            }
        }
    });
    match handle.await {
        Ok(result) => result,
        Err(e) => {
            warn!("Spawning worker returned error {:?}", e);
            false
        }
    }
}

// Must be async as required by tokio::select!
async fn repeat(aggregator: &MutexAggregator, period: Duration, mut rx: Receiver<()>) {
    let mut interval = time::interval(period);
    loop {
        tokio::select! {
            _ = interval.tick() => {},
            _ = rx.recv() => {
                debug!("Termination signal received, leave scheduler");
                break;
            }
        }
        let aggregator = aggregator.clone();
        if !spawn_worker(aggregator, SystemTime::now() - period).await {
            warn!("Worker returned error, leave scheduler");
            break;
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