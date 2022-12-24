use log::{debug, info, warn};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::signal;
use tokio::sync::broadcast;
use aggregate_event_duality::aggregator::aggregator_facade::AggregatorFacade;
use aggregate_event_duality::rest::http_server::spawn_http_server;
use aggregate_event_duality::util::deletion_scheduler::{MutexDeletionTask, spawn_deletion_scheduler};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let aggregator = AggregatorFacade::new(":memory:")?;
    let aggregator= Arc::new(Mutex::new(aggregator));

    let (tx, rx_scheduler) = broadcast::channel(1);
    let rx_http_server = tx.subscribe();

    let period = Duration::from_secs(10);
    let deletion_task: MutexDeletionTask<rusqlite::Error> = aggregator.clone(); // Aggregator implements trait DeletionTask
    let scheduler_handle = spawn_deletion_scheduler(&deletion_task, rx_scheduler, period);
    tokio::pin!(scheduler_handle);

    let http_server_handle = spawn_http_server(&aggregator, rx_http_server, 5);
    tokio::pin!(http_server_handle);

    loop {
        tokio::select! {
            _ = &mut scheduler_handle => {
                info!("Deletion scheduler terminated");
                break;
            }
            _ = &mut http_server_handle => {
                info!("HTTP Server terminated");
                break;
            }
            s = signal::ctrl_c() => {
                match s {
                    Ok(()) => {
                        debug!("Termination signal received");
                        tx.send(())?;
                    },
                    Err(err) => {
                        warn!("Unable to listen for shutdown signal: {}", err);
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}