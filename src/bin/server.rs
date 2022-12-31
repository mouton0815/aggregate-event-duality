use log::{debug, info};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::{join, signal};
use tokio::sync::broadcast;
use aggregate_event_duality::aggregator::aggregator_facade::AggregatorFacade;
use aggregate_event_duality::rest::http_server::spawn_http_server;
use aggregate_event_duality::util::deletion_scheduler::{MutexDeletionTask, spawn_deletion_scheduler};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let aggregator = AggregatorFacade::new(":memory:")?;
    let aggregator= Arc::new(Mutex::new(aggregator));

    // Channel to inform the HTTP server and the delete scheduler to terminate.
    // The termination signal is triggered by signal::ctrl_c() below.
    let (tx, rx1) = broadcast::channel(1);
    let rx2 = tx.subscribe();

    // Start a task that periodically deletes older events.
    // Note that AggregatorFacade implements trait DeletionTask.
    let period = Duration::from_secs(120);
    let deletion_task: MutexDeletionTask<rusqlite::Error> = aggregator.clone();
    let delete_scheduler = spawn_deletion_scheduler(&deletion_task, rx1, period);

    let http_server = spawn_http_server(&aggregator, rx2, 5);

    signal::ctrl_c().await?;
    debug!("Termination signal received");
    tx.send(())?;

    let (_,_) = join!(delete_scheduler, http_server);
    info!("Deletion scheduler terminated");
    info!("HTTP Server terminated");

    Ok(())
}