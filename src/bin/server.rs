use log::{debug, info, warn};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::signal;
use tokio::sync::broadcast;
use aggregate_event_duality::aggregator::Aggregator;
use aggregate_event_duality::rest::http_server::spawn_http_server;
use aggregate_event_duality::scheduler::spawn_scheduler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let aggregator = Aggregator::new(":memory:")?;
    let aggregator= Arc::new(Mutex::new(aggregator));

    let (tx, rx_scheduler) = broadcast::channel(1);
    let rx_http_server = tx.subscribe();

    let scheduler_handle = spawn_scheduler(&aggregator, rx_scheduler, Duration::from_secs(10));
    tokio::pin!(scheduler_handle);

    let http_server_handle = spawn_http_server(&aggregator, rx_http_server, 5);
    tokio::pin!(http_server_handle);

    loop {
        tokio::select! {
            _ = &mut scheduler_handle => {
                info!("Scheduler terminated");
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