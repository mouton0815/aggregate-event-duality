use std::error::Error;
use std::sync::{Arc, Mutex};
use tokio::signal;
use tokio::sync::broadcast;
use aggregate_event_duality::aggregator::company_aggregator::CompanyAggregator;
use aggregate_event_duality::rest::http_server::spawn_http_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let aggregator = CompanyAggregator::new(":memory:")?;
    let aggregator= Arc::new(Mutex::new(aggregator));

    let (tx, rx) = broadcast::channel(1);

    let handle = spawn_http_server(aggregator, rx);
    tokio::pin!(handle);

    loop {
        tokio::select! {
            _ = &mut handle => {
                println!("HTTP Server terminated");
                break;
            }
            s = signal::ctrl_c() => {
                match s {
                    Ok(()) => {
                        println!("Termination signal received");
                        tx.send(())?;
                    },
                    Err(err) => {
                        eprintln!("Unable to listen for shutdown signal: {}", err);
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}