extern crate core;

use std::sync::{Arc, Mutex};
use aggregate_event_duality::aggregator::company_aggregator::CompanyAggregator;
use aggregate_event_duality::rest::http_server::spawn_http_server;

#[tokio::main]
async fn main() {

    let aggregator = CompanyAggregator::new(":memory:")
        .unwrap_or_else(|error| panic!("{}", error));
    let aggregator= Arc::new(Mutex::new(aggregator));

    spawn_http_server(aggregator).await;
}