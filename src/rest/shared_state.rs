use axum::extract::FromRef;
use crate::aggregator::aggregator_facade::MutexAggregator;

#[derive(FromRef,Clone)]
pub struct SharedState {
    pub aggregator: MutexAggregator,
    pub repeat_every_secs: u64,
}
