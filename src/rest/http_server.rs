use std::net::SocketAddr;
use log::{debug, info};
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use axum::{routing::{delete, get, patch, post}, Router};
use crate::aggregator::aggregator_facade::MutexAggregator;
use crate::rest::rest_handlers::{delete_person, get_persons, get_person_events, get_location_events, get_locations, patch_person, post_person};
use crate::rest::shared_state::SharedState;


pub fn spawn_http_server(aggregator: MutexAggregator, mut rx: Receiver<()>, repeat_every_secs: u64) -> JoinHandle<()> {
    info!("Spawn HTTP server");
    let shared_state = SharedState {
        aggregator,
        repeat_every_secs
    };
    let routes = Router::new()
        .route("/persons", get(get_persons))
        .route("/persons", post(post_person))
        .route("/persons/:person_id", patch(patch_person))
        .route("/persons/:person_id", delete(delete_person))
        .route("/person-events", get(get_person_events))
        .route("/locations", post(get_locations))
        .route("/location-events", get(get_location_events))
        .with_state(shared_state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tokio::spawn(async move {
        axum::Server::bind(&addr)
            .serve(routes.into_make_service())
            .with_graceful_shutdown(async {
                rx.recv().await.unwrap();
                debug!("Termination signal received, leave HTTP server");
            })
            .await
            .unwrap()
    })
}
