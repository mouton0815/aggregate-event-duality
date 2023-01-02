use std::convert::Infallible;
use log::{debug, info};
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use warp::Filter;
use crate::aggregator::aggregator_facade::MutexAggregator;
use crate::domain::event_type::EventType;
use crate::domain::person_id::PersonId;
use crate::rest::rest_handlers::{post_person, patch_person, delete_person, get_persons, get_events, get_locations};

const REVISION_HEADER: &'static str = "X-Revision";

fn with_aggregator(aggregator: MutexAggregator)
    -> impl Filter<Extract = (MutexAggregator,), Error = Infallible> + Clone {
    warp::any().map(move || aggregator.clone())
}

// Allows to pass any constant to a Warp filter
fn with_constant<T:Send+Copy>(argument: T) -> impl Filter<Extract = (T,), Error = Infallible> + Clone {
    warp::any().map(move || argument)
}

pub fn spawn_http_server(aggregator: &MutexAggregator, mut rx: Receiver<()>, repeat_every_secs: u64) -> JoinHandle<()> {
    info!("Spawn HTTP server");

    let path_persons = "persons";
    let path_person_events = "person-events";
    let path_locations = "locations";
    let path_location_events = "location-events";

    let route_get_persons = warp::path(path_persons)
        .and(warp::get())
        .and(with_aggregator(aggregator.clone()))
        .and(with_constant(REVISION_HEADER))
        .and_then(get_persons);

    let route_post_person = warp::path(path_persons)
        .and(warp::post())
        .and(with_aggregator(aggregator.clone()))
        .and(with_constant(path_persons))
        .and(warp::body::json())
        .and_then(post_person);

    let route_patch_person = warp::path(path_persons)
        .and(warp::patch())
        .and(with_aggregator(aggregator.clone()))
        .and(warp::path::param::<PersonId>())
        .and(warp::body::json())
        .and_then(patch_person);

    let route_delete_person = warp::path(path_persons)
        .and(warp::delete())
        .and(with_aggregator(aggregator.clone()))
        .and(warp::path::param::<PersonId>())
        .and_then(delete_person);

    let route_get_person_events = warp::path(path_person_events)
        .and(warp::get())
        .and(with_aggregator(aggregator.clone()))
        .and(with_constant(EventType::PERSON))
        .and(with_constant(repeat_every_secs))
        .and(warp::header::optional::<usize>(REVISION_HEADER))
        .and_then(get_events);

    let route_get_locations = warp::path(path_locations)
        .and(warp::get())
        .and(with_aggregator(aggregator.clone()))
        .and(with_constant(REVISION_HEADER))
        .and_then(get_locations);

    let route_get_location_events = warp::path(path_location_events)
        .and(warp::get())
        .and(with_aggregator(aggregator.clone()))
        .and(with_constant(EventType::LOCATION))
        .and(with_constant(repeat_every_secs))
        .and(warp::header::optional::<usize>(REVISION_HEADER))
        .and_then(get_events);

    let routes = route_get_persons
        .or(route_post_person)
        .or(route_patch_person)
        .or(route_delete_person)
        .or(route_get_person_events)
        .or(route_get_locations)
        .or(route_get_location_events);

    let (_, server) = warp::serve(routes)
        .bind_with_graceful_shutdown(([127, 0, 0, 1], 3000), async move {
            rx.recv().await.unwrap();
            debug!("Termination signal received, leave HTTP server");
        });

    tokio::spawn(server)
}