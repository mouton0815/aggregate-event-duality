use std::convert::Infallible;
use log::info;
use warp::Filter;
use crate::rest::rest_handlers::{MutexedPersonAggregator, post_person, patch_person, get_person_events, delete_person, get_persons};

fn with_aggregator(aggregator: MutexedPersonAggregator)
    -> impl Filter<Extract = (MutexedPersonAggregator,), Error = Infallible> + Clone {
    warp::any().map(move || aggregator.clone())
}

// TODO: Isn't there a simpler way??
fn with_constant<T:Send+Copy>(argument: T) -> impl Filter<Extract = (T,), Error = Infallible> + Clone {
    warp::any().map(move || argument)
}

pub async fn spawn_http_server(aggregator: MutexedPersonAggregator, repeat_every_secs: u64) {
    info!("Spawn HTTP server");

    let path = "persons";
    let route_get_persons = warp::path(path)
        .and(warp::get())
        .and(with_aggregator(aggregator.clone()))
        .and_then(get_persons);

    let route_post_person = warp::path(path)
        .and(warp::post())
        .and(with_aggregator(aggregator.clone()))
        .and(with_constant(path))
        .and(warp::body::json())
        .and_then(post_person);

    let route_patch_person = warp::path(path)
        .and(warp::patch())
        .and(with_aggregator(aggregator.clone()))
        .and(warp::path::param::<u32>())
        .and(warp::body::json())
        .and_then(patch_person);

    let route_delete_person = warp::path(path)
        .and(warp::delete())
        .and(with_aggregator(aggregator.clone()))
        .and(warp::path::param::<u32>())
        .and_then(delete_person);

    let route_get_person_events = warp::path("person-events")
        .and(warp::get())
        .and(with_aggregator(aggregator.clone()))
        .and(with_constant(repeat_every_secs))
        .and(warp::header::optional::<u32>("x-from-revision"))
        .and_then(get_person_events);

    let routes = route_get_persons
        .or(route_post_person)
        .or(route_patch_person)
        .or(route_delete_person)
        .or(route_get_person_events);

    warp::serve(routes)
        .run(([127, 0, 0, 1], 3000))
        .await;
}