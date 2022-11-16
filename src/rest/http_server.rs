use std::convert::Infallible;
use log::info;
use warp::Filter;
use crate::rest::rest_handlers::{get_companies, MutexedCompanyAggregator, post_company, patch_company, get_company_events, delete_company};

fn with_aggregator(aggregator: MutexedCompanyAggregator)
    -> impl Filter<Extract = (MutexedCompanyAggregator,), Error = Infallible> + Clone {
    warp::any().map(move || aggregator.clone())
}

pub async fn spawn_http_server(aggregator: MutexedCompanyAggregator) {
    info!("Spawn HTTP server");

    let path = "companies";
    let route_get_companies = warp::path(path)
        .and(warp::get())
        .and(with_aggregator(aggregator.clone()))
        .and_then(get_companies);

    let route_post_company = warp::path(path)
        .and(warp::post())
        .and(with_aggregator(aggregator.clone()))
        .and(warp::body::json())
        .and_then(post_company);

    let route_patch_company = warp::path(path)
        .and(warp::patch())
        .and(with_aggregator(aggregator.clone()))
        .and(warp::path::param::<u32>())
        .and(warp::body::json())
        .and_then(patch_company);

    let route_delete_company = warp::path(path)
        .and(warp::delete())
        .and(with_aggregator(aggregator.clone()))
        .and(warp::path::param::<u32>())
        .and_then(delete_company);

    let route_get_company_events = warp::path("company-events")
        .and(warp::get())
        .and(with_aggregator(aggregator.clone()))
        .and(warp::header::optional::<u32>("x-from-revision"))
        .and_then(get_company_events);

    let routes = route_get_companies
        .or(route_post_company)
        .or(route_patch_company)
        .or(route_delete_company)
        .or(route_get_company_events);

    warp::serve(routes)
        .run(([127, 0, 0, 1], 3000))
        .await;
}