use std::convert::Infallible;
use std::time::Duration;
use serde::{Serialize, Deserialize};
use futures_util::StreamExt;
use warp::http::StatusCode;
use warp::{reply, Reply, sse};
use warp::sse::Event;
use crate::aggregator::aggregator_facade::MutexAggregator;
use crate::domain::event_type::EventType;
use crate::domain::person_data::PersonData;
use crate::domain::person_id::PersonId;
use crate::domain::person_patch::PersonPatch;
use crate::rest::event_fetcher::EventFetcher;
use crate::util::scheduled_stream::ScheduledStream;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
struct ErrorResult {
    error: String
}

pub async fn post_person(aggregator: MutexAggregator, path: &str, person: PersonData) -> Result<Box<dyn Reply>, Infallible> {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.insert(&person) {
        Ok(result) => {
            let (person_id, person_data) = result;
            let location = format!("/{}/{}", path, person_id);
            let response = reply::json(&person_data);
            let response = reply::with_status(response, StatusCode::CREATED);
            let response = reply::with_header(response,"Location", location);
            Ok(Box::new(response))
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Ok(Box::new(reply::with_status(reply::json(&message), StatusCode::INTERNAL_SERVER_ERROR)))
        }
    }
}

pub async fn patch_person(aggregator: MutexAggregator, person_id: PersonId, person: PersonPatch) -> Result<Box<dyn Reply>, Infallible> {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.update(person_id, &person) {
        Ok(result) => {
            match result {
                Some(person) => Ok(Box::new(reply::json(&person))),
                None => Ok(Box::new(reply::with_status("Person not found", StatusCode::NOT_FOUND)))
            }
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Ok(Box::new(reply::with_status(reply::json(&message), StatusCode::INTERNAL_SERVER_ERROR)))
        }
    }
}

pub async fn delete_person(aggregator: MutexAggregator, person_id: PersonId) -> Result<Box<dyn Reply>, Infallible> {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.delete(person_id) {
        Ok(result) => {
            match result {
                true => Ok(Box::new(reply())),
                false => Ok(Box::new(reply::with_status("Person not found", StatusCode::NOT_FOUND)))
            }
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Ok(Box::new(reply::with_status(reply::json(&message), StatusCode::INTERNAL_SERVER_ERROR)))
        }
    }
}

pub async fn get_persons(aggregator: MutexAggregator, revision_header_name: &str) -> Result<Box<dyn Reply>, Infallible> {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.get_persons() {
        Ok(result) => {
            let (revision, persons) = result;
            Ok(Box::new(reply::with_header(reply::json(&persons), revision_header_name, revision)))
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Ok(Box::new(reply::with_status(reply::json(&message), StatusCode::INTERNAL_SERVER_ERROR)))
        }
    }
}

pub async fn get_locations(aggregator: MutexAggregator, revision_header_name: &str) -> Result<Box<dyn Reply>, Infallible> {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.get_locations() {
        Ok(result) => {
            let (revision, locations) = result;
            Ok(Box::new(reply::with_header(reply::json(&locations), revision_header_name, revision)))
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Ok(Box::new(reply::with_status(reply::json(&message), StatusCode::INTERNAL_SERVER_ERROR)))
        }
    }
}

pub async fn get_events(aggregator: MutexAggregator, event_type: EventType, repeat_every_secs: u64, from_revision: Option<usize>) -> Result<impl Reply, Infallible> {
    let from_revision = from_revision.unwrap_or(1);
    let fetcher = Box::new(EventFetcher::new(aggregator, event_type, from_revision));
    let stream = ScheduledStream::new(Duration::from_secs(repeat_every_secs), fetcher);
    let stream = stream.map(move |item| {
        Ok::<Event, Infallible>(Event::default().data(item))
    });
    Ok(sse::reply(stream))
}
