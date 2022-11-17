use std::convert::Infallible;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use serde::{Serialize, Deserialize};
use futures_util::StreamExt;
use warp::http::StatusCode;
use warp::{reply, Reply, sse};
use warp::sse::Event;
use crate::aggregator::person_aggregator::PersonAggregator;
use crate::domain::person_rest::{PersonPost, PersonPatch};
use crate::util::scheduled_stream::{Fetcher, ScheduledStream};


pub type MutexedPersonAggregator = Arc<Mutex<PersonAggregator>>;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
struct ErrorResult {
    error: String
}

pub async fn post_person(aggregator: MutexedPersonAggregator, person: PersonPost) -> Result<impl Reply, Infallible> {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.create(&person) {
        Ok(result) => {
            Ok(reply::with_status(reply::json(&result), StatusCode::CREATED))
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Ok(reply::with_status(reply::json(&message), StatusCode::INTERNAL_SERVER_ERROR))
        }
    }
}

pub async fn patch_person(aggregator: MutexedPersonAggregator, person_id: u32, person: PersonPatch) -> Result<Box<dyn Reply>, Infallible> {
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

pub async fn delete_person(aggregator: MutexedPersonAggregator, person_id: u32) -> Result<Box<dyn Reply>, Infallible> {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.delete(person_id) {
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

pub async fn get_persons(aggregator: MutexedPersonAggregator) -> Result<Box<dyn Reply>, Infallible> {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.get_aggregates() {
        Ok(result) => {
            let (revision, persons) = result;
            Ok(Box::new(reply::with_header(reply::json(&persons), "X-From-Revision", revision)))
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Ok(Box::new(reply::with_status(reply::json(&message), StatusCode::INTERNAL_SERVER_ERROR))) // TODO: Better errors
        }
    }
}

struct PersonEventFetcher {
    offset: u32,
    aggregator: MutexedPersonAggregator
}

impl PersonEventFetcher {
    fn new(offset: u32, aggregator: MutexedPersonAggregator) -> Self {
        Self { offset, aggregator }
    }
}

impl Fetcher<String> for PersonEventFetcher {
    fn fetch(&mut self) -> Result<Vec<String>, Box<dyn Error>> {
        let mut aggregator = self.aggregator.lock().unwrap();
        let results = aggregator.get_events(self.offset);
        return match results {
            Err(err) => Err(err),
            Ok(events) => {
                self.offset += events.len() as u32;
                Ok(events)
            }
        }
    }
}

pub async fn get_person_events(aggregator: MutexedPersonAggregator, repeat_every_secs: u64, from_revision: Option<u32>) -> Result<impl Reply, Infallible> {
    let from_revision = from_revision.unwrap_or(1);
    let fetcher = Box::new(PersonEventFetcher::new(from_revision, aggregator));
    let stream = ScheduledStream::new(Duration::from_secs(repeat_every_secs), fetcher);
    let stream = stream.map(move |item| {
        Ok::<Event, Infallible>(Event::default().data(item))
    });
    Ok(sse::reply(stream))
}
