use std::convert::Infallible;
use std::time::Duration;
use axum::http::StatusCode;
use axum::{extract::State, Json, TypedHeader};
use axum::extract::Path;
use axum::response::Sse;
use axum::response::sse::Event;
use futures::Stream;
use serde::{Serialize, Deserialize};
use futures_util::StreamExt;
use crate::aggregator::aggregator_facade::MutexAggregator;
use crate::domain::event_type::EventType;
use crate::domain::location_map::LocationMap;
use crate::domain::person_data::PersonData;
use crate::domain::person_id::PersonId;
use crate::domain::person_map::PersonMap;
use crate::domain::person_patch::PersonPatch;
use crate::rest::event_fetcher::EventFetcher;
use crate::rest::location_header::LocationHeader;
use crate::rest::revision_header::RevisionHeader;
use crate::util::scheduled_stream::ScheduledStream;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct ErrorResult {
    error: String
}

type PostResponse = Result<(StatusCode, TypedHeader<LocationHeader>, Json<PersonData>), (StatusCode, Json<ErrorResult>)>;

pub async fn post_person(State(aggregator): State<MutexAggregator>, Json(person): Json<PersonData>) -> PostResponse {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.insert(&person) {
        Ok(result) => {
            let (person_id, person_data) = result;
            let location = format!("/persons/{}", person_id);
            let location_header = LocationHeader::from(location);
            Ok((StatusCode::CREATED, TypedHeader(location_header), Json(person_data)))
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(message)))
        }
    }
}

type PatchResponse = Result<Json<PersonData>, (StatusCode, Json<ErrorResult>)>;

pub async fn patch_person(State(aggregator): State<MutexAggregator>, Path(person_id): Path<PersonId>, Json(person): Json<PersonPatch>) -> PatchResponse {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.update(person_id, &person) {
        Ok(result) => {
            match result {
                Some(person) => Ok(Json(person)),
                None => {
                    let message = ErrorResult{ error: "Person not found".to_string() };
                    Err((StatusCode::NOT_FOUND, Json(message)))
                }
            }
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(message)))
        }
    }
}

type DeleteResponse = Result<StatusCode, (StatusCode, Json<ErrorResult>)>;

pub async fn delete_person(State(aggregator): State<MutexAggregator>, Path(person_id): Path<PersonId>) -> DeleteResponse {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.delete(person_id) {
        Ok(result) => {
            match result {
                true => Ok(StatusCode::OK),
                false => {
                    let message = ErrorResult{ error: "Person not found".to_string() };
                    Err((StatusCode::NOT_FOUND, Json(message)))
                }
            }
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(message)))
        }
    }
}

type GetPersonsResponse = Result<(TypedHeader<RevisionHeader>, Json<PersonMap>), (StatusCode, Json<ErrorResult>)>;

pub async fn get_persons(State(aggregator): State<MutexAggregator>) -> GetPersonsResponse {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.get_persons() {
        Ok(result) => {
            let (revision, persons) = result;
            Ok((TypedHeader(RevisionHeader::from(revision)), Json(persons)))
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(message)))
        }
    }
}

type GetLocationsResponse = Result<(TypedHeader<RevisionHeader>, Json<LocationMap>), (StatusCode, Json<ErrorResult>)>;

pub async fn get_locations(State(aggregator): State<MutexAggregator>) -> GetLocationsResponse {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.get_locations() {
        Ok(result) => {
            let (revision, locations) = result;
            Ok((TypedHeader(RevisionHeader::from(revision)), Json(locations)))
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(message)))
        }
    }
}


// Note: type GetEventsResponse = Sse<impl Stream<Item = Result<Event, Infallible>>> does not work as feature is unstable

pub async fn get_person_events(State(aggregator): State<MutexAggregator>, State(repeat_every_seconds): State<u64>, TypedHeader(from_revision): TypedHeader<RevisionHeader>)
    -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    get_events(aggregator, EventType::PERSON, repeat_every_seconds, from_revision)
}

pub async fn get_location_events(State(aggregator): State<MutexAggregator>, State(repeat_every_seconds): State<u64>, TypedHeader(from_revision): TypedHeader<RevisionHeader>)
    -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    get_events(aggregator, EventType::LOCATION, repeat_every_seconds, from_revision)
}

fn get_events(aggregator: MutexAggregator, event_type: EventType, repeat_every_seconds: u64, from_revision: RevisionHeader)
    -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let fetcher = Box::new(EventFetcher::new(aggregator, event_type, from_revision.into()));
    let stream = ScheduledStream::new(Duration::from_secs(repeat_every_seconds), fetcher);
    let stream = stream.map(move |item| {
        Ok::<Event, Infallible>(Event::default().data(item))
    });
    Sse::new(stream)
}
