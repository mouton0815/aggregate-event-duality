use std::convert::Infallible;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use serde::{Serialize, Deserialize};
use futures_util::StreamExt;
use warp::http::StatusCode;
use warp::{reply, Reply, sse};
use warp::sse::Event;
use crate::aggregator::company_aggregator::CompanyAggregator;
use crate::domain::company_rest::{CompanyPost, CompanyPatch};
use crate::util::scheduled_stream::{Fetcher, ScheduledStream};


pub type MutexedCompanyAggregator = Arc<Mutex<CompanyAggregator>>;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
struct ErrorResult {
    error: String
}

pub async fn post_company(aggregator: MutexedCompanyAggregator, company: CompanyPost) -> Result<impl Reply, Infallible> {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.create(&company) {
        Ok(result) => {
            Ok(reply::with_status(reply::json(&result), StatusCode::CREATED))
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Ok(reply::with_status(reply::json(&message), StatusCode::INTERNAL_SERVER_ERROR))
        }
    }
}

pub async fn patch_company(aggregator: MutexedCompanyAggregator, company_id: u32, company: CompanyPatch) -> Result<Box<dyn Reply>, Infallible> {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.update(company_id, &company) {
        Ok(result) => {
            match result {
                Some(company) => Ok(Box::new(reply::json(&company))),
                None => Ok(Box::new(reply::with_status("Company not found", StatusCode::NOT_FOUND)))
            }
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Ok(Box::new(reply::with_status(reply::json(&message), StatusCode::INTERNAL_SERVER_ERROR)))
        }
    }
}

pub async fn get_companies(aggregator: MutexedCompanyAggregator) -> Result<Box<dyn Reply>, Infallible> {
    let mut aggregator = aggregator.lock().unwrap();
    return match aggregator.get_aggregates() {
        Ok(result) => {
            let (revision, companies) = result;
            Ok(Box::new(reply::with_header(reply::json(&companies), "X-Company-Revision", revision)))
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Ok(Box::new(reply::with_status(reply::json(&message), StatusCode::INTERNAL_SERVER_ERROR))) // TODO: Better errors
        }
    }
}

struct CompanyEventFetcher {
    offset: u32,
    aggregator: MutexedCompanyAggregator
}

impl CompanyEventFetcher {
    fn new(offset: u32, aggregator: MutexedCompanyAggregator) -> Self {
        Self { offset, aggregator }
    }
}

impl Fetcher<String> for CompanyEventFetcher {
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

pub async fn get_company_events(aggregator: MutexedCompanyAggregator, from_revision: u32) -> Result<impl Reply, Infallible> {
    let fetcher = Box::new(CompanyEventFetcher::new(from_revision, aggregator));
    let stream = ScheduledStream::new(Duration::from_secs(1), fetcher);
    let stream = stream.map(move |item| {
        Ok::<Event, Infallible>(Event::default().data(item))
    });
    Ok(sse::reply(stream))
}
