use std::convert::Infallible;
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use tokio::sync::Mutex;
use warp::http::StatusCode;
use warp::{reply, Reply};
use crate::aggregator::company_aggregator::CompanyAggregator;
use crate::domain::company_rest::{CompanyPost, CompanyPatch};

pub type MutexedCompanyAggregator = Arc<Mutex<CompanyAggregator>>;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
struct ErrorResult {
    error: String
}

pub async fn post_company(aggregator: MutexedCompanyAggregator, company: CompanyPost) -> Result<impl Reply, Infallible> {
    let mut aggregator = aggregator.lock().await;
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
    let mut aggregator = aggregator.lock().await;
    return match aggregator.update(company_id, &company) {
        Ok(result) => {
            match result {
                Some(company) => Ok(Box::new(reply::json(&company))),
                None => Ok(Box::new(StatusCode::NOT_FOUND))
            }
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Ok(Box::new(reply::with_status(reply::json(&message), StatusCode::INTERNAL_SERVER_ERROR)))
        }
    }
}

pub async fn get_companies(aggregator: MutexedCompanyAggregator) -> Result<Box<dyn Reply>, Infallible> {
    let mut aggregator = aggregator.lock().await;
    return match aggregator.get_all() {
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
