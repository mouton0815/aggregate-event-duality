use std::convert::Infallible;
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use tokio::sync::Mutex;
use warp::http::StatusCode;
use warp::{reply, Reply};
use crate::aggregator::company_aggregator::CompanyAggregator;
use crate::domain::company_rest::{CompanyPost, CompanyPut};

pub type MutexedCompanyAggregator = Arc<Mutex<CompanyAggregator>>;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
struct ErrorResult {
    error: String
}

// TODO: Pass company by reference?
pub async fn post_company(aggregator: MutexedCompanyAggregator, company: CompanyPost) -> Result<impl Reply, Infallible> {
    let mut aggregator = aggregator.lock().await;
    return match aggregator.create(&company) {
        Ok(result) => {
            Ok(reply::with_status(reply::json(&result), StatusCode::CREATED))
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Ok(reply::with_status(reply::json(&message), StatusCode::INTERNAL_SERVER_ERROR)) // TODO: Better errors
        }
    }
}

// TODO: Pass company by reference?
pub async fn put_company(aggregator: MutexedCompanyAggregator, company_id: u32, company: CompanyPut) -> Result<impl Reply, Infallible> {
    let mut aggregator = aggregator.lock().await;
    return match aggregator.update(company_id, &company) {
        Ok(result) => {
            Ok(reply::with_status(reply::json(&result), StatusCode::OK))
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Ok(reply::with_status(reply::json(&message), StatusCode::INTERNAL_SERVER_ERROR)) // TODO: Better errors
        }
    }
}

pub async fn get_companies(aggregator: MutexedCompanyAggregator) -> Result<impl Reply, Infallible> {
    let mut aggregator = aggregator.lock().await;
    return match aggregator.get_all() {
        Ok(result) => {
            Ok(reply::with_status(reply::json(&result), StatusCode::CREATED))
        },
        Err(error) => {
            let message = ErrorResult{ error: error.to_string() };
            Ok(reply::with_status(reply::json(&message), StatusCode::INTERNAL_SERVER_ERROR)) // TODO: Better errors
        }
    }
}
