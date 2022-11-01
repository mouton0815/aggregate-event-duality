use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use warp::Filter;
use crate::aggregator::company_aggregator::CompanyAggregator;
use crate::http_server::handlers::{get_companies, post_company};

pub type MutexedCompanyAggregator = Arc<Mutex<CompanyAggregator>>;

mod handlers {
    use std::convert::Infallible;
    use serde::{Serialize, Deserialize};
    use warp::http::StatusCode;
    use crate::domain::company_rest::CompanyPost;
    use crate::http_server::MutexedCompanyAggregator;

    #[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
    struct ErrorResult {
        error: String
    }

    // TODO: Pass company by reference?
    pub async fn post_company(company: CompanyPost, aggregator: MutexedCompanyAggregator) -> Result<impl warp::Reply, Infallible> {
        let mut aggregator = aggregator.lock().await;
        return match aggregator.create(&company) {
            Ok(result) => {
                let json = warp::reply::json(&result);
                Ok(warp::reply::with_status(json, StatusCode::CREATED))
            },
            Err(error) => {
                let message = ErrorResult{ error: error.to_string() };
                let json = warp::reply::json(&message);
                Ok(warp::reply::with_status(json, StatusCode::INTERNAL_SERVER_ERROR))
            }
        }
    }

    pub async fn get_companies(aggregator: MutexedCompanyAggregator) -> Result<impl warp::Reply, Infallible> {
        let res = ErrorResult{ error: String::from("Foo & Bar") };
        let body = warp::reply::json(&res);
        Ok(warp::reply::with_header(body, "X-Company-Revision", 1))
    }
}

fn with_aggregator(aggregator: MutexedCompanyAggregator)
    -> impl Filter<Extract = (MutexedCompanyAggregator,), Error = Infallible> + Clone {
    warp::any().map(move || aggregator.clone())
}

pub fn spawn_http_server(aggregator: MutexedCompanyAggregator, mut rx: Receiver<()>) -> JoinHandle<()> {
    println!("Spawn HTTP server");

    let get_companies = warp::path("foo")
        .and(warp::get())
        .and(with_aggregator(aggregator.clone()))
        .and_then(get_companies);

    let post_company = warp::path("companies")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_aggregator(aggregator.clone()))
        .and_then(post_company);

    let (_, server) = warp::serve(post_company)
        .bind_with_graceful_shutdown(([127, 0, 0, 1], 3000), async move {
            rx.recv().await.unwrap();
            println!("Termination signal received, leave HTTP server");
        });

    tokio::spawn(server)
}