use std::error::Error;
use std::time::Duration;
use futures_util::StreamExt;
use rand::{Rng, SeedableRng, thread_rng};
use rand::rngs::StdRng;
use thiserror::Error;
use aggregate_event_duality::util::scheduled_stream::{Fetcher, ScheduledStream};

#[derive(Error,Debug)]
pub enum MyError {
    #[error("End of sequence")]
    EndOfSequence
}

struct RandomGenerator {
    limit: u32,
    counter: u32,
    num_gen: StdRng
}

impl RandomGenerator {
    fn new(limit: u32) -> Self {
        Self { limit, counter: 0, num_gen: StdRng::from_rng(thread_rng()).unwrap() }
    }
}

impl Fetcher<String> for RandomGenerator {
    fn fetch(&mut self) -> Result<Vec<String>, Box<dyn Error>> {
        println!("start: {}", self.counter);
        let mut results = Vec::new();
        let bound = self.num_gen.gen_range(0 .. self.limit + 1);
        println!("count: {}", bound);
        for _ in 0..bound {
            self.counter += 1;
            results.push(self.counter.to_string());
        }
        Ok(results)
    }
}

#[tokio::main]
async fn main() {
    let g = Box::new(RandomGenerator::new(5));
    let mut s = ScheduledStream::new(Duration::from_secs(3), g);
    while let Some(item) = s.next().await {
        println!("-----> {}", item)
    }
}
