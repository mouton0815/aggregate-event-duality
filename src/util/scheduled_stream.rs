use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use rand::Rng;
use rand::rngs::ThreadRng;
use tokio::time::{Interval, interval};

pub trait Fetcher {
    fn fetch(&mut self) -> Option<Vec<String>>;
}

////////////////////////

struct RandomGenerator {
    limit: u32,
    counter: u32,
    num_gen: ThreadRng
}

impl RandomGenerator {
    fn new(limit: u32) -> Self {
        Self { limit, counter: 0, num_gen: rand::thread_rng() }
    }
}

impl Fetcher for RandomGenerator {
    fn fetch(&mut self) -> Option<Vec<String>> {
        println!("Fetch from {}", self.counter);
        let mut results = Vec::new();
        let bound = self.num_gen.gen_range(0 .. self.limit + 1);
        println!("--b--> {}", bound);
        for _ in 0..bound {
            self.counter += 1;
            results.push(self.counter.to_string());
        }
        Some(results)
    }
}

////////////////////////

pub struct ScheduledStream {
    interval: Interval,
    buffer: VecDeque<String>,
    fetcher: Box<dyn Fetcher + 'static>
}

impl ScheduledStream {
    pub fn new(duration: Duration, fetcher: Box<dyn Fetcher + 'static>) -> Self {
        Self {
            interval: interval(duration),
            buffer: VecDeque::new(),
            fetcher
        }
    }
}

impl Stream for ScheduledStream {
    type Item = String;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<String>> {
        if self.buffer.len() == 0 {
            ready!(self.interval.poll_tick(cx));
            match self.fetcher.fetch() {
                None => return Poll::Ready(None), // Terminate polling
                Some(batch) => {
                    for item in batch {
                        self.buffer.push_back(item);
                    }
                }
            }
        }
        return match self.buffer.pop_front() {
            Some(x) => Poll::Ready(Some(x)),
            None => Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (std::usize::MAX, None)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use futures_util::StreamExt;
    use crate::util::scheduled_stream::{Fetcher, ScheduledStream};

    struct TestFetcher {
        batches: Vec<Vec<&'static str>>,
        index: usize
    }

    impl TestFetcher {
        fn new(batches: Vec<Vec<&'static str>>) -> Self {
            Self { batches, index: 0 }
        }
    }

    impl Fetcher for TestFetcher {
        fn fetch(&mut self) -> Option<Vec<String>> {
            if self.index == self.batches.len() {
                return None
            }
            let iter = self.batches[self.index].iter();
            self.index += 1;
            Some(iter.map(|y| String::from(*y)).collect())
        }
    }

    #[tokio::test]
    async fn test_empty_first_batch() {
        exec_test(vec![vec![], vec!["1","2"], vec!["3"]], vec!["1","2","3"]).await
    }

    #[tokio::test]
    async fn test_empty_last_batch() {
        exec_test(vec![vec!["1"], vec!["2","3"], vec![]], vec!["1","2","3"]).await
    }

    async fn exec_test(data: Vec<Vec<&'static str>>, ref_results: Vec<&str>) {
        let g = Box::new(TestFetcher::new(data));
        let mut s = ScheduledStream::new(Duration::from_millis(3), g);
        let mut v = Vec::new();
        while let Some(item) = s.next().await {
            v.push(item);
        }
        assert_eq!(v, ref_results);
    }
}

/*
#[tokio::main]
async fn main() {
    let g = Box::new(RandomGenerator::new(5));
    let mut s = ScheduledStream::new(Duration::from_secs(3), g);
    while let Some(item) = s.next().await {
        println!("-----> {}", item)
    }
}
*/