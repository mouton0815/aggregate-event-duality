use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use rand::Rng;
use rand::rngs::ThreadRng;
use tokio::time::{Interval, interval};

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

pub trait Generator {
    fn generate(&mut self) -> Vec<String>;
}

impl Generator for RandomGenerator {
    fn generate(&mut self) -> Vec<String> {
        println!("Fetch from {}", self.counter);
        let mut results = Vec::new();
        let bound = self.num_gen.gen_range(0 .. self.limit + 1);
        println!("--b--> {}", bound);
        for _ in 0..bound {
            self.counter += 1;
            results.push(self.counter.to_string());
        }
        results
    }
}

pub struct ScheduledStream {
    interval: Interval,
    buffer: VecDeque<String>,
    generator: Box<dyn Generator + 'static>
}

impl ScheduledStream {
    pub fn new(duration: Duration, generator: Box<dyn Generator + 'static>) -> Self {
        Self {
            interval: interval(duration),
            buffer: VecDeque::new(),
            generator
        }
    }
}

impl Stream for ScheduledStream {
    type Item = String;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<String>> {
        if self.buffer.len() == 0 {
            ready!(self.interval.poll_tick(cx));
            for item in self.generator.generate() {
                self.buffer.push_back(item);
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

#[tokio::main]
async fn main() {
    let g = Box::new(RandomGenerator::new(5));
    let mut s = ScheduledStream::new(Duration::from_secs(3), g);
    while let Some(item) = s.next().await {
        println!("---> {}", item)
    }
}