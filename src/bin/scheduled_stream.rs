use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;
use futures_util::{Stream, StreamExt};
use rand::Rng;
use rand::rngs::ThreadRng;
use tokio::time::{Interval, interval};

/*
struct Generator {
    counter: u32,
    randgen: ThreadRng
}

impl Generator {
    fn new() -> Self {
        Self { counter: 0, randgen: rand::thread_rng() }
    }

    fn generate(&mut self) -> Vec<u32> {
        println!("Fetch from {}", self.counter);
        let mut results = Vec::new();
        let x = self.randgen.gen_range(0..3); // yields [0,4]
        println!("--x--> {}", x);
        for _ in 0..x { // yields [0,3]
            self.counter += 1;
            results.push(self.counter);
        }
        results
    }
}
*/

struct RandomGenerator {
    counter: u32,
    randgen: ThreadRng
}

impl RandomGenerator {
    fn new() -> Self {
        Self { counter: 0, randgen: rand::thread_rng() }
    }
}

trait Generator {
    fn generate(&mut self) -> Vec<u32>;
}

impl Generator for RandomGenerator {
    fn generate(&mut self) -> Vec<u32> {
        println!("Fetch from {}", self.counter);
        let mut results = Vec::new();
        let x = self.randgen.gen_range(0..3); // yields [0,4]
        println!("--x--> {}", x);
        for _ in 0..x { // yields [0,3]
            self.counter += 1;
            results.push(self.counter);
        }
        results
    }
}

pub struct ScheduledStream {
    /// Future that completes the next time the `Interval` yields a value.
    interval: Interval, // TODO: Could be a simple "sleep"
    buffer: VecDeque<String>,
    is_first: bool,
    generator: Box<dyn Generator + 'static>
}

impl ScheduledStream {
    pub fn new(duration: Duration, generator: Box<dyn Generator + 'static>) -> Self {
        Self {
            interval: interval(duration),
            buffer: VecDeque::new(),
            is_first: true,
            generator
        }
    }
}

impl Stream for ScheduledStream {
    type Item = String;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<String>> {
        if self.buffer.len() == 0 {
            if self.is_first {
                self.is_first = false;
            } else {
                ready!(self.interval.poll_tick(cx));
            }
            let results = self.generator.generate();
            println!("--g--> {}", results.len());
            for item in results {
                self.buffer.push_back(item.to_string());
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

fn generator() -> Vec<u32> {
    vec![1, 3, 5]
}

#[tokio::main]
async fn main() {
    let g = Box::new(RandomGenerator::new());
    let mut s = ScheduledStream::new(Duration::from_secs(3), g);
    while let Some(item) = s.next().await {
        println!("---> {}", item)
    }
}