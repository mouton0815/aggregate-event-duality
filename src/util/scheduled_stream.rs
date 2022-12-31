use std::collections::VecDeque;
use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_util::Stream;
use log::error;
use tokio::time::{Interval, interval};

///
/// Trait for custom fetcher implementations needed by [ScheduledStream](ScheduledStream).
///
pub trait Fetcher<T, E> {
    fn fetch(&mut self) -> Result<Vec<T>, E>;
}

pub type BoxedFetcher<T, E> = Box<dyn Fetcher<T, E> + Send>;

///
/// An implementation of [Stream](futures_util::Stream) that periodically fetches items
/// from a source through a [Fetcher](Fetcher). While ``Fetcher::fetch()`` returns a vector
/// of items, method ``poll_next()`` returns the items one-by-one, utilizing a buffer.
///
pub struct ScheduledStream<T, E> {
    interval: Interval,
    buffer: Box<VecDeque<T>>,
    fetcher: BoxedFetcher<T, E>
}

impl<T, E> ScheduledStream<T, E> {
    pub fn new(duration: Duration, fetcher: BoxedFetcher<T, E>) -> Self {
        Self {
            interval: interval(duration),
            buffer: Box::new(VecDeque::new()),
            fetcher
        }
    }
}

impl<T, E: Debug> Stream for ScheduledStream<T, E> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if self.buffer.len() == 0 {
            ready!(self.interval.poll_tick(cx));
            match self.fetcher.fetch() {
                Ok(batch) => {
                    for item in batch {
                        self.buffer.push_back(item);
                    }
                }
                Err(err) => {
                    error!("Fetcher returned error {:?}, stop polling", err);
                    return Poll::Ready(None)
                }
            }
        }
        return match self.buffer.pop_front() {
            Some(x) => Poll::Ready(Some(x)),
            None => Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use futures_util::StreamExt;
    use crate::util::scheduled_stream::{Fetcher, ScheduledStream};

    #[derive(thiserror::Error,Debug)]
    enum TestError {
        #[error("End of sequence")]
        EndOfSequence
    }

    struct TestFetcher {
        batches: Vec<Vec<&'static str>>,
        index: usize
    }

    impl TestFetcher {
        fn new(batches: Vec<Vec<&'static str>>) -> Self {
            Self { batches, index: 0 }
        }
    }

    impl Fetcher<String, TestError> for TestFetcher {
        fn fetch(&mut self) -> Result<Vec<String>, TestError> {
            if self.index == self.batches.len() {
                return Err(TestError::EndOfSequence)
            }
            let iter = self.batches[self.index].iter();
            self.index += 1;
            Ok(iter.map(|y| String::from(*y)).collect())
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
