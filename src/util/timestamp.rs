use std::time::{SystemTime, UNIX_EPOCH};

/// Abstraction from a clock with seconds resolution
/// The goal is to make the clock mockable by a simple counter, which is handy for unit tests.
pub trait Timestamp {
    fn as_secs(&mut self) -> u64;
}

pub type BoxedTimestamp = Box<dyn Timestamp + Send>;

/// A Unix clock with seconds resolution.
/// Every call to ``get()`` returns the current number of seconds elapsed since 1.1.1970.
pub struct UnixTimestamp;

impl UnixTimestamp {
    pub fn new() -> Box<Self> {
        Box::new(Self)
    }
}

impl Timestamp for UnixTimestamp {
    fn as_secs(&mut self) -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    }
}

#[cfg(test)]
pub mod tests {
    use crate::util::timestamp::Timestamp;

    /// Timestamp implementation as a simpler counter that is incremented by each call to ``as_secs()``.
    /// Used by the unit tests of [Aggregator](crate::aggregator::Aggregator).
    pub struct IncrementalTimestamp {
        tick: u64
    }

    impl IncrementalTimestamp {
        pub fn new() -> Box<Self> {
            Box::new(Self { tick: 0 })
        }
    }

    impl Timestamp for IncrementalTimestamp {
        fn as_secs(&mut self) -> u64 {
            self.tick += 1;
            self.tick
        }
    }

    #[test]
    pub fn test_timestamp() {
        let mut ticker = IncrementalTimestamp::new();
        assert_eq!(ticker.as_secs(), 1);
        assert_eq!(ticker.as_secs(), 2);
    }
}

