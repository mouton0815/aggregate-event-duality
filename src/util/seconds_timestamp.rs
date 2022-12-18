use std::time::{SystemTime, UNIX_EPOCH};

/// Abstraction from a clock with seconds resolution
pub trait SecondsTimestamp {
    fn get(&mut self) -> u64;
}

/// A Unix clock with seconds resolution.
/// Every call to ``get()`` returns the current number of seconds elapsed since 1.1.1970.
pub struct UnixTimestamp;

impl UnixTimestamp {
    pub fn new() -> Box<Self> {
        Box::new(Self)
    }
}

impl SecondsTimestamp for UnixTimestamp {
    fn get(&mut self) -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    }
}

#[cfg(test)]
pub mod tests {
    use crate::util::seconds_timestamp::SecondsTimestamp;

    pub struct IncrementalTimestamp {
        tick: u64
    }

    impl IncrementalTimestamp {
        pub fn new() -> Box<Self> {
            Box::new(Self { tick: 0 })
        }
    }

    impl SecondsTimestamp for IncrementalTimestamp {
        fn get(&mut self) -> u64 {
            self.tick += 1;
            self.tick
        }
    }

    #[test]
    pub fn test_timestamp() {
        let mut ticker = IncrementalTimestamp::new();
        assert_eq!(ticker.get(), 1);
        assert_eq!(ticker.get(), 2);
    }
}

