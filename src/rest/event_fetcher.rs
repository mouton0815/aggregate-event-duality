use crate::aggregator::MutexAggregator;
use crate::util::scheduled_stream::Fetcher;

pub struct PersonEventFetcher {
    aggregator: MutexAggregator,
    offset: u32
}

impl PersonEventFetcher {
    pub fn new(aggregator: MutexAggregator, offset: u32) -> Self {
        Self { aggregator, offset }
    }
}

impl Fetcher<String, rusqlite::Error> for PersonEventFetcher {
    fn fetch(&mut self) -> Result<Vec<String>, rusqlite::Error> {
        let mut aggregator = self.aggregator.lock().unwrap();
        let results = aggregator.get_person_events(self.offset);
        return match results {
            Err(err) => Err(err),
            Ok(events) => {
                self.offset += events.len() as u32;
                Ok(events)
            }
        }
    }
}

pub struct LocationEventFetcher {
    aggregator: MutexAggregator,
    offset: u32
}

impl LocationEventFetcher {
    pub fn new(aggregator: MutexAggregator, offset: u32) -> Self {
        Self { aggregator, offset }
    }
}

impl Fetcher<String, rusqlite::Error> for LocationEventFetcher {
    fn fetch(&mut self) -> Result<Vec<String>, rusqlite::Error> {
        let mut aggregator = self.aggregator.lock().unwrap();
        let results = aggregator.get_location_events(self.offset);
        return match results {
            Err(err) => Err(err),
            Ok(events) => {
                self.offset += events.len() as u32;
                Ok(events)
            }
        }
    }
}

