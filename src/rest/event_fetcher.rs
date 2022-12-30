use crate::aggregator::aggregator_facade::MutexAggregator;
use crate::util::scheduled_stream::Fetcher;

///
/// Implementation of trait [Fetcher](Fetcher) for serialized objects of class
/// [PersonEvent](crate::domain::person_event::PersonEvent) retrieved from
/// [PersonEventTable](crate::database::event_table::PersonEventTable) trough
/// [PersonAggregator](crate::aggregator::person_aggregator::PersonAggregator) via
/// [AggregatorFacade](crate::aggregator::aggregator_facade::AggregatorFacade).
///
/// Class ``PersonEventFetcher`` is used by
/// [ScheduledStream](crate::util::scheduled_stream::ScheduledStream) instantiated in function
/// [get_person_events](crate::rest::rest_handlers::get_person_events).
///
pub struct PersonEventFetcher {
    aggregator: MutexAggregator,
    offset: usize
}

impl PersonEventFetcher {
    pub fn new(aggregator: MutexAggregator, offset: usize) -> Self {
        Self { aggregator, offset }
    }
}

impl Fetcher<String, rusqlite::Error> for PersonEventFetcher {
    fn fetch(&mut self) -> Result<Vec<String>, rusqlite::Error> {
        let mut aggregator = self.aggregator.lock().unwrap();
        return match aggregator.get_person_events(self.offset) {
            Err(err) => Err(err),
            Ok(events) => {
                self.offset += events.len();
                Ok(events)
            }
        }
    }
}

///
/// Implementation of trait [Fetcher](Fetcher) for serialized objects of class
/// [LocationEvent](crate::domain::location_event::LocationEvent) retrieved from
/// [LocationEventTable](crate::database::event_table::LocationEventTable) trough
/// [LocationAggregator](crate::aggregator::location_aggregator::LocationAggregator) via
/// [AggregatorFacade](crate::aggregator::aggregator_facade::AggregatorFacade).
///
/// Class ``LocationEventFetcher`` is used by
/// [ScheduledStream](crate::util::scheduled_stream::ScheduledStream) instantiated in function
/// [get_location_events](crate::rest::rest_handlers::get_location_events).
///
pub struct LocationEventFetcher {
    aggregator: MutexAggregator,
    offset: usize
}

impl LocationEventFetcher {
    pub fn new(aggregator: MutexAggregator, offset: usize) -> Self {
        Self { aggregator, offset }
    }
}

impl Fetcher<String, rusqlite::Error> for LocationEventFetcher {
    fn fetch(&mut self) -> Result<Vec<String>, rusqlite::Error> {
        let mut aggregator = self.aggregator.lock().unwrap();
        return match aggregator.get_location_events(self.offset) {
            Err(err) => Err(err),
            Ok(events) => {
                self.offset += events.len();
                Ok(events)
            }
        }
    }
}
