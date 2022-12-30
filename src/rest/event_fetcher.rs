use crate::aggregator::aggregator_facade::MutexAggregator;
use crate::domain::event_type::EventType;
use crate::util::scheduled_stream::Fetcher;

///
/// Implementation of trait [Fetcher](Fetcher) for serialized events retrieved from either
/// [PersonEventTable](crate::database::event_table::PersonEventTable) or
/// [LocationEventTable](crate::database::event_table::LocationEventTable) trough
/// [AggregatorFacade](crate::aggregator::aggregator_facade::AggregatorFacade).
///
/// Class ``EventFetcher`` is used by
/// [ScheduledStream](crate::util::scheduled_stream::ScheduledStream) instantiated in function
/// [get_events](crate::rest::rest_handlers::get_events).
///
pub struct EventFetcher {
    aggregator: MutexAggregator,
    event_type: EventType,
    offset: usize
}

impl EventFetcher {
    pub fn new(aggregator: MutexAggregator, event_type: EventType, offset: usize) -> Self {
        Self { aggregator, event_type, offset }
    }
}

impl Fetcher<String, rusqlite::Error> for EventFetcher {
    fn fetch(&mut self) -> Result<Vec<String>, rusqlite::Error> {
        let mut aggregator = self.aggregator.lock().unwrap();
        return match aggregator.get_events(self.event_type, self.offset) {
            Err(err) => Err(err),
            Ok(events) => {
                self.offset += events.len();
                Ok(events)
            }
        }
    }
}