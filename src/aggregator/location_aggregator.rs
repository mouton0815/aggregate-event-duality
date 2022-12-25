use std::time::Duration;
use rusqlite::{Connection, Result, Transaction};
use crate::aggregator::aggregator_trait::AggregatorTrait;
use crate::database::event_table::LocationEventTable;
use crate::database::revision_table::{RevisionTable, RevisionType};
use crate::domain::location_event::LocationEvent;
use crate::domain::person_data::PersonData;
use crate::domain::person_patch::PersonPatch;
use crate::util::timestamp::{BoxedTimestamp, UnixTimestamp};

pub struct LocationAggregator {
    timestamp: BoxedTimestamp
}

impl AggregatorTrait for LocationAggregator {
    type Records = ();

    fn create_tables(&mut self, connection: &Connection) -> Result<()> {
        // TODO: Create tables
        Ok(())
    }

    fn insert(&mut self, tx: &Transaction, person_id: u32, person: &PersonData) -> Result<()> {
        // TODO: Implement
        Ok(())
    }

    fn update(&mut self, tx: &Transaction, person_id: u32, person: &PersonData, patch: &PersonPatch) -> Result<()> {
        // TODO: Implement
        Ok(())
    }

    fn delete(&mut self, tx: &Transaction, person_id: u32, _: &PersonData) -> Result<()> {
        // TODO: Implement
        Ok(())
    }

    fn get_all(&mut self, tx: &Transaction) -> Result<(u32, Self::Records)> {
        // TODO: Implement
        Ok((0, ()))
    }

    fn get_events(&mut self, tx: &Transaction, from_revision: u32) -> Result<Vec<String>> {
        LocationEventTable::read(&tx, from_revision)
    }

    fn delete_events(&mut self, tx: &Transaction, created_before: Duration) -> Result<usize> {
        let created_before = self.timestamp.as_secs() - created_before.as_secs();
        LocationEventTable::delete_before(&tx, created_before)
    }
}

impl LocationAggregator {
    pub fn new() -> Self {
        Self::new_internal(UnixTimestamp::new())
    }

    fn new_internal(timestamp: BoxedTimestamp) -> Self {
        Self{ timestamp }
    }

    fn write_event_and_revision(&mut self, tx: &Transaction, timestamp: u64, event: LocationEvent) -> Result<()> {
        let event = Self::stringify(event);
        let revision = LocationEventTable::insert(&tx, timestamp, event.as_str())?;
        RevisionTable::upsert(&tx, RevisionType::LOCATION, revision)
    }

    fn stringify(event: LocationEvent) -> String {
        serde_json::to_string(&event).unwrap() // Errors should not happen, panic accepted
    }
}

#[cfg(test)]
mod tests {
}