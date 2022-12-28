use std::time::Duration;
use rusqlite::{Connection, Result, Transaction};
use crate::aggregator::aggregator_trait::AggregatorTrait;
use crate::database::event_table::LocationEventTable;
use crate::database::location_table::LocationTable;
use crate::database::revision_table::{RevisionTable, RevisionType};
use crate::domain::location_data::LocationData;
use crate::domain::location_event::LocationEvent;
use crate::domain::location_patch::LocationPatch;
use crate::domain::person_data::PersonData;
use crate::domain::person_patch::PersonPatch;
use crate::util::timestamp::{BoxedTimestamp, UnixTimestamp};

///
/// Does statistics on persons (currently counting only) and stores the results in table ```location```.
/// Writes the corresponding events and updates the corresponding revision number.
///
pub struct LocationAggregator {
    timestamp: BoxedTimestamp
}

impl LocationAggregator {
    pub fn new() -> Self {
        Self::new_internal(UnixTimestamp::new())
    }

    fn new_internal(timestamp: BoxedTimestamp) -> Self {
        Self{ timestamp }
    }

    fn select_or_init(tx: &Transaction, name: &str) -> Result<LocationData> {
        Ok(match LocationTable::select_by_name(tx, name)? {
            Some(location_data) => location_data,
            None => LocationData::new(0, 0)
        })
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

impl AggregatorTrait for LocationAggregator {
    type Records = ();

    fn create_tables(&mut self, connection: &Connection) -> Result<()> {
        LocationTable::create_table(connection)?;
        LocationEventTable::create_table(connection)
    }

    fn insert(&mut self, tx: &Transaction, _: u32, person: &PersonData) -> Result<()> {
        if let Some(name) = person.location.as_ref() {
            let mut location_data = Self::select_or_init(tx, name)?;
            if let Some(patch) = LocationPatch::for_insert(&location_data, person) {
                location_data.apply_patch(&patch);
                LocationTable::upsert(tx, name, &location_data)?;
                // TODO: Write event and revision
            }
        }
        Ok(())
    }

    fn update(&mut self, tx: &Transaction, _: u32, person: &PersonData, patch: &PersonPatch) -> Result<()> {
        // TODO: If total == 0, send name:null event and delete location record
        if person.location.is_some() {
            let name = person.location.as_ref().unwrap();
            let mut location_data = Self::select_or_init(tx, name)?;
            if let Some(patch) = LocationPatch::for_update_old(&location_data, person, patch) {
                location_data.apply_patch(&patch);
                LocationTable::upsert(tx, name, &location_data)?;
                // TODO: Write event and revision
            }
        }
        if patch.location.is_value() {
            // Location of person changed, increment counters of new location
            let name = patch.location.as_ref().unwrap();
            let mut location_data = Self::select_or_init(tx, name)?;
            if let Some(patch) = LocationPatch::for_update_new(&location_data, person, patch) {
                location_data.apply_patch(&patch);
                LocationTable::upsert(tx, name, &location_data)?;
                // TODO: Write event and revision
            }
        }
        Ok(())
    }

    fn delete(&mut self, tx: &Transaction, _: u32, person: &PersonData) -> Result<()> {
        // TODO: If total == 0, send name:null event and delete location record
        if let Some(name) = person.location.as_ref() {
            let mut location_data = Self::select_or_init(tx, name)?;
            if let Some(patch) = LocationPatch::for_delete(&location_data, person) {
                location_data.apply_patch(&patch);
                // TODO: Derive record from patch and store it. Write event and revision.
            }
        }
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

#[cfg(test)]
mod tests {
}