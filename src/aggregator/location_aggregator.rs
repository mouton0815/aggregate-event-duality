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

    // TODO: Document method
    fn update_or_delete(&mut self, tx: &Transaction, name: &str, mut data: LocationData, patch: LocationPatch) -> Result<()> {
        if patch.total.is_some() && patch.total.unwrap() == 0 {
            LocationTable::delete(tx, name)?;
            // TODO: Write event and revision
        } else {
            data.apply_patch(&patch);
            LocationTable::upsert(tx, name, &data)?;
            // TODO: Write event and revision
        }
        Ok(())
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
            let mut data = Self::select_or_init(tx, name)?;
            if let Some(patch) = LocationPatch::for_insert(&data, person) {
                data.apply_patch(&patch);
                LocationTable::upsert(tx, name, &data)?;
                // TODO: Write event and revision
            }
        }
        Ok(())
    }

    fn update(&mut self, tx: &Transaction, _: u32, person: &PersonData, patch: &PersonPatch) -> Result<()> {
        if person.location.is_some() {
            // This means that the person originally had a location.
            // Now either the location of the person stays the same, then adapt all counters except total.
            // Or the location of the person changes, then decrement then counters of old location.
            let name = person.location.as_ref().unwrap();
            let data = Self::select_or_init(tx, name)?;
            if let Some(patch) = LocationPatch::for_update_old(&data, person, patch) {
                self.update_or_delete(tx, name, data, patch)?;
            }
        }
        if patch.location.is_value() {
            // This means that the location of the person changed.
            // Increment the counters of the new location.
            let name = patch.location.as_ref().unwrap();
            let mut data = Self::select_or_init(tx, name)?;
            if let Some(patch) = LocationPatch::for_update_new(&data, person, patch) {
                data.apply_patch(&patch);
                LocationTable::upsert(tx, name, &data)?;
                // TODO: Write event and revision
            }
        }
        Ok(())
    }

    fn delete(&mut self, tx: &Transaction, _: u32, person: &PersonData) -> Result<()> {
        if let Some(name) = person.location.as_ref() {
            let data = Self::select_or_init(tx, name)?;
            if let Some(patch) = LocationPatch::for_delete(&data, person) {
                self.update_or_delete(tx, name, data, patch)?;
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