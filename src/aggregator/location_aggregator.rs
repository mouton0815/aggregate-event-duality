use std::time::Duration;
use rusqlite::{Connection, Result, Transaction};
use crate::aggregator::aggregator_trait::AggregatorTrait;
use crate::database::event_table::LocationEventTable;
use crate::database::location_table::LocationTable;
use crate::database::revision_table::{RevisionTable, RevisionType};
use crate::domain::location_data::LocationData;
use crate::domain::location_event::LocationEvent;
use crate::domain::location_map::LocationMap;
use crate::domain::location_patch::LocationPatch;
use crate::domain::person_data::PersonData;
use crate::domain::person_patch::PersonPatch;
use crate::util::patch::Patch;
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

    fn upsert(&mut self, tx: &Transaction, name: &str, mut data: LocationData, patch: LocationPatch) -> Result<()> {
        data.apply_patch(&patch);
        LocationTable::upsert(tx, name, &data)?;
        let event = LocationEvent::new(name, Some(patch));
        self.write_event_and_revision(tx, event)
    }

    // TODO: Document method
    fn update_or_delete(&mut self, tx: &Transaction, name: &str, mut data: LocationData, patch: LocationPatch) -> Result<()> {
        // If after an update or delete the attribute "total" is 0, then delete the corresponding
        // location record and write an event that indicates deletion, i.e. { <location>: null }.
        let event : LocationEvent;
        if patch.total.is_some() && patch.total.unwrap() == 0 {
            LocationTable::delete(tx, name)?;
            event = LocationEvent::new(name, None);
        } else {
            data.apply_patch(&patch);
            LocationTable::upsert(tx, name, &data)?;
            event = LocationEvent::new(name, Some(patch));
        }
        self.write_event_and_revision(tx, event)
    }

    fn write_event_and_revision(&mut self, tx: &Transaction, event: LocationEvent) -> Result<()> {
        let event = Self::stringify(event);
        let timestamp = self.timestamp.as_secs();
        let revision = LocationEventTable::insert(&tx, timestamp, event.as_str())?;
        RevisionTable::upsert(&tx, RevisionType::LOCATION, revision)
    }

    fn stringify(event: LocationEvent) -> String {
        serde_json::to_string(&event).unwrap() // Errors should not happen, panic accepted
    }
}

impl AggregatorTrait for LocationAggregator {
    type Records = LocationMap;

    fn create_tables(&mut self, connection: &Connection) -> Result<()> {
        LocationTable::create_table(connection)?;
        LocationEventTable::create_table(connection)
    }

    fn insert(&mut self, tx: &Transaction, _: u32, person: &PersonData) -> Result<()> {
        if let Some(name) = person.location.as_ref() {
            let data = Self::select_or_init(tx, name)?;
            if let Some(patch) = LocationPatch::for_insert(&data, person) {
                self.upsert(tx, name, data, patch)?;
            }
        }
        Ok(())
    }

    fn update(&mut self, tx: &Transaction, _: u32, person: &PersonData, patch: &PersonPatch) -> Result<()> {
        if let Some(name) = person.location.as_ref() {
            // The person had a location before the update - select the corresponding record.
            let data = Self::select_or_init(tx, name)?;
            if patch.location.is_absent() {
                // The location of the person stays the same.
                // Increment or decrement the counters for all aggregate values, except "total".
                if let Some(patch) = LocationPatch::for_update(&data, person, patch) {
                    self.upsert(tx, name, data, patch)?;
                }
            } else {
                // The location of the person changed.
                // Decrement the counters of the old location or delete the location record.
                if let Some(patch) = LocationPatch::for_delete(&data, person) {
                    self.update_or_delete(tx, name, data, patch)?;
                }
            }
        }
        if let Patch::Value(name) = patch.location.as_ref() {
            // The location of the person changed.
            // Increment the counters of the new location.
            let data = Self::select_or_init(tx, name)?;
            if let Some(patch) = LocationPatch::for_change(&data, person, patch) {
                self.upsert(tx, name, data, patch)?;
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
        let revision = RevisionTable::read(&tx, RevisionType::PERSON)?;
        let locations = LocationTable::select_all(&tx)?;
        Ok((revision, locations))
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