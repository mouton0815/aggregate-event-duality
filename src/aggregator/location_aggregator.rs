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
    use rusqlite::{Connection, Result, Transaction};
    use crate::aggregator::aggregator_trait::AggregatorTrait;
    use crate::aggregator::location_aggregator::LocationAggregator;
    use crate::database::event_table::LocationEventTable;
    use crate::database::location_table::LocationTable;
    use crate::database::revision_table::{RevisionTable, RevisionType};
    use crate::domain::location_data::LocationData;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;
    use crate::util::timestamp::tests::IncrementalTimestamp;

    //
    // Test aggregation functions
    //

    #[test]
    pub fn test_insert() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", Some("here"), Some(123));
        assert!(aggregator.insert(&tx, 1, &person).is_ok());

        check_record(&tx, "here", Some(LocationData::new(1, 1)));
        check_events(&tx, &[r#"{"here":{"total":1,"married":1}}"#]);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_update_keep_location_keep_spouse() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", Some("here"), Some(123));
        let patch = PersonPatch::new(None, Patch::Absent, Patch::Absent);
        assert!(aggregator.insert(&tx, 1, &person).is_ok());
        assert!(aggregator.update(&tx, 1, &person, &patch).is_ok());

        check_record(&tx, "here", Some(LocationData::new(1, 1)));
        check_events(&tx, &[r#"{"here":{"total":1,"married":1}}"#]); // No update event
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_update_keep_location_set_spouse() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", Some("here"), None);
        let patch = PersonPatch::new(None, Patch::Absent, Patch::Value(123));
        assert!(aggregator.insert(&tx, 1, &person).is_ok());
        assert!(aggregator.update(&tx, 1, &person, &patch).is_ok());

        check_record(&tx, "here", Some(LocationData::new(1, 1)));
        check_events(&tx, &[r#"{"here":{"total":1}}"#, r#"{"here":{"married":1}}"#]);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_update_keep_location_delete_spouse() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", Some("here"), Some(123));
        let patch = PersonPatch::new(None, Patch::Absent, Patch::Null);
        assert!(aggregator.insert(&tx, 1, &person).is_ok());
        assert!(aggregator.update(&tx, 1, &person, &patch).is_ok());

        check_record(&tx, "here", Some(LocationData::new(1, 0)));
        check_events(&tx, &[r#"{"here":{"total":1,"married":1}}"#, r#"{"here":{"married":0}}"#]);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_update_set_location_keep_spouse() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", None, Some(123));
        let patch = PersonPatch::new(None, Patch::Value("here"), Patch::Absent);
        assert!(aggregator.insert(&tx, 1, &person).is_ok());
        assert!(aggregator.update(&tx, 1, &person, &patch).is_ok());

        check_record(&tx, "here", Some(LocationData::new(1, 1)));
        check_events(&tx, &[r#"{"here":{"total":1,"married":1}}"#]);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_update_set_location_set_spouse() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", None, None);
        let patch = PersonPatch::new(None, Patch::Value("here"), Patch::Value(123));
        assert!(aggregator.insert(&tx, 1, &person).is_ok());
        assert!(aggregator.update(&tx, 1, &person, &patch).is_ok());

        check_record(&tx, "here", Some(LocationData::new(1, 1)));
        check_events(&tx, &[r#"{"here":{"total":1,"married":1}}"#]);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_update_set_location_delete_spouse() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", None, Some(123));
        let patch = PersonPatch::new(None, Patch::Value("here"), Patch::Null);
        assert!(aggregator.insert(&tx, 1, &person).is_ok());
        assert!(aggregator.update(&tx, 1, &person, &patch).is_ok());

        check_record(&tx, "here", Some(LocationData::new(1, 0)));
        check_events(&tx, &[r#"{"here":{"total":1}}"#]);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_update_remove_location_keep_spouse() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        let person1 = PersonData::new("Hans", Some("here"), None);
        let person2 = PersonData::new("Inge", Some("here"), Some(456));
        let patch = PersonPatch::new(None, Patch::Null, Patch::Absent);
        assert!(aggregator.insert(&tx, 1, &person1).is_ok());
        assert!(aggregator.insert(&tx, 2, &person2).is_ok());
        assert!(aggregator.update(&tx, 2, &person2, &patch).is_ok());

        check_record(&tx, "here", Some(LocationData::new(1, 0)));
        check_events(&tx, &[
            r#"{"here":{"total":1}}"#, // TODO: Should initial value of "married" be 0 ?
            r#"{"here":{"total":2,"married":1}}"#,
            r#"{"here":{"total":1,"married":0}}"#]);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_update_remove_location_remove_spouse() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        let person1 = PersonData::new("Hans", Some("here"), None);
        let person2 = PersonData::new("Inge", Some("here"), Some(456));
        let patch = PersonPatch::new(None, Patch::Null, Patch::Null);
        assert!(aggregator.insert(&tx, 1, &person1).is_ok());
        assert!(aggregator.insert(&tx, 2, &person2).is_ok());
        assert!(aggregator.update(&tx, 2, &person2, &patch).is_ok());

        check_record(&tx, "here", Some(LocationData::new(1, 0)));
        check_events(&tx, &[
            r#"{"here":{"total":1}}"#, // TODO: Should initial value of "married" be 0 ?
            r#"{"here":{"total":2,"married":1}}"#,
            r#"{"here":{"total":1,"married":0}}"#]);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_update_remove_last_location() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", Some("here"), Some(123));
        let patch = PersonPatch::new(None, Patch::Null, Patch::Absent);
        assert!(aggregator.insert(&tx, 1, &person).is_ok());
        assert!(aggregator.update(&tx, 1, &person, &patch).is_ok());

        check_record(&tx, "here", None);
        check_events(&tx, &[r#"{"here":{"total":1,"married":1}}"#, r#"{"here":null}"#]);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_delete() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        let person1 = PersonData::new("Hans", Some("here"), None);
        let person2 = PersonData::new("Inge", Some("here"), Some(123));
        assert!(aggregator.insert(&tx, 1, &person1).is_ok());
        assert!(aggregator.insert(&tx, 2, &person2).is_ok());
        assert!(aggregator.delete(&tx, 2, &person2).is_ok());

        check_record(&tx, "here", Some(LocationData::new(1, 0)));
        check_events(&tx, &[
            r#"{"here":{"total":1}}"#,
            r#"{"here":{"total":2,"married":1}}"#,
            r#"{"here":{"total":1,"married":0}}"#]);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_delete_last() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", Some("here"), Some(123));
        assert!(aggregator.insert(&tx, 1, &person).is_ok());
        assert!(aggregator.delete(&tx, 1, &person).is_ok());

        check_record(&tx, "here", None);
        check_events(&tx, &[
            r#"{"here":{"total":1,"married":1}}"#,
            r#"{"here":null}"#]);
        assert!(tx.commit().is_ok());
    }

    //
    // Helper functions for test
    //

    fn create_aggregator() -> LocationAggregator {
        let timestamp = IncrementalTimestamp::new();
        LocationAggregator::new_internal(timestamp)
    }

    fn create_connection() -> Connection {
        let connection = Connection::open(":memory:");
        assert!(connection.is_ok());
        let connection = connection.unwrap();
        assert!(LocationTable::create_table(&connection).is_ok());
        assert!(LocationEventTable::create_table(&connection).is_ok());
        assert!(RevisionTable::create_table(&connection).is_ok());
        connection
    }

    fn check_record(tx: &Transaction, name: &str, loc_ref: Option<LocationData>) {
        let loc_res = LocationTable::select_by_name(&tx, name);
        assert!(loc_res.is_ok());
        let loc_res = loc_res.unwrap();
        assert_eq!(loc_res, loc_ref);
    }

    fn check_events(tx: &Transaction, events_ref: &[&str]) {
        compare_revision(tx, RevisionType::LOCATION, events_ref.len());
        compare_events(LocationEventTable::read(tx, 0), events_ref);
    }

    fn compare_revision(tx: &Transaction, revision_type: RevisionType, revision_ref: usize) {
        let revision = RevisionTable::read(&tx, revision_type);
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), revision_ref as u32);
    }

    fn compare_events(events: Result<Vec<String>>, events_ref: &[&str]) {
        assert!(events.is_ok());
        let events = events.unwrap();
        assert_eq!(events.len(), events_ref.len());
        for (index, &event_ref) in events_ref.iter().enumerate() {
            let event = events.get(index);
            assert!(event.is_some());
            let event = event.unwrap();
            assert_eq!(event, event_ref);
        }
    }
}