use std::time::Duration;
use rusqlite::{Connection, Result, Transaction};
use crate::aggregator::aggregator_trait::AggregatorTrait;
use crate::database::event_table::LocationEventTable;
use crate::database::location_table::LocationTable;
use crate::database::revision_table::RevisionTable;
use crate::domain::event_type::EventType;
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

    ///
    /// Private method that performs an upsert for the ``location`` table, creates the
    /// corresponding [LocationEvent](crate::domain::location_event::LocationEvent),
    /// writes it to database, and increments the revision number.
    ///
    fn upsert(&mut self, tx: &Transaction, name: &str, mut data: LocationData, patch: LocationPatch) -> Result<()> {
        data.apply_patch(&patch);
        LocationTable::upsert(tx, name, &data)?;
        let event = LocationEvent::new(name, Some(patch));
        self.write_event_and_revision(tx, event)
    }

    ///
    /// Private method that performs an update or a delete for the ``location`` table.
    /// The deletion is chosen if the no further persons with the given location ``name``
    /// exist (the ``total`` counter became 0). The method then creates the corresponding
    /// [LocationEvent](crate::domain::location_event::LocationEvent),
    /// writes it to database, and increments the revision number.
    ///
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
        RevisionTable::upsert(&tx, EventType::LOCATION, revision)
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

    fn get_all(&mut self, tx: &Transaction) -> Result<(usize, Self::Records)> {
        let revision = RevisionTable::read(&tx, EventType::LOCATION)?;
        let locations = LocationTable::select_all(&tx)?;
        Ok((revision, locations))
    }

    fn get_events(&mut self, tx: &Transaction, from_revision: usize) -> Result<Vec<String>> {
        LocationEventTable::read(&tx, from_revision)
    }

    fn delete_events(&mut self, tx: &Transaction, created_before: Duration) -> Result<usize> {
        let created_before = self.timestamp.as_secs() - created_before.as_secs();
        LocationEventTable::delete_before(&tx, created_before)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use rusqlite::{Connection, Transaction};
    use crate::aggregator::aggregator_trait::AggregatorTrait;
    use crate::aggregator::location_aggregator::LocationAggregator;
    use crate::aggregator::person_aggregator::tests::{compare_events, compare_revision};
    use crate::database::event_table::LocationEventTable;
    use crate::database::location_table::LocationTable;
    use crate::database::revision_table::RevisionTable;
    use crate::domain::event_type::EventType;
    use crate::domain::location_data::LocationData;
    use crate::domain::location_map::LocationMap;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;
    use crate::util::timestamp::tests::IncrementalTimestamp;

    //
    // Test aggregation functions
    //

    // Runs LocationAggregator::insert() for variants of input data
    fn test_insert(persons: &[PersonData], record_ref: Option<LocationData>, events_ref: &[&str]) {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        for person in persons {
            assert!(aggregator.insert(&tx, 1, &person).is_ok());
        }

        check_record(&tx, "here", record_ref);
        check_events(&tx, events_ref);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_insert_with_spouse() {
        test_insert(
            &[PersonData::new("Hans", Some("here"), Some(123))],
            Some(LocationData::new(1, 1)),
            &[r#"{"here":{"total":1,"married":1}}"#]);
    }

    #[test]
    pub fn test_insert_no_spouse() {
        test_insert(
            &[PersonData::new("Hans", Some("here"), None)],
            Some(LocationData::new(1, 0)),
            &[r#"{"here":{"total":1,"married":0}}"#]);
    }

    #[test]
    pub fn test_insert_twice_no_spouse() {
        test_insert(
            &[
                PersonData::new("Hans", Some("here"), None),
                PersonData::new("Inge", Some("here"), None)],
            Some(LocationData::new(2, 0)),
            &[
                r#"{"here":{"total":1,"married":0}}"#,
                r#"{"here":{"total":2}}"#]);
    }

    // Runs LocationAggregator::insert() followed by LocationAggregator::update() for variants of input data
    fn test_update(persons: &[PersonData], patch: PersonPatch, record_ref: Option<LocationData>, events_ref: &[&str]) {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        for person in persons {
            assert!(aggregator.insert(&tx, 1, &person).is_ok());
        }
        assert!(aggregator.update(&tx, 1, &persons.last().unwrap(), &patch).is_ok());

        check_record(&tx, "here", record_ref);
        check_events(&tx, events_ref);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_update_keep_location_keep_spouse() {
        test_update(
            &[PersonData::new("Hans", Some("here"), Some(123))],
            PersonPatch::new(None, Patch::Absent, Patch::Absent),
            Some(LocationData::new(1, 1)),
            &[r#"{"here":{"total":1,"married":1}}"#]); // No update event
    }

    #[test]
    pub fn test_update_keep_location_set_spouse() {
        test_update(
            &[PersonData::new("Hans", Some("here"), None)],
            PersonPatch::new(None, Patch::Absent, Patch::Value(123)),
            Some(LocationData::new(1, 1)),
            &[
                r#"{"here":{"total":1,"married":0}}"#,
                r#"{"here":{"married":1}}"#]);
    }

    #[test]
    pub fn test_update_keep_location_delete_spouse() {
        test_update(
            &[PersonData::new("Hans", Some("here"), Some(123))],
            PersonPatch::new(None, Patch::Absent, Patch::Null),
            Some(LocationData::new(1, 0)),
            &[
                r#"{"here":{"total":1,"married":1}}"#,
                r#"{"here":{"married":0}}"#]);
    }

    #[test]
    pub fn test_update_set_location_keep_spouse() {
        test_update(
            &[PersonData::new("Hans", None, Some(123))],
            PersonPatch::new(None, Patch::Value("here"), Patch::Absent),
            Some(LocationData::new(1, 1)),
            &[r#"{"here":{"total":1,"married":1}}"#]);
    }

    #[test]
    pub fn test_update_set_location_set_spouse() {
        test_update(
            &[PersonData::new("Hans", None, None)],
            PersonPatch::new(None, Patch::Value("here"), Patch::Value(123)),
            Some(LocationData::new(1, 1)),
            &[r#"{"here":{"total":1,"married":1}}"#]);
    }

    #[test]
    pub fn test_update_set_location_delete_spouse() {
        test_update(
            &[PersonData::new("Hans", None, Some(123))],
            PersonPatch::new(None, Patch::Value("here"), Patch::Null),
            Some(LocationData::new(1, 0)),
            &[r#"{"here":{"total":1,"married":0}}"#]);
    }

    #[test]
    pub fn test_update_remove_location_keep_spouse() {
        test_update(
            &[
                PersonData::new("Hans", Some("here"), None),
                PersonData::new("Inge", Some("here"), Some(456))],
            PersonPatch::new(None, Patch::Null, Patch::Absent),
            Some(LocationData::new(1, 0)),
            &[
                r#"{"here":{"total":1,"married":0}}"#,
                r#"{"here":{"total":2,"married":1}}"#,
                r#"{"here":{"total":1,"married":0}}"#]);
    }

    #[test]
    pub fn test_update_remove_location_remove_spouse() {
        test_update(
            &[
                PersonData::new("Hans", Some("here"), None),
                PersonData::new("Inge", Some("here"), Some(456))],
            PersonPatch::new(None, Patch::Null, Patch::Null),
            Some(LocationData::new(1, 0)),
            &[
                r#"{"here":{"total":1,"married":0}}"#,
                r#"{"here":{"total":2,"married":1}}"#,
                r#"{"here":{"total":1,"married":0}}"#]);
     }

    #[test]
    pub fn test_update_remove_last_location() {
        test_update(
            &[PersonData::new("Hans", Some("here"), Some(123))],
            PersonPatch::new(None, Patch::Null, Patch::Absent),
            None,
            &[
                r#"{"here":{"total":1,"married":1}}"#,
                r#"{"here":null}"#]);
    }

    #[test]
    pub fn test_update_change_location_keep_spouse() {
        test_update(
            &[
                PersonData::new("Hans", Some("there"), None),
                PersonData::new("Inge", Some("there"), Some(123))],
            PersonPatch::new(None, Patch::Value("here"), Patch::Absent),
            Some(LocationData::new(1, 1)),
            &[
                r#"{"there":{"total":1,"married":0}}"#,
                r#"{"there":{"total":2,"married":1}}"#,
                r#"{"there":{"total":1,"married":0}}"#,
                r#"{"here":{"total":1,"married":1}}"#]);
    }

    #[test]
    pub fn test_update_change_last_location_keep_spouse() {
        test_update(
            &[PersonData::new("Hans", Some("there"), Some(123))],
            PersonPatch::new(None, Patch::Value("here"), Patch::Absent),
            Some(LocationData::new(1, 1)),
            &[
                r#"{"there":{"total":1,"married":1}}"#,
                r#"{"there":null}"#,
                r#"{"here":{"total":1,"married":1}}"#]);
    }

    #[test]
    pub fn test_update_change_location_set_spouse() {
        test_update(
            &[
                PersonData::new("Hans", Some("there"), None),
                PersonData::new("Inge", Some("there"), None)],
            PersonPatch::new(None, Patch::Value("here"), Patch::Value(123)),
            Some(LocationData::new(1, 1)),
            &[
                r#"{"there":{"total":1,"married":0}}"#,
                r#"{"there":{"total":2}}"#,
                r#"{"there":{"total":1}}"#,
                r#"{"here":{"total":1,"married":1}}"#]);
    }

    #[test]
    pub fn test_update_change_last_location_set_spouse() {
        test_update(
            &[PersonData::new("Hans", Some("there"), None)],
            PersonPatch::new(None, Patch::Value("here"), Patch::Value(123)),
            Some(LocationData::new(1, 1)),
            &[
                r#"{"there":{"total":1,"married":0}}"#,
                r#"{"there":null}"#,
                r#"{"here":{"total":1,"married":1}}"#]);
    }

    #[test]
    pub fn test_update_change_location_delete_spouse() {
        test_update(
            &[
                PersonData::new("Hans", Some("there"), None),
                PersonData::new("Inge", Some("there"), Some(123))],
            PersonPatch::new(None, Patch::Value("here"), Patch::Null),
            Some(LocationData::new(1, 0)),
            &[
                r#"{"there":{"total":1,"married":0}}"#,
                r#"{"there":{"total":2,"married":1}}"#,
                r#"{"there":{"total":1,"married":0}}"#,
                r#"{"here":{"total":1,"married":0}}"#]);
    }

    #[test]
    pub fn test_update_change_last_location_delete_spouse() {
        test_update(
            &[PersonData::new("Hans", Some("there"), Some(123))],
            PersonPatch::new(None, Patch::Value("here"), Patch::Null),
            Some(LocationData::new(1, 0)),
            &[
                r#"{"there":{"total":1,"married":1}}"#,
                r#"{"there":null}"#,
                r#"{"here":{"total":1,"married":0}}"#]);
    }

    // Runs LocationAggregator::insert() followed by LocationAggregator::delete() for variants of input data
    fn test_delete(persons: &[PersonData], record_ref: Option<LocationData>, events_ref: &[&str]) {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();
        let mut aggregator = create_aggregator();

        for person in persons {
            assert!(aggregator.insert(&tx, 1, &person).is_ok());
        }
        assert!(aggregator.delete(&tx, 1, &persons.last().unwrap()).is_ok());

        check_record(&tx, "here", record_ref);
        check_events(&tx, events_ref);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_delete_not_last() {
        test_delete(
            &[
                PersonData::new("Hans", Some("here"), None),
                PersonData::new("Inge", Some("here"), Some(123))
            ],
            Some(LocationData::new(1, 0)),
            &[
                r#"{"here":{"total":1,"married":0}}"#,
                r#"{"here":{"total":2,"married":1}}"#,
                r#"{"here":{"total":1,"married":0}}"#]);
    }

    #[test]
    pub fn test_delete_last() {
        test_delete(
            &[PersonData::new("Hans", Some("here"), Some(123))],
            None,
            &[
                r#"{"here":{"total":1,"married":1}}"#,
                r#"{"here":null}"#]);
    }

    //
    // Test read operations
    //

    #[test]
    pub fn test_get_all() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();

        let loc = LocationData::new(1, 3);
        assert!(LocationTable::upsert(&tx, "here", &loc).is_ok());
        assert!(RevisionTable::upsert(&tx, EventType::LOCATION, 2).is_ok());

        let mut aggregator = create_aggregator();
        let loc_res = aggregator.get_all(&tx);
        assert!(loc_res.is_ok());

        let mut loc_map = LocationMap::new();
        loc_map.put("here", LocationData::new(1, 3));
        let loc_ref = (2, loc_map);
        assert_eq!(loc_res.unwrap(), loc_ref);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_get_all_empty() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();

        let mut aggregator = create_aggregator();
        let loc_res = aggregator.get_all(&tx);
        assert!(loc_res.is_ok());

        let loc_ref = (0, LocationMap::new());
        assert_eq!(loc_res.unwrap(), loc_ref);
        assert!(tx.commit().is_ok());
    }

    //
    // Test event-related functions
    //

    #[test]
    pub fn test_get_events() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();

        let person = PersonData::new("Hans", Some("here"), None);
        let patch = PersonPatch::new(None, Patch::Absent, Patch::Value(123));
        let mut aggregator = create_aggregator();
        assert!(aggregator.insert(&tx, 1, &person).is_ok());
        assert!(aggregator.update(&tx, 1, &person, &patch).is_ok());

        let event_ref1 = r#"{"here":{"total":1,"married":0}}"#;
        let event_ref2 = r#"{"here":{"married":1}}"#;
        get_events_and_compare(&tx, 0, &[&event_ref1, &event_ref2]);
        get_events_and_compare(&tx, 1, &[&event_ref1, &event_ref2]);
        get_events_and_compare(&tx, 2, &[&event_ref2]);
        get_events_and_compare(&tx, 3, &[]);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_delete_events() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();

        let person1 = PersonData::new("Hans", Some("here"), None);
        let person2 = PersonData::new("Inge", Some("there"), None);
        let patch2 = PersonPatch::new(None, Patch::Value("here"), Patch::Value(123));
        let mut aggregator = create_aggregator();
        assert!(aggregator.insert(&tx, 1, &person1).is_ok());
        assert!(aggregator.insert(&tx, 2, &person2).is_ok());
        assert!(aggregator.update(&tx, 2, &person2, &patch2).is_ok());

        // IncrementalTimestamp is at 5 inside delete_events() below (note that  update()
        // creates two events; minus 2 yields 3, so it deletes all events <3 (i.e. the first two)
        // and keeps the last two
        let result = aggregator.delete_events(&tx, Duration::from_secs(2));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2); // Two events deleted

        get_events_and_compare(&tx, 0, &[
            r#"{"there":null}"#,
            r#"{"here":{"total":2,"married":1}}"#]);
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

    fn get_events_and_compare(tx: &Transaction, from_revision: usize, ref_events: &[&str]) {
        let mut aggregator = create_aggregator();
        let events = aggregator.get_events(&tx, from_revision);
        assert!(events.is_ok());
        let events = events.unwrap();
        assert_eq!(events.len(), ref_events.len());
        for (index, &ref_event) in ref_events.iter().enumerate() {
            assert_eq!(events[index], *ref_event);
        }
    }

    fn check_record(tx: &Transaction, name: &str, loc_ref: Option<LocationData>) {
        let loc_res = LocationTable::select_by_name(&tx, name);
        assert!(loc_res.is_ok());
        let loc_res = loc_res.unwrap();
        assert_eq!(loc_res, loc_ref);
    }

    fn check_events(tx: &Transaction, events_ref: &[&str]) {
        compare_revision(tx, EventType::LOCATION, events_ref.len());
        compare_events(LocationEventTable::read(tx, 0), events_ref);
    }
}