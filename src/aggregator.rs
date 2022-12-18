use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use log::{debug, info, warn};
use rusqlite::{Connection, Transaction};
use crate::database::event_table::{LocationEventTable, PersonEventTable};
use crate::database::location_view::LocationView;
use crate::database::person_table::PersonTable;
use crate::database::revision_table::{RevisionTable, RevisionType};
use crate::domain::location_event_builder::LocationEventBuilder;
use crate::domain::location_map::LocationMap;
use crate::domain::person_data::PersonData;
use crate::domain::person_event_builder::PersonEventBuilder;
use crate::domain::person_map::PersonMap;
use crate::domain::person_patch::PersonPatch;

pub struct Aggregator {
    conn: Connection
}

pub type MutexAggregator = Arc<Mutex<Aggregator>>;

impl Aggregator {
    pub fn new(db_path: &str) -> Result<Self, Box<dyn Error>> {
        let conn = Connection::open(db_path)?;
        PersonTable::create_table(&conn)?;
        PersonEventTable::create_table(&conn)?;
        LocationEventTable::create_table(&conn)?;
        RevisionTable::create_table(&conn)?;
        Ok(Self{ conn })
    }

    pub fn insert<'a>(&mut self, person: &'a PersonData) -> Result<(u32, &'a PersonData), Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        let person_id = PersonTable::insert(&tx, &person)?;
        // Create and write events and their revisions
        let event = PersonEventBuilder::for_insert(person_id, &person);
        Self::write_person_event_and_revision(&tx, event)?;
        let event = LocationEventBuilder::for_insert(person_id, &person);
        Self::write_location_event_and_revision(&tx, event)?;
        tx.commit()?;
        info!("Created {:?}", person);
        Ok((person_id, person))
    }

    pub fn update(&mut self, person_id: u32, patch: &PersonPatch) -> Result<Option<PersonData>, rusqlite::Error> {
        let tx = self.conn.transaction()?;
        match PersonTable::select_by_id(&tx, person_id)? {
            Some(before) => {
                let after = PersonTable::update(&tx, person_id, &patch)?.unwrap();
                // Create and write events and their revisions
                let event = PersonEventBuilder::for_update(person_id, &before, &after);
                Self::write_person_event_and_revision(&tx, event)?;
                let is_last = Self::is_last_location(&tx, &before)?;
                let event = LocationEventBuilder::for_update(person_id, &before, &after, is_last);
                Self::write_location_event_and_revision(&tx, event)?;
                tx.commit()?;
                info!("Updated {:?} from {:?}", before, patch);
                Ok(Some(after))
            },
            None => {
                tx.rollback()?; // There should be no changes, so tx.commit() would also work
                warn!("Person {} not found", person_id);
                Ok(None)
            }
        }
    }

    pub fn delete(&mut self, person_id: u32) -> Result<bool, Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        match PersonTable::select_by_id(&tx, person_id)? {
            Some(before) => {
                PersonTable::delete(&tx, person_id)?;
                // Create and write events and their revisions
                let event = PersonEventBuilder::for_delete(person_id);
                Self::write_person_event_and_revision(&tx, event)?;
                let is_last = Self::is_last_location(&tx, &before)?;
                let event = LocationEventBuilder::for_delete(person_id, &before, is_last);
                Self::write_location_event_and_revision(&tx, event)?;
                tx.commit()?;
                info!("Deleted {:?}", before);
                Ok(true)
            },
            None => {
                tx.rollback()?; // There should be no changes, so tx.commit() would also work
                warn!("Person {} not found", person_id);
                Ok(false)
            }
        }
    }

    pub fn get_persons(&mut self) -> Result<(u32, PersonMap), Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        let revision = RevisionTable::read(&tx, RevisionType::PERSON)?;
        let persons = PersonTable::select_all(&tx)?;
        tx.commit()?;
        Ok((revision, persons))
    }

    pub fn get_person_events(&mut self, from_revision: u32) -> Result<Vec<String>, Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        let events = PersonEventTable::read(&tx, from_revision)?;
        tx.commit()?;
        Ok(events)
    }

    // TODO: Use streams (for all collection results)
    pub fn get_locations(&mut self) -> Result<(u32, LocationMap), Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        let revision = RevisionTable::read(&tx, RevisionType::LOCATION)?;
        let locations = LocationView::select_all(&tx)?;
        tx.commit()?;
        Ok((revision, locations))
    }

    pub fn get_location_events(&mut self, from_revision: u32) -> Result<Vec<String>, Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        let events = LocationEventTable::read(&tx, from_revision)?;
        tx.commit()?;
        Ok(events)
    }

    // TODO: Unit test
    pub fn delete_events_before(&mut self, timestamp: &SystemTime) -> Result<usize, Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        let timestamp = timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs();
        let count = PersonEventTable::delete_before(&tx, timestamp)?;
        tx.commit()?;
        debug!("Deleted {} old events", count);
        Ok(count)
    }

    //
    // Private functions
    //

    fn is_last_location(tx: &Transaction, person: &PersonData) -> Result<bool, rusqlite::Error> {
        match person.location.as_ref() {
            Some(location) => Ok(!PersonTable::exists_location(tx, location)?),
            None => Ok(false)
        }
    }

    fn write_person_event_and_revision(tx: &Transaction, event: Option<String>) -> Result<(), rusqlite::Error> {
        if event.is_some() {
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            let revision = PersonEventTable::insert(&tx, timestamp, event.unwrap().as_str())?;
            RevisionTable::upsert(&tx, RevisionType::PERSON, revision)?;
        }
        Ok(())
    }

    fn write_location_event_and_revision(tx: &Transaction, event: Option<String>) -> Result<(), rusqlite::Error> {
        if event.is_some() {
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            let revision = LocationEventTable::insert(&tx, timestamp, event.unwrap().as_str())?;
            RevisionTable::upsert(&tx, RevisionType::LOCATION, revision)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::thread;
    use std::time::{Duration, SystemTime};
    use crate::aggregator::Aggregator;
    use crate::database::revision_table::{RevisionTable, RevisionType};
    use crate::domain::person_data::PersonData;
    use crate::domain::person_map::PersonMap;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;

    //
    // Test insert/update/delete
    //

    #[test]
    pub fn test_insert() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", None, None);
        let person_res = aggregator.insert(&person);
        assert!(person_res.is_ok());
        let (person_id, person_data) = person_res.unwrap();
        assert_eq!(person_id, 1);
        assert_eq!(person_data, &person);
    }

    #[test]
    pub fn test_update() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", None, None);
        let patch = PersonPatch::new(Some("Inge"), Patch::Value("Here"), Patch::Value(123));
        assert!(aggregator.insert(&person).is_ok());
        let person_res = aggregator.update(1, &patch);
        assert!(person_res.is_ok());

        let person_ref = PersonData::new("Inge", Some("Here"), Some(123));
        assert_eq!(person_res.unwrap(), Some(person_ref));
    }

    #[test]
    pub fn test_update_missing() {
        let mut aggregator = create_aggregator();

        let person_update = PersonPatch::new(Some("Inge"), Patch::Value("Nowhere"), Patch::Null);
        let person_res = aggregator.update(1, &person_update);
        assert!(person_res.is_ok());
        assert_eq!(person_res.unwrap(), None);
    }

    #[test]
    pub fn test_delete() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", None, None);
        assert!(aggregator.insert(&person).is_ok());
        let person_res = aggregator.delete(1);
        assert!(person_res.is_ok());
        assert!(person_res.unwrap()); // Should be true

        let events_ref = [r#"{"1":{"name":"Hans"}}"#, r#"{"1":null}"#];
        check_person_events(&mut aggregator, &events_ref);
    }

    //
    // Test events produced during insert/update/delete
    //

    #[test]
    pub fn test_insert_events_no_location() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", None, None);
        assert!(aggregator.insert(&person).is_ok());

        let events_ref = [r#"{"1":{"name":"Hans"}}"#];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_insert_events_with_location() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", Some("Here"), None);
        assert!(aggregator.insert(&person).is_ok());

        let events_ref = [r#"{"1":{"name":"Hans","location":"Here"}}"#];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [r#"{"Here":{"1":{"name":"Hans","location":"Here"}}}"#];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_update_events_no_change() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = PersonPatch::new(Some("Hans"), Patch::Absent, Patch::Absent);
        assert!(aggregator.insert(&person).is_ok());
        assert!(aggregator.update(1, &patch).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","location":"Here"}}"# // No update event because nothing was changed
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [
            r#"{"Here":{"1":{"name":"Hans","location":"Here"}}}"# // Ditto
        ];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_update_events_set_location() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", None, None);
        let patch = PersonPatch::new(Some("Hans"), Patch::Value("Here"), Patch::Value(123));
        assert!(aggregator.insert(&person).is_ok());
        assert!(aggregator.update(1, &patch).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans"}}"#,
            r#"{"1":{"location":"Here","spouseId":123}}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [
            r#"{"Here":{"1":{"name":"Hans","location":"Here","spouseId":123}}}"#
        ];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_update_events_no_location() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", None, None);
        let patch = PersonPatch::new(Some("Hans"), Patch::Absent, Patch::Value(123));
        assert!(aggregator.insert(&person).is_ok());
        assert!(aggregator.update(1, &patch).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans"}}"#,
            r#"{"1":{"spouseId":123}}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_update_events_keep_location() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = PersonPatch::new(Some("Inge"), Patch::Absent, Patch::Absent);
        assert!(aggregator.insert(&person).is_ok());
        assert!(aggregator.update(1, &patch).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","location":"Here"}}"#,
            r#"{"1":{"name":"Inge"}}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [
            r#"{"Here":{"1":{"name":"Hans","location":"Here"}}}"#,
            r#"{"Here":{"1":{"name":"Inge"}}}"#
        ];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_update_events_same_location() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = PersonPatch::new(Some("Hans"), Patch::Value("Here"), Patch::Value(123));
        assert!(aggregator.insert(&person).is_ok());
        assert!(aggregator.update(1, &patch).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","location":"Here"}}"#,
            r#"{"1":{"spouseId":123}}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [
            r#"{"Here":{"1":{"name":"Hans","location":"Here"}}}"#,
            r#"{"Here":{"1":{"spouseId":123}}}"#,
        ];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_update_events_change_one_location() {
        let mut aggregator = create_aggregator();

        let person1 = PersonData::new("Hans", Some("Here"), None);
        let person2 = PersonData::new("Inge", Some("Here"), None);
        let patch1 = PersonPatch::new(Some("Hans"), Patch::Value("There"), Patch::Absent);
        assert!(aggregator.insert(&person1).is_ok());
        assert!(aggregator.insert(&person2).is_ok());
        assert!(aggregator.update(1, &patch1).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","location":"Here"}}"#,
            r#"{"2":{"name":"Inge","location":"Here"}}"#,
            r#"{"1":{"location":"There"}}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [
            r#"{"Here":{"1":{"name":"Hans","location":"Here"}}}"#,
            r#"{"Here":{"2":{"name":"Inge","location":"Here"}}}"#,
            r#"{"Here":{"1":null},"There":{"1":{"name":"Hans","location":"There"}}}"#,
        ];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_update_events_change_last_location() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = PersonPatch::new(Some("Hans"), Patch::Value("There"), Patch::Absent);
        assert!(aggregator.insert(&person).is_ok());
        assert!(aggregator.update(1, &patch).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","location":"Here"}}"#,
            r#"{"1":{"location":"There"}}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [
            r#"{"Here":{"1":{"name":"Hans","location":"Here"}}}"#,
            r#"{"Here":null,"There":{"1":{"name":"Hans","location":"There"}}}"#,
        ];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_update_events_remove_one_location() {
        let mut aggregator = create_aggregator();

        let person1 = PersonData::new("Hans", Some("Here"), None);
        let person2 = PersonData::new("Fred", Some("Here"), None);
        let patch1 = PersonPatch::new(Some("Hans"), Patch::Null, Patch::Absent);
        assert!(aggregator.insert(&person1).is_ok());
        assert!(aggregator.insert(&person2).is_ok());
        assert!(aggregator.update(1, &patch1).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","location":"Here"}}"#,
            r#"{"2":{"name":"Fred","location":"Here"}}"#,
            r#"{"1":{"location":null}}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [
            r#"{"Here":{"1":{"name":"Hans","location":"Here"}}}"#,
            r#"{"Here":{"2":{"name":"Fred","location":"Here"}}}"#,
            r#"{"Here":{"1":null}}"#
        ];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_update_events_remove_last_location() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = PersonPatch::new(Some("Hans"), Patch::Null, Patch::Absent);
        assert!(aggregator.insert(&person).is_ok());
        assert!(aggregator.update(1, &patch).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","location":"Here"}}"#,
            r#"{"1":{"location":null}}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [
            r#"{"Here":{"1":{"name":"Hans","location":"Here"}}}"#,
            r#"{"Here":null}"#
        ];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_update_events_missing() {
        let mut aggregator = create_aggregator();

        let person_update = PersonPatch::new(Some("Inge"), Patch::Value("Nowhere"), Patch::Null);
        assert!(aggregator.update(1, &person_update).is_ok());

        check_person_events(&mut aggregator, &[]);
        check_location_events(&mut aggregator, &[]);
    }

    #[test]
    pub fn test_delete_events_remove_one_location() {
        let mut aggregator = create_aggregator();

        let person1 = PersonData::new("Hans", Some("Here"), None);
        let person2 = PersonData::new("Inge", Some("Here"), None);
        assert!(aggregator.insert(&person1).is_ok());
        assert!(aggregator.insert(&person2).is_ok());
        assert!(aggregator.delete(1).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","location":"Here"}}"#,
            r#"{"2":{"name":"Inge","location":"Here"}}"#,
            r#"{"1":null}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [
            r#"{"Here":{"1":{"name":"Hans","location":"Here"}}}"#,
            r#"{"Here":{"2":{"name":"Inge","location":"Here"}}}"#,
            r#"{"Here":{"1":null}}"#
        ];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_delete_events_remove_last_location() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", Some("Here"), None);
        assert!(aggregator.insert(&person).is_ok());
        assert!(aggregator.delete(1).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","location":"Here"}}"#,
            r#"{"1":null}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [
            r#"{"Here":{"1":{"name":"Hans","location":"Here"}}}"#,
            r#"{"Here":null}"#
        ];
        check_location_events(&mut aggregator, &events_ref);
    }

    //
    // Test read operations
    //

    #[test]
    pub fn test_get_aggregates_empty() {
        let mut aggregator = create_aggregator();

        let persons_res = aggregator.get_persons();
        assert!(persons_res.is_ok());

        let person_ref = (0, PersonMap::new());
        assert_eq!(persons_res.unwrap(), person_ref);
    }

    #[test]
    pub fn test_get_aggregates() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", None, None);
        assert!(aggregator.insert(&person).is_ok());
        let persons_res = aggregator.get_persons();
        assert!(persons_res.is_ok());

        let mut person_map = PersonMap::new();
        person_map.put(1, PersonData::new("Hans", None, None));
        let person_ref = (1, person_map);
        assert_eq!(persons_res.unwrap(), person_ref);
    }

    #[test]
    pub fn test_get_events() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", None, None);
        let patch = PersonPatch::new(Some("Inge"), Patch::Value("Nowhere"), Patch::Value(5));
        assert!(aggregator.insert(&person).is_ok());
        assert!(aggregator.update(1, &patch).is_ok());

        let event_ref1 = r#"{"1":{"name":"Hans"}}"#;
        let event_ref2 = r#"{"1":{"name":"Inge","location":"Nowhere","spouseId":5}}"#;
        get_person_events_and_compare(&mut aggregator, 0, &[&event_ref1, &event_ref2]);
        get_person_events_and_compare(&mut aggregator, 1, &[&event_ref1, &event_ref2]);
        get_person_events_and_compare(&mut aggregator, 2, &[&event_ref2]);
        get_person_events_and_compare(&mut aggregator, 3, &[]);
    }

    #[test]
    pub fn test_delete_events_before() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", None, None);
        let patch = PersonPatch::new(Some("Inge"), Patch::Value("Nowhere"), Patch::Value(5));
        assert!(aggregator.insert(&person).is_ok());
        thread::sleep(Duration::from_millis(1010)); // Need to sleep >1sec because of Unix time resolution
        let timestamp = SystemTime::now();
        thread::sleep(Duration::from_millis(10));
        assert!(aggregator.update(1, &patch).is_ok());

        let result = aggregator.delete_events_before(&timestamp);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);

        let event_ref = r#"{"1":{"name":"Inge","location":"Nowhere","spouseId":5}}"#;
        get_person_events_and_compare(&mut aggregator, 0, &[&event_ref]);
    }

    //
    // Helper functions for test
    //

    fn create_aggregator() -> Aggregator {
        let aggregator = Aggregator::new(":memory:");
        assert!(aggregator.is_ok());
        aggregator.unwrap()
    }

    fn get_person_events_and_compare(aggregator: &mut Aggregator, from_revision: u32, ref_events: &[&str]) {
        let events = aggregator.get_person_events(from_revision);
        assert!(events.is_ok());
        let events = events.unwrap();
        assert_eq!(events.len(), ref_events.len());
        for (index, &ref_event) in ref_events.iter().enumerate() {
            assert_eq!(events[index], *ref_event);
        }
    }

    fn check_person_events(aggregator: &mut Aggregator, events_ref: &[&str]) {
        check_revision(aggregator, RevisionType::PERSON, events_ref.len());
        let events = aggregator.get_person_events(0);
        check_events(events, events_ref);
    }

    fn check_location_events(aggregator: &mut Aggregator, events_ref: &[&str]) {
        check_revision(aggregator, RevisionType::LOCATION, events_ref.len());
        let events = aggregator.get_location_events(0);
        check_events(events, events_ref);
    }

    fn check_revision(aggregator: &mut Aggregator, revision_type: RevisionType, revision_ref: usize) {
        let tx = aggregator.conn.transaction().unwrap();
        let revision = RevisionTable::read(&tx, revision_type);
        assert!(tx.commit().is_ok());
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), revision_ref as u32);
    }

    fn check_events(events: Result<Vec<String>, Box<dyn Error>>, events_ref: &[&str]) {
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