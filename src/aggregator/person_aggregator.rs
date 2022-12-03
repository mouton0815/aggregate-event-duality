// TODO: Rename to aggregator.js
use std::error::Error;
use log::{info, warn};
use rusqlite::{Connection, Transaction};
use crate::database::event_table::{LocationEventTable, PersonEventTable};
use crate::database::location_view::LocationView;
use crate::database::person_table::PersonTable;
use crate::database::revision_table::{RevisionTable, RevisionType};
use crate::domain::location_event::LocationEvent;
use crate::domain::location_map::LocationMap;
use crate::domain::person_data::PersonData;
use crate::domain::person_event::PersonEvent;
use crate::domain::person_map::PersonMap;
use crate::domain::person_patch::PersonPatch;
use crate::util::patch::Patch;

pub struct PersonAggregator {
    conn: Connection
}

impl PersonAggregator {
    pub fn new(db_path: &str) -> Result<PersonAggregator, Box<dyn Error>> {
        let conn = Connection::open(db_path)?;
        PersonTable::create_table(&conn)?;
        PersonEventTable::create_table(&conn)?;
        LocationEventTable::create_table(&conn)?;
        RevisionTable::create_table(&conn)?;
        Ok(PersonAggregator { conn })
    }

    pub fn insert(&mut self, person: &PersonData) -> Result<(u32, PersonData), Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        let person_id = PersonTable::insert(&tx, &person)?;
        // Create and write events and their revisions
        Self::write_person_event_for_insert(&tx, person_id, person)?;
        Self::write_location_event_for_insert(&tx, person_id, person)?;
        // Select updated company aggregate for returning
        let aggregate = PersonTable::select_by_id(&tx, person_id)?.unwrap(); // Must exist
        tx.commit()?;
        info!("Created {:?} from {:?}", aggregate, person);
        Ok((person_id, aggregate))
    }

    pub fn update(&mut self, person_id: u32, person: &PersonPatch) -> Result<Option<PersonData>, rusqlite::Error> {
        let tx = self.conn.transaction()?;
        let old_location = LocationView::select_by_person(&tx, person_id)?;
        if PersonTable::update(&tx, person_id, &person)? {
            Self::write_person_event_for_update(&tx, person_id, person)?;
            Self::write_location_event_for_update(&tx, person_id, person, old_location)?;
            // Select updated company aggregate for returning (must exist)
            let aggregate = PersonTable::select_by_id(&tx, person_id)?.unwrap();
            tx.commit()?;
            info!("Updated {:?} from {:?}", aggregate, person);
            Ok(Some(aggregate))
        } else {
            tx.rollback()?; // There should be no changes, so tx.commit() would also work
            warn!("Person aggregate {} not found", person_id);
            Ok(None)
        }
    }

    pub fn delete(&mut self, person_id: u32) -> Result<bool, Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        let old_location = LocationView::select_by_person(&tx, person_id)?;
        if PersonTable::delete(&tx, person_id)? {
            Self::write_person_event_for_delete(&tx, person_id)?;
            Self::write_location_event_for_delete(&tx, person_id, old_location)?;
            tx.commit()?;
            info!("Deleted person aggregate {}", person_id);
            Ok(true)
        } else {
            tx.rollback()?; // There should be no changes, so tx.commit() would also work
            warn!("Person aggregate {} not found", person_id);
            Ok(false)
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
    // TODO: Put into own class (but then passing a Connection does not work...)
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

    //
    // Private functions
    //

    fn write_person_event_for_insert(tx: &Transaction, person_id: u32, person: &PersonData) -> Result<(), rusqlite::Error> {
        let person_event = PersonEvent::for_insert(person_id, &person);
        Self::write_person_event_and_revision(&tx, &person_event)?;
        Ok(())
    }

    fn write_person_event_for_update(tx: &Transaction, person_id: u32, person: &PersonPatch) -> Result<(), rusqlite::Error> {
        let person_event = PersonEvent::for_update(person_id, person);
        Self::write_person_event_and_revision(&tx, &person_event)?;
        Ok(())
    }

    fn write_person_event_for_delete(tx: &Transaction, person_id: u32) -> Result<(), rusqlite::Error> {
        let person_event = PersonEvent::for_delete(person_id);
        Self::write_person_event_and_revision(&tx, &person_event)?;
        Ok(())
    }

    fn write_person_event_and_revision(tx: &Transaction, event: &PersonEvent) -> Result<u32, rusqlite::Error> {
        match serde_json::to_string(&event) {
            Ok(json) => {
                let revision = PersonEventTable::insert(&tx, json.as_str())?;
                RevisionTable::upsert(&tx, RevisionType::PERSON, revision)?;
                Ok(revision)
            },
            Err(error) => {
                Err(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            }
        }
    }

    fn write_location_event_for_insert(tx: &Transaction, person_id: u32, person: &PersonData) -> Result<(), rusqlite::Error> {
        if person.location.is_some() {
            let location = person.location.as_ref().unwrap();
            let location_event = LocationEvent::for_insert_person(location, person_id, person);
            Self::write_location_event_and_revision(&tx, &location_event)?;
        }
        Ok(())
    }

    fn write_location_event_for_update(tx: &Transaction, person_id: u32, person: &PersonPatch, old_location: Option<String>) -> Result<(), rusqlite::Error> {
        let new_location = person.location.as_ref();
        let need_delete_event = old_location.is_some() && new_location.is_null()
            || Self::locations_differ(old_location.as_ref(), new_location);
        let need_update_event = old_location.is_none() && new_location.is_value()
            || Self::locations_differ(old_location.as_ref(), new_location);
        if need_delete_event {
            // New location of person is null or differs from old position:
            // Create event to remove person from location aggregate
            Self::write_location_event_for_delete(&tx, person_id, old_location)?;
        }
        if need_update_event {
            // Old location of person is null or differs from new position:
            // Create event to add person to location aggregate
            let event = LocationEvent::for_update_person(new_location.unwrap(), person_id, person);
            Self::write_location_event_and_revision(&tx, &event)?;
        }
        Ok(())
    }

    fn locations_differ(old_location: Option<&String>, new_location: Patch<&String>) -> bool {
        old_location.is_some() && new_location.is_value() && old_location.unwrap() != new_location.unwrap()
    }

    fn write_location_event_for_delete(tx: &Transaction, person_id: u32, old_location: Option<String>) -> Result<(), rusqlite::Error> {
        if old_location.is_some() {
            let old_location = old_location.as_ref().unwrap();
            if PersonTable::count_by_location(&tx, old_location)? == 0 {
                // This was the last person with old_location: create event for complete removal of location aggregate
                let location_event = LocationEvent::for_delete_person(old_location, person_id, true);
                Self::write_location_event_and_revision(&tx, &location_event)?;
            } else {
                // Other persons with old_location exist: create event for removal of only this person from location aggregate
                let location_event = LocationEvent::for_delete_person(old_location, person_id, false);
                Self::write_location_event_and_revision(&tx, &location_event)?;
            }
        }
        Ok(())
    }

    fn write_location_event_and_revision(tx: &Transaction, event: &LocationEvent) -> Result<u32, rusqlite::Error> {
        match serde_json::to_string(&event) {
            Ok(json) => {
                let revision = LocationEventTable::insert(&tx, json.as_str())?;
                RevisionTable::upsert(&tx, RevisionType::LOCATION, revision)?;
                Ok(revision)
            },
            Err(error) => {
                Err(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use crate::aggregator::person_aggregator::PersonAggregator;
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
        assert_eq!(person_data, person);
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
    pub fn test_update_events_set_location() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", None, None);
        let patch = PersonPatch::new(Some("Hans"), Patch::Value("Here"), Patch::Value(123));
        assert!(aggregator.insert(&person).is_ok());
        assert!(aggregator.update(1, &patch).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans"}}"#,
            r#"{"1":{"name":"Hans","location":"Here","spouseId":123}}"#
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
            r#"{"1":{"name":"Hans","spouseId":123}}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_update_events_keep_location() {
        let mut aggregator = create_aggregator();

        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = PersonPatch::new(Some("Hans"), Patch::Absent, Patch::Absent);
        assert!(aggregator.insert(&person).is_ok());
        assert!(aggregator.update(1, &patch).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","location":"Here"}}"#,
            r#"{"1":{"name":"Hans"}}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [
            r#"{"Here":{"1":{"name":"Hans","location":"Here"}}}"#
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
            r#"{"1":{"name":"Hans","location":"Here","spouseId":123}}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [
            r#"{"Here":{"1":{"name":"Hans","location":"Here"}}}"#
        ];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_update_events_change_one_location() {
        let mut aggregator = create_aggregator();

        let person1 = PersonData::new("Hans", Some("Here"), None);
        let person2 = PersonData::new("Inge", Some("Here"), None);
        let patch = PersonPatch::new(Some("Hans"), Patch::Value("There"), Patch::Absent);
        assert!(aggregator.insert(&person1).is_ok());
        assert!(aggregator.insert(&person2).is_ok());
        assert!(aggregator.update(1, &patch).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","location":"Here"}}"#,
            r#"{"2":{"name":"Inge","location":"Here"}}"#,
            r#"{"1":{"name":"Hans","location":"There"}}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [
            r#"{"Here":{"1":{"name":"Hans","location":"Here"}}}"#,
            r#"{"Here":{"2":{"name":"Inge","location":"Here"}}}"#,
            r#"{"Here":{"1":null}}"#,
            r#"{"There":{"1":{"name":"Hans","location":"There"}}}"#,
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
            r#"{"1":{"name":"Hans","location":"There"}}"#
        ];
        check_person_events(&mut aggregator, &events_ref);

        let events_ref = [
            r#"{"Here":{"1":{"name":"Hans","location":"Here"}}}"#,
            r#"{"Here":null}"#,
            r#"{"There":{"1":{"name":"Hans","location":"There"}}}"#,
        ];
        check_location_events(&mut aggregator, &events_ref);
    }

    #[test]
    pub fn test_update_events_remove_one_location() {
        let mut aggregator = create_aggregator();

        let person1 = PersonData::new("Hans", Some("Here"), None);
        let person2 = PersonData::new("Fred", Some("Here"), None);
        let patch = PersonPatch::new(Some("Hans"), Patch::Null, Patch::Absent);
        assert!(aggregator.insert(&person1).is_ok());
        assert!(aggregator.insert(&person2).is_ok());
        assert!(aggregator.update(1, &patch).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","location":"Here"}}"#,
            r#"{"2":{"name":"Fred","location":"Here"}}"#,
            r#"{"1":{"name":"Hans","location":null}}"#
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
            r#"{"1":{"name":"Hans","location":null}}"#
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

    //
    // Helper functions for test
    //

    fn create_aggregator() -> PersonAggregator {
        let aggregator = PersonAggregator::new(":memory:");
        assert!(aggregator.is_ok());
        aggregator.unwrap()
    }

    fn get_person_events_and_compare(aggregator: &mut PersonAggregator, from_revision: u32, ref_events: &[&str]) {
        let events = aggregator.get_person_events(from_revision);
        assert!(events.is_ok());
        let events = events.unwrap();
        assert_eq!(events.len(), ref_events.len());
        for (index, &ref_event) in ref_events.iter().enumerate() {
            assert_eq!(events[index], *ref_event);
        }
    }

    fn check_person_events(aggregator: &mut PersonAggregator, events_ref: &[&str]) {
        check_revision(aggregator, RevisionType::PERSON, events_ref.len());
        let events = aggregator.get_person_events(0);
        check_events(events, events_ref);
    }

    fn check_location_events(aggregator: &mut PersonAggregator, events_ref: &[&str]) {
        check_revision(aggregator, RevisionType::LOCATION, events_ref.len());
        let events = aggregator.get_location_events(0);
        check_events(events, events_ref);
    }

    fn check_revision(aggregator: &mut PersonAggregator, revision_type: RevisionType, revision_ref: usize) {
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