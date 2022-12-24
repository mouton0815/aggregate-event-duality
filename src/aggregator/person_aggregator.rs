use std::time::Duration;
use rusqlite::{Connection, Result, Transaction};
use crate::aggregator::aggregator_trait::AggregatorTrait;
use crate::database::event_table::PersonEventTable;
use crate::database::person_table::PersonTable;
use crate::database::revision_table::{RevisionTable, RevisionType};
use crate::domain::person_data::PersonData;
use crate::domain::person_event::PersonEvent;
use crate::domain::person_map::PersonMap;
use crate::domain::person_patch::PersonPatch;
use crate::util::timestamp::BoxedTimestamp;

pub struct PersonAggregator {
    timestamp: BoxedTimestamp
}

impl AggregatorTrait for PersonAggregator {
    type Key = u32;
    type Record = PersonData;
    type Records = PersonMap;

    fn create_tables(&mut self, connection: &Connection) -> Result<()> {
        PersonTable::create_table(&connection)?;
        PersonEventTable::create_table(&connection)?;
        RevisionTable::create_table(&connection)
    }

    fn insert(&mut self, tx: &Transaction, person: &PersonData) -> Result<Self::Key> {
        let timestamp = self.timestamp.as_secs();
        let person_id = PersonTable::insert(&tx, &person)?;
        let event = PersonEvent::for_insert(person_id, person);
        self.write_event_and_revision(&tx, timestamp, event)?;
        Ok(person_id)
    }

    fn update(&mut self, tx: &Transaction, person_id: u32, person: &PersonData, patch: &PersonPatch) -> Result<Self::Record> {
        let after = PersonTable::update(&tx, person_id, &patch)?.unwrap();
        let patch = PersonPatch::of(person, &after); // Recompute patch for minimal change set
        if patch.is_change() {
            let timestamp = self.timestamp.as_secs();
            let event = PersonEvent::for_update(person_id, &patch);
            self.write_event_and_revision(&tx, timestamp, event)?;
        }
        Ok(after)
    }

    fn delete(&mut self, tx: &Transaction, person_id: u32, _: &PersonData) -> Result<()> {
        let timestamp = self.timestamp.as_secs();
        if PersonTable::delete(&tx, person_id)? {
            let event = PersonEvent::for_delete(person_id);
            self.write_event_and_revision(&tx, timestamp, event)?;
        }
        Ok(())
    }

    fn get_all(&mut self, tx: &Transaction) -> Result<(u32, Self::Records)> {
        let revision = RevisionTable::read(&tx, RevisionType::PERSON)?;
        let persons = PersonTable::select_all(&tx)?;
        Ok((revision, persons))
    }

    fn get_events(&mut self, tx: &Transaction, from_revision: u32) -> Result<Vec<String>> {
        PersonEventTable::read(&tx, from_revision)
    }

    fn delete_events(&mut self, tx: &Transaction, created_before: Duration) -> Result<usize> {
        let created_before = self.timestamp.as_secs() - created_before.as_secs();
        PersonEventTable::delete_before(&tx, created_before)
    }
}

impl PersonAggregator {
    pub fn new(timestamp: BoxedTimestamp) -> Self {
        Self{ timestamp }
    }

    pub fn get_one(&mut self, tx: &Transaction, person_id: u32) -> Result<Option<PersonData>> {
        PersonTable::select_by_id(&tx, person_id)
    }

    fn write_event_and_revision(&mut self, tx: &Transaction, timestamp: u64, event: PersonEvent) -> Result<()> {
        let event = Self::stringify(event);
        let revision = PersonEventTable::insert(&tx, timestamp, event.as_str())?;
        RevisionTable::upsert(&tx, RevisionType::PERSON, revision)
    }

    fn stringify(event: PersonEvent) -> String {
        serde_json::to_string(&event).unwrap() // Errors should not happen, panic accepted
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use rusqlite::{Connection, Result, Transaction};
    use crate::aggregator::aggregator_trait::AggregatorTrait;
    use crate::aggregator::person_aggregator::PersonAggregator;
    use crate::database::event_table::PersonEventTable;
    use crate::database::person_table::PersonTable;
    use crate::database::revision_table::{RevisionTable, RevisionType};
    use crate::domain::person_data::PersonData;
    use crate::domain::person_map::PersonMap;
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

        let person = PersonData::new("Hans", Some("Here"), None);
        let mut aggregator = create_aggregator();
        assert!(aggregator.insert(&tx, &person).is_ok());

        let events_ref = [r#"{"1":{"name":"Hans","location":"Here"}}"#];
        check_events(&tx, &events_ref);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_update() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();

        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = PersonPatch::new(Some("Inge"), Patch::Null, Patch::Value(123));
        let mut aggregator = create_aggregator();
        assert!(aggregator.insert(&tx, &person).is_ok());
        assert!(aggregator.update(&tx, 1, &person, &patch).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","location":"Here"}}"#,
            r#"{"1":{"name":"Inge","location":null,"spouseId":123}}"#
        ];
        check_events(&tx, &events_ref);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_update_no_change() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();

        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = PersonPatch::new(None, Patch::Value("Here"), Patch::Null);
        let mut aggregator = create_aggregator();
        assert!(aggregator.insert(&tx, &person).is_ok());
        assert!(aggregator.update(&tx, 1, &person, &patch).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","location":"Here"}}"#
        ];
        check_events(&tx, &events_ref);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_delete() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();

        let person = PersonData::new("Hans", None, None);
        let mut aggregator = create_aggregator();
        assert!(aggregator.insert(&tx, &person).is_ok());
        assert!(aggregator.delete(&tx,1, &person).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans"}}"#,
            r#"{"1":null}"#
        ];
        check_events(&tx, &events_ref);
        assert!(tx.commit().is_ok());
    }

    //
    // Test read operations
    //

    #[test]
    pub fn test_get_all() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();

        let person = PersonData::new("Hans", None, None);
        assert!(PersonTable::insert(&tx, &person).is_ok());
        assert!(RevisionTable::upsert(&tx, RevisionType::PERSON, 2).is_ok());

        let mut aggregator = create_aggregator();
        let persons_res = aggregator.get_all(&tx);
        assert!(persons_res.is_ok());

        let mut person_map = PersonMap::new();
        person_map.put(1, PersonData::new("Hans", None, None));
        let person_ref = (2, person_map);
        assert_eq!(persons_res.unwrap(), person_ref);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_get_all_empty() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();

        let mut aggregator = create_aggregator();
        let persons_res = aggregator.get_all(&tx);
        assert!(persons_res.is_ok());

        let person_ref = (0, PersonMap::new());
        assert_eq!(persons_res.unwrap(), person_ref);
        assert!(tx.commit().is_ok());
    }

    //
    // Test event-related functions
    //

    #[test]
    pub fn test_get_events() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();

        let person = PersonData::new("Hans", None, None);
        let patch = PersonPatch::new(Some("Inge"), Patch::Value("Nowhere"), Patch::Value(123));
        let mut aggregator = create_aggregator();
        assert!(aggregator.insert(&tx, &person).is_ok());
        assert!(aggregator.update(&tx,1, &person, &patch).is_ok());

        let event_ref1 = r#"{"1":{"name":"Hans"}}"#;
        let event_ref2 = r#"{"1":{"name":"Inge","location":"Nowhere","spouseId":123}}"#;
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

        let person1 = PersonData::new("Hans", None, None);
        let person2 = PersonData::new("Inge", None, None);
        let patch2 = PersonPatch::new(Some("Fred"), Patch::Value("Nowhere"), Patch::Value(123));
        let mut aggregator = create_aggregator();
        assert!(aggregator.insert(&tx, &person1).is_ok());
        assert!(aggregator.insert(&tx, &person2).is_ok());
        assert!(aggregator.update(&tx, 2, &person2, &patch2).is_ok());

        // IncrementalTimestamp is at 4 inside delete_events(); minus 1 yields 3,
        // so it deletes the first two events and keeps the last
        let result = aggregator.delete_events(&tx, Duration::from_secs(1));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);

        let event_ref = r#"{"2":{"name":"Fred","location":"Nowhere","spouseId":123}}"#;
        get_events_and_compare(&tx, 0, &[&event_ref]);
        assert!(tx.commit().is_ok());
    }

    //
    // Helper functions for test
    //

    fn create_aggregator() -> PersonAggregator {
        let timestamp = IncrementalTimestamp::new();
        PersonAggregator::new(timestamp)
    }

    fn create_connection() -> Connection {
        let connection = Connection::open(":memory:");
        assert!(connection.is_ok());
        let connection = connection.unwrap();
        assert!(PersonTable::create_table(&connection).is_ok());
        assert!(PersonEventTable::create_table(&connection).is_ok());
        assert!(RevisionTable::create_table(&connection).is_ok());
        connection
    }

    fn get_events_and_compare(tx: &Transaction, from_revision: u32, ref_events: &[&str]) {
        let mut aggregator = create_aggregator();
        let events = aggregator.get_events(&tx, from_revision);
        assert!(events.is_ok());
        let events = events.unwrap();
        assert_eq!(events.len(), ref_events.len());
        for (index, &ref_event) in ref_events.iter().enumerate() {
            assert_eq!(events[index], *ref_event);
        }
    }

    fn check_events(tx: &Transaction, events_ref: &[&str]) {
        compare_revision(tx, RevisionType::PERSON, events_ref.len());
        compare_events(PersonEventTable::read(tx, 0), events_ref);
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