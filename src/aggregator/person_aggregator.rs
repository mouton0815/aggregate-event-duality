use std::time::Duration;
use rusqlite::{Connection, Result, Transaction};
use crate::aggregator::aggregator_trait::AggregatorTrait;
use crate::database::event_table::PersonEventTable;
use crate::database::person_table::PersonTable;
use crate::database::revision_table::RevisionTable;
use crate::domain::event_type::EventType;
use crate::domain::person_data::PersonData;
use crate::domain::person_event::PersonEvent;
use crate::domain::person_id::PersonId;
use crate::domain::person_map::PersonMap;
use crate::domain::person_patch::PersonPatch;
use crate::util::timestamp::{BoxedTimestamp, UnixTimestamp};

// TODO: Rename to PersonEventWriter?

///
/// Writes events and revision for person changes and for that reason implements
/// [AggregatorTrait](crate::aggregator::aggregator_trait::AggregatorTrait).
/// The actual updates of table ```person``` are already done in
/// [AggregatorFacade](crate::aggregator::aggregator_facade::AggregatorFacade)
/// before delegating to the aggregators.
///
pub struct PersonAggregator {
    timestamp: BoxedTimestamp
}

impl PersonAggregator {
    pub fn new() -> Self {
        Self::new_internal(UnixTimestamp::new())
    }

    fn new_internal(timestamp: BoxedTimestamp) -> Self {
        Self{ timestamp }
    }

    fn write_event_and_revision(&mut self, tx: &Transaction, timestamp: u64, event: PersonEvent) -> Result<()> {
        let event = Self::stringify(event);
        let revision = PersonEventTable::insert(&tx, timestamp, event.as_str())?;
        RevisionTable::upsert(&tx, EventType::PERSON, revision)
    }

    fn stringify(event: PersonEvent) -> String {
        serde_json::to_string(&event).unwrap() // Errors should not happen, panic accepted
    }
}

impl AggregatorTrait for PersonAggregator {
    type Records = PersonMap;

    fn create_tables(&mut self, connection: &Connection) -> Result<()> {
        PersonEventTable::create_table(&connection)
    }

    fn insert(&mut self, tx: &Transaction, id: PersonId, person: &PersonData) -> Result<()> {
        let timestamp = self.timestamp.as_secs();
        let event = PersonEvent::for_insert(id, person);
        self.write_event_and_revision(&tx, timestamp, event)
    }

    fn update(&mut self, tx: &Transaction, id: PersonId, _: &PersonData, patch: &PersonPatch) -> Result<()> {
        let timestamp = self.timestamp.as_secs();
        let event = PersonEvent::for_update(id, &patch);
        self.write_event_and_revision(&tx, timestamp, event)
    }

    fn delete(&mut self, tx: &Transaction, id: PersonId, _: &PersonData) -> Result<()> {
        let timestamp = self.timestamp.as_secs();
        let event = PersonEvent::for_delete(id);
        self.write_event_and_revision(&tx, timestamp, event)
    }

    fn get_all(&mut self, tx: &Transaction) -> Result<(usize, Self::Records)> {
        let revision = RevisionTable::read(&tx, EventType::PERSON)?;
        let persons = PersonTable::select_all(&tx)?;
        Ok((revision, persons))
    }

    fn get_events(&mut self, tx: &Transaction, from_revision: usize) -> Result<Vec<String>> {
        PersonEventTable::read(&tx, from_revision)
    }

    fn delete_events(&mut self, tx: &Transaction, created_before: Duration) -> Result<usize> {
        let created_before = self.timestamp.as_secs() - created_before.as_secs();
        PersonEventTable::delete_before(&tx, created_before)
    }
}

#[cfg(test)]
pub mod tests {
    use std::time::Duration;
    use rusqlite::{Connection, Result, Transaction};
    use crate::aggregator::aggregator_trait::AggregatorTrait;
    use crate::aggregator::person_aggregator::PersonAggregator;
    use crate::database::event_table::PersonEventTable;
    use crate::database::person_table::PersonTable;
    use crate::database::revision_table::RevisionTable;
    use crate::domain::event_type::EventType;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_id::PersonId;
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
        assert!(aggregator.insert(&tx, PersonId::from(1), &person).is_ok());

        let events_ref = [r#"{"1":{"name":"Hans","city":"Here"}}"#];
        check_events(&tx, &events_ref);
        assert!(tx.commit().is_ok());
    }

    #[test]
    pub fn test_update() {
        let mut conn = create_connection();
        let tx = conn.transaction().unwrap();

        let person = PersonData::new("Hans", Some("Here"), None);
        let patch = PersonPatch::new(Some("Inge"), Patch::Null, Patch::Value(PersonId::from(123)));
        let mut aggregator = create_aggregator();
        assert!(aggregator.insert(&tx, PersonId::from(1), &person).is_ok());
        assert!(aggregator.update(&tx, PersonId::from(1), &person, &patch).is_ok());

        let events_ref = [
            r#"{"1":{"name":"Hans","city":"Here"}}"#,
            r#"{"1":{"name":"Inge","city":null,"spouse":123}}"#
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
        assert!(aggregator.insert(&tx, PersonId::from(1), &person).is_ok());
        assert!(aggregator.delete(&tx, PersonId::from(1), &person).is_ok());

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
        assert!(RevisionTable::upsert(&tx, EventType::PERSON, 2).is_ok());

        let mut aggregator = create_aggregator();
        let persons_res = aggregator.get_all(&tx);
        assert!(persons_res.is_ok());

        let mut person_map = PersonMap::new();
        person_map.put(PersonId::from(1), PersonData::new("Hans", None, None));
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
        let patch = PersonPatch::new(Some("Inge"), Patch::Value("Nowhere"), Patch::Value(PersonId::from(123)));
        let mut aggregator = create_aggregator();
        assert!(aggregator.insert(&tx, PersonId::from(1), &person).is_ok());
        assert!(aggregator.update(&tx, PersonId::from(1), &person, &patch).is_ok());

        let event_ref1 = r#"{"1":{"name":"Hans"}}"#;
        let event_ref2 = r#"{"1":{"name":"Inge","city":"Nowhere","spouse":123}}"#;
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
        let patch2 = PersonPatch::new(Some("Fred"), Patch::Value("Nowhere"), Patch::Value(PersonId::from(123)));
        let mut aggregator = create_aggregator();
        assert!(aggregator.insert(&tx, PersonId::from(1), &person1).is_ok());
        assert!(aggregator.insert(&tx, PersonId::from(2), &person2).is_ok());
        assert!(aggregator.update(&tx, PersonId::from(2), &person2, &patch2).is_ok());

        // IncrementalTimestamp is at 4 inside delete_events() below; minus 1 yields 3,
        // so it deletes all events <3 (i.e. the first two) and keeps the last one
        let result = aggregator.delete_events(&tx, Duration::from_secs(1));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2); // Two events deleted

        get_events_and_compare(&tx, 0, &[
            r#"{"2":{"name":"Fred","city":"Nowhere","spouse":123}}"#]);
        assert!(tx.commit().is_ok());
    }

    //
    // Helper functions for test
    //

    fn create_aggregator() -> PersonAggregator {
        let timestamp = IncrementalTimestamp::new();
        PersonAggregator::new_internal(timestamp)
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

    fn check_events(tx: &Transaction, events_ref: &[&str]) {
        compare_revision(tx, EventType::PERSON, events_ref.len());
        compare_events(PersonEventTable::read(tx, 0), events_ref);
    }

    // Function is also used by LocationAggregator tests
    pub fn compare_revision(tx: &Transaction, event_type: EventType, revision_ref: usize) {
        let revision = RevisionTable::read(&tx, event_type);
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), revision_ref);
    }

    // Function is also used by LocationAggregator tests
    pub fn compare_events(events: Result<Vec<String>>, events_ref: &[&str]) {
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