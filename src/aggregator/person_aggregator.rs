use std::error::Error;
use log::{info, warn};
use rusqlite::{Connection, Transaction};
use crate::database::person_aggregate_table::{create_person_aggregate_table, delete_person_aggregate, insert_person_aggregate, read_person_aggregate, read_person_aggregates, update_person_aggregate};
use crate::database::person_event_table::{create_person_event_table, insert_person_event, read_person_events};
use crate::database::revision_table::{create_revision_table, read_person_revision, upsert_person_revision};
use crate::domain::person_aggregate::PersonAggregate;
use crate::domain::person_data::PersonData;
use crate::domain::person_event::PersonEvent;
use crate::domain::person_patch::PersonPatch;
use crate::util::patch::Patch;

pub struct PersonAggregator {
    conn: Connection
}

impl PersonAggregator {
    pub fn new(db_path: &str) -> Result<PersonAggregator, Box<dyn Error>> {
        let conn = Connection::open(db_path)?;
        create_person_aggregate_table(&conn)?;
        create_person_event_table(&conn)?;
        create_revision_table(&conn)?;
        Ok(PersonAggregator { conn })
    }

    pub fn create(&mut self, person: &PersonData) -> Result<PersonAggregate, Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        let person_id = insert_person_aggregate(&tx, &person)?;
        let aggregate = read_person_aggregate(&tx, person_id)?.unwrap(); // Must exist
        let event = Self::create_event_for_post(person_id, person);
        Self::write_event_and_revision(&tx, &event)?;
        tx.commit()?;
        info!("Created {:?} from {:?}", aggregate, person);
        Ok(aggregate)
    }

    pub fn update(&mut self, person_id: u32, person: &PersonPatch) -> Result<Option<PersonAggregate>, rusqlite::Error> {
        let tx = self.conn.transaction()?;
        if update_person_aggregate(&tx, person_id, &person)? {
            let aggregate = read_person_aggregate(&tx, person_id)?.unwrap(); // Must exist
            let event = Self::create_event_for_patch(person_id, person);
            Self::write_event_and_revision(&tx, &event)?;
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
        if delete_person_aggregate(&tx, person_id)? {
            let event = Self::create_event_for_delete(person_id);
            Self::write_event_and_revision(&tx, &event)?;
            tx.commit()?;
            info!("Deleted person aggregate {}", person_id);
            Ok(true)
        } else {
            tx.rollback()?; // There should be no changes, so tx.commit() would also work
            warn!("Person aggregate {} not found", person_id);
            Ok(false)
        }
    }

    pub fn get_aggregates(&mut self) -> Result<(u32, Vec<PersonAggregate>), Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        let revision = read_person_revision(&tx)?;
        let persons = read_person_aggregates(&tx)?;
        tx.commit()?;
        Ok((revision, persons))
    }

    pub fn get_events(&mut self, from_revision: u32) -> Result<Vec<String>, Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        let events = read_person_events(&tx, from_revision)?;
        tx.commit()?;
        Ok(events)
    }

    fn create_event_for_post(person_id: u32, person: &PersonData) -> PersonEvent {
        PersonEvent {
            person_id,
            data: Patch::Value(PersonPatch {
                name: Some(person.name.clone()),
                location: match &person.location {
                    Some(x) => Patch::Value(x.clone()),
                    None => Patch::Absent
                },
                spouse_id: match person.spouse_id {
                    Some(x) => Patch::Value(x),
                    None => Patch::Absent
                }
            })
        }
    }

    fn create_event_for_patch(person_id: u32, person: &PersonPatch) -> PersonEvent {
        PersonEvent {
            person_id,
            data: Patch::Value(PersonPatch { // TODO: Directly clone person?
                name: person.name.clone(),
                location: person.location.clone(),
                spouse_id: person.spouse_id.clone()
            })
        }
    }

    fn create_event_for_delete(person_id: u32) -> PersonEvent {
        PersonEvent {
            person_id,
            data: Patch::Null
        }
    }

    fn write_event_and_revision(tx: &Transaction, event: &PersonEvent) -> Result<u32, rusqlite::Error> {
        match serde_json::to_string(&event) {
            Ok(json) => {
                let revision = insert_person_event(&tx, json.as_str())?;
                upsert_person_revision(&tx, revision)?;
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
    use crate::aggregator::person_aggregator::PersonAggregator;
    use crate::database::person_event_table::read_person_events;
    use crate::database::revision_table::read_person_revision;
    use crate::domain::person_aggregate::PersonAggregate;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;

    #[test]
    pub fn test_create() {
        let mut aggregator = create_aggregator();

        let person = create_person_data();
        let person_res = aggregator.create(&person);
        assert!(person_res.is_ok());

        let person_ref = create_person_ref();
        assert_eq!(person_res.unwrap(), person_ref);

        check_events_and_revision(&mut aggregator, 1);
    }

    #[test]
    pub fn test_update() {
        let mut aggregator = create_aggregator();

        let person = create_person_data();
        let person_update = create_person_patch();
        let person_res = aggregator.create(&person);
        assert!(person_res.is_ok());
        let person_res = aggregator.update(1, &person_update);
        assert!(person_res.is_ok());

        let person_ref = PersonAggregate {
            person_id: 1,
            name: String::from("Inge"),
            location: Some(String::from("Nowhere")),
            spouse_id: Some(12345)
        };

        assert_eq!(person_res.unwrap(), Some(person_ref));

        check_events_and_revision(&mut aggregator, 2);
    }

    #[test]
    pub fn test_update_missing() {
        let mut aggregator = create_aggregator();

        let person_update = create_person_patch();
        let person_res = aggregator.update(1, &person_update);
        assert!(person_res.is_ok());
        assert_eq!(person_res.unwrap(), None);
    }

    #[test]
    pub fn test_delete() {
        let mut aggregator = create_aggregator();

        let person = create_person_data();
        let person_res = aggregator.create(&person);
        assert!(person_res.is_ok());
        let person_res = aggregator.delete(1);
        assert!(person_res.is_ok());
        let person_res = person_res.unwrap();
        assert!(person_res);

        check_events_and_revision(&mut aggregator, 2);
    }

    #[test]
    pub fn test_get_aggregates_empty() {
        let mut aggregator = create_aggregator();

        let persons_res = aggregator.get_aggregates();
        assert!(persons_res.is_ok());

        let person_ref = (0, Vec::new());
        assert_eq!(persons_res.unwrap(), person_ref);
    }

    #[test]
    pub fn test_get_aggregates() {
        let mut aggregator = create_aggregator();

        let person = create_person_data();
        assert!(aggregator.create(&person).is_ok());
        let persons_res = aggregator.get_aggregates();
        assert!(persons_res.is_ok());

        let person_ref = (1, vec!(create_person_ref()));
        assert_eq!(persons_res.unwrap(), person_ref);
    }

    #[test]
    pub fn test_get_events() {
        let mut aggregator = create_aggregator();

        let person = create_person_data();
        let person_update = create_person_patch();
        assert!(aggregator.create(&person).is_ok());
        assert!(aggregator.update(1, &person_update).is_ok());

        let event_ref1 = r#"{"personId":1,"data":{"name":"Hans"}}"#;
        let event_ref2 = r#"{"personId":1,"data":{"name":"Inge","location":"Nowhere","spouseId":12345}}"#;
        get_events_and_compare(&mut aggregator, 0, &[&event_ref1, &event_ref2]);
        get_events_and_compare(&mut aggregator, 1, &[&event_ref1, &event_ref2]);
        get_events_and_compare(&mut aggregator, 2, &[&event_ref2]);
        get_events_and_compare(&mut aggregator, 3, &[]);
    }

    fn create_aggregator() -> PersonAggregator {
        let aggregator = PersonAggregator::new(":memory:");
        assert!(aggregator.is_ok());
        aggregator.unwrap()
    }

    fn create_person_data() -> PersonData {
        PersonData {
            name: String::from("Hans"),
            location: None,
            spouse_id: None
        }
    }

    fn create_person_patch() -> PersonPatch {
        PersonPatch {
            name: Some(String::from("Inge")),
            location: Patch::Value(String::from("Nowhere")),
            spouse_id: Patch::Value(12345)
        }
    }

    fn create_person_ref() -> PersonAggregate {
        PersonAggregate {
            person_id: 1,
            name: String::from("Hans"),
            location: None,
            spouse_id: None
        }
    }

    fn get_events_and_compare(aggregator: &mut PersonAggregator, from_revision: u32, ref_events: &[&str]) {
        let events = aggregator.get_events(from_revision);
        assert!(events.is_ok());
        let events = events.unwrap();
        assert_eq!(events.len(), ref_events.len());
        for (index, &ref_event) in ref_events.iter().enumerate() {
            assert_eq!(events[index], *ref_event);
        }
    }

    fn check_events_and_revision(aggregator: &mut PersonAggregator, revision_ref: u32) {
        let tx = aggregator.conn.transaction().unwrap();
        let revision = read_person_revision(&tx);
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), revision_ref);
        // TODO: Better use aggregator.get_events(0), but this means duplicate borrowing
        let events = read_person_events(&tx, 0);
        assert!(events.is_ok());
        assert_eq!(events.unwrap().len(), revision_ref as usize);
    }
}