use std::sync::{Arc, Mutex};
use std::time::Duration;
use log::{info, warn};
use rusqlite::{Connection, Result};
use crate::aggregator::aggregator_trait::AggregatorTrait;
use crate::aggregator::location_aggregator::LocationAggregator;
use crate::aggregator::person_aggregator::PersonAggregator;
use crate::database::person_table::PersonTable;
use crate::database::revision_table::RevisionTable;
use crate::domain::event_type::EventType;
use crate::domain::location_map::LocationMap;
use crate::domain::person_data::PersonData;
use crate::domain::person_map::PersonMap;
use crate::domain::person_patch::PersonPatch;
use crate::util::deletion_scheduler::DeletionTask;

// TODO: Rename to PersonProcessor?

///
/// This class is the facade to the REST handlers and the scheduler.
/// It processes and stores person data and delegates to the aggregators.
/// It also creates the transaction boundary for all database operations.
///
pub struct AggregatorFacade {
    connection: Connection,
    person_aggr: PersonAggregator,
    location_aggr: LocationAggregator
}

pub type MutexAggregator = Arc<Mutex<AggregatorFacade>>;

impl AggregatorFacade {
    pub fn new(db_path: &str) -> Result<Self> {
        let connection = Connection::open(db_path)?;
        PersonTable::create_table(&connection)?;
        RevisionTable::create_table(&connection)?;
        let mut person_aggr = PersonAggregator::new();
        person_aggr.create_tables(&connection)?;
        let mut location_aggr = LocationAggregator::new();
        location_aggr.create_tables(&connection)?;
        Ok(Self{ connection, person_aggr, location_aggr })
    }

    pub fn insert(&mut self, person: &PersonData) -> Result<(u32, PersonData)> {
        let tx = self.connection.transaction()?;
        let person_id = PersonTable::insert(&tx, &person)?;
        self.person_aggr.insert(&tx, person_id, &person)?;
        self.location_aggr.insert(&tx, person_id, &person)?;
        tx.commit()?;
        info!("Created {:?} with id {}", person, person_id);
        Ok((person_id, person.clone()))
    }

    pub fn update(&mut self, person_id: u32, patch: &PersonPatch) -> Result<Option<PersonData>> {
        let tx = self.connection.transaction()?;
        match PersonTable::select_by_id(&tx, person_id)? {
            Some(before) => {
                let after = PersonTable::update(&tx, person_id, &patch)?;
                // Recompute patch for minimal change set
                if let Some(patch) = PersonPatch::of(&before, &after) {
                    self.person_aggr.update(&tx, person_id, &before, &patch)?;
                    self.location_aggr.update(&tx, person_id, &before, &patch)?;
                }
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

    pub fn delete(&mut self, person_id: u32) -> Result<bool> {
        let tx = self.connection.transaction()?;
        match PersonTable::select_by_id(&tx, person_id)? {
            Some(before) => {
                PersonTable::delete(&tx, person_id)?;
                self.person_aggr.delete(&tx, person_id, &before)?;
                self.location_aggr.delete(&tx, person_id, &before)?;
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

    pub fn get_persons(&mut self) -> Result<(usize, PersonMap)> {
        let tx = self.connection.transaction()?;
        let result = self.person_aggr.get_all(&tx)?;
        tx.commit()?;
        Ok(result)
    }

    pub fn get_locations(&mut self) -> Result<(usize, LocationMap)> {
        let tx = self.connection.transaction()?;
        let result = self.location_aggr.get_all(&tx)?;
        tx.commit()?;
        Ok(result)
    }

    pub fn get_events(&mut self, event_type: EventType, from_revision: usize) -> Result<Vec<String>> {
        let tx = self.connection.transaction()?;
        let events = match event_type {
            EventType::PERSON => self.person_aggr.get_events(&tx, from_revision),
            EventType::LOCATION => self.location_aggr.get_events(&tx, from_revision)
        }?;
        tx.commit()?;
        Ok(events)
    }

    pub fn delete_events(&mut self, created_before: Duration) -> Result<usize> {
        let tx = self.connection.transaction()?;
        let mut count = self.person_aggr.delete_events(&tx, created_before)?;
        count += self.location_aggr.delete_events(&tx, created_before)?;
        tx.commit()?;
        if count > 0 {
            info!("Deleted {} outdated events", count);
        }
        Ok(count)
    }
}

// Implementation of the task for the deletion scheduler
impl DeletionTask<rusqlite::Error> for AggregatorFacade {
    fn delete(&mut self, created_before: Duration) -> Result<()> {
        match self.delete_events(created_before) {
            Ok(_) => Ok(()),
            Err(e) => Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregator::aggregator_facade::AggregatorFacade;
    use crate::aggregator::person_aggregator::tests::compare_events;
    use crate::domain::event_type::EventType;
    use crate::domain::location_data::LocationData;
    use crate::domain::location_map::LocationMap;
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
        assert_eq!(person_res.unwrap(), true);
    }

    #[test]
    pub fn test_delete_missing() {
        let mut aggregator = create_aggregator();

        let person_res = aggregator.delete(1);
        assert!(person_res.is_ok());
        assert_eq!(person_res.unwrap(), false);
    }

    //
    // Test read operations
    //

    #[test]
    pub fn test_get_persons_empty() {
        let mut aggregator = create_aggregator();

        let persons_res = aggregator.get_persons();
        assert!(persons_res.is_ok());

        let person_ref = (0, PersonMap::new());
        assert_eq!(persons_res.unwrap(), person_ref);
    }

    #[test]
    pub fn test_get_persons() {
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
    pub fn test_get_locations() {
        let mut aggregator = create_aggregator();

        let person1 = PersonData::new("Hans", Some("here"), Some(123));
        let person2 = PersonData::new("Inge", Some("there"), None);
        let person3 = PersonData::new("Fred", Some("here"), None);
        assert!(aggregator.insert(&person1).is_ok());
        assert!(aggregator.insert(&person2).is_ok());
        assert!(aggregator.insert(&person3).is_ok());

        let loc_res = aggregator.get_locations();
        assert!(loc_res.is_ok());

        let mut loc_map = LocationMap::new();
        loc_map.put("here", LocationData::new(2, 1));
        loc_map.put("there", LocationData::new(1, 0));
        let loc_ref = (3, loc_map);
        assert_eq!(loc_res.unwrap(), loc_ref);
    }

    #[test]
    pub fn test_get_events() {
        let mut aggregator = create_aggregator();

        let person1 = PersonData::new("Hans", Some("here"), Some(123));
        let person2 = PersonData::new("Inge", Some("there"), None);
        let person3 = PersonData::new("Fred", Some("here"), None);
        assert!(aggregator.insert(&person1).is_ok());
        assert!(aggregator.insert(&person2).is_ok());
        assert!(aggregator.insert(&person3).is_ok());

        let events = aggregator.get_events(EventType::PERSON, 0);
        compare_events(events, &[
            r#"{"1":{"name":"Hans","location":"here","spouseId":123}}"#,
            r#"{"2":{"name":"Inge","location":"there"}}"#,
            r#"{"3":{"name":"Fred","location":"here"}}"#
        ]);
        let events = aggregator.get_events(EventType::LOCATION, 0);
        compare_events(events, &[
            r#"{"here":{"total":1,"married":1}}"#,
            r#"{"there":{"total":1,"married":0}}"#,
            r#"{"here":{"total":2}}"#
        ]);
    }

    //
    // Helper functions for test
    //

    fn create_aggregator() -> AggregatorFacade {
        let aggregator = AggregatorFacade::new(":memory:");
        assert!(aggregator.is_ok());
        aggregator.unwrap()
    }
}