use std::time::Duration;
use rusqlite::{Connection, Result, Transaction};
use crate::domain::person_data::PersonData;
use crate::domain::person_patch::PersonPatch;

// TODO: Or only "Aggregator"?
pub trait AggregatorTrait {
    type Output;

    fn create_tables(&mut self, connection: &Connection) -> Result<()>;

    fn insert(&mut self, tx: &Transaction, person: &PersonData) -> Result<Option<u32>>;
    fn update(&mut self, tx: &Transaction, person_id: u32, person: &PersonData, patch: &PersonPatch) -> Result<Option<PersonData>>;
    fn delete(&mut self, tx: &Transaction, person_id: u32, person: &PersonData) -> Result<()>;

    fn get_all(&mut self, tx: &Transaction) -> Result<(u32, Self::Output)>;

    fn get_events(&mut self, tx: &Transaction, from_revision: u32) -> Result<Vec<String>>;
    fn delete_events(&mut self, tx: &Transaction, created_before: Duration) -> Result<usize>;
}