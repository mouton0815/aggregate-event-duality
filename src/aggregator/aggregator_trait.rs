use std::time::Duration;
use rusqlite::{Connection, Result, Transaction};
use crate::domain::person_data::PersonData;
use crate::domain::person_patch::PersonPatch;

pub trait AggregatorTrait {
    type Key;
    type Value;
    type Records;

    fn create_tables(&mut self, connection: &Connection) -> Result<()>;

    fn insert(&mut self, tx: &Transaction, person: &PersonData) -> Result<Self::Key>;
    fn update(&mut self, tx: &Transaction, person_id: u32, person: &PersonData, patch: &PersonPatch) -> Result<Self::Value>;
    fn delete(&mut self, tx: &Transaction, person_id: u32, person: &PersonData) -> Result<()>;

    fn get_all(&mut self, tx: &Transaction) -> Result<(u32, Self::Records)>;

    fn get_events(&mut self, tx: &Transaction, from_revision: u32) -> Result<Vec<String>>;
    fn delete_events(&mut self, tx: &Transaction, created_before: Duration) -> Result<usize>;
}