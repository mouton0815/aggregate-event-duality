use log::debug;
use rusqlite::{Connection, params, Result, Transaction};

pub enum RevisionType {
    PERSON = 1,
    LOCATION = 2
}

// The tableId field denotes the corresponding aggregate tables
// e.g. RevisionType::PERSON = 1 => "person_aggregate"
const CREATE_REVISION_TABLE: &'static str =
    "CREATE TABLE IF NOT EXISTS revision (
        tableId INTEGER NOT NULL PRIMARY KEY,
        revision INTEGER NOT NULL
    )";

const UPSERT_REVISION: &'static str =
    "INSERT INTO revision (tableId, revision) VALUES (?, ?)
      ON CONFLICT(tableId) DO
      UPDATE SET revision = excluded.revision";

const SELECT_REVISION : &'static str =
    "SELECT revision FROM revision WHERE tableId = ?";

// This is just a namespace to keep method names short
pub struct RevisionTable;

impl RevisionTable {
    pub fn create_table(conn: &Connection) -> Result<()> {
        debug!("Execute\n{}", CREATE_REVISION_TABLE);
        conn.execute(CREATE_REVISION_TABLE, [])?;
        Ok(())
    }

    pub fn upsert(tx: &Transaction, revision_type: RevisionType, revision: u32) -> Result<()> {
        debug!("Execute\n{} with: {}", UPSERT_REVISION, revision);
        tx.execute(UPSERT_REVISION, params![revision_type as u32, revision])?;
        Ok(())
    }

    pub fn read(tx: &Transaction, revision_type: RevisionType) -> Result<u32> {
        let mut stmt = tx.prepare(SELECT_REVISION)?;
        let mut rows = stmt.query([revision_type as u32])?;
        match rows.next()? {
            Some(row) => Ok(row.get(0)?),
            None => Ok(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use crate::database::revision_table::{RevisionTable, RevisionType};

    #[test]
    fn test_upsert_initial() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(RevisionTable::upsert(&tx, RevisionType::LOCATION, 100).is_ok());
        assert!(tx.commit().is_ok());

        check_result(&mut conn, 100);
    }

    #[test]
    fn test_upsert_conflict() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(RevisionTable::upsert(&tx, RevisionType::LOCATION, 100).is_ok());
        assert!(RevisionTable::upsert(&tx, RevisionType::LOCATION, 101).is_ok());
        assert!(tx.commit().is_ok());

        check_result(&mut conn, 101);
    }

    #[test]
    fn test_read_empty() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let revision = RevisionTable::read(&tx, RevisionType::LOCATION);
        assert!(tx.commit().is_ok());
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), 0);
    }

    fn create_connection_and_table() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        let conn = conn.unwrap();
        assert!(RevisionTable::create_table(&conn).is_ok());
        conn
    }

    fn check_result(conn: &mut Connection, ref_revision: u32) {
        let tx = conn.transaction().unwrap();
        let revision = RevisionTable::read(&tx, RevisionType::LOCATION);
        assert!(tx.commit().is_ok());
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), ref_revision);
    }
}
