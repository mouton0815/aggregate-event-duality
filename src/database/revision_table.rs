use const_format::formatcp;
use log::debug;
use rusqlite::{Connection, params, Result, Transaction};

enum RevisionType {
    Person = 1 // TODO: Remove this, overly complicated/generalized
}

const REVISION_TABLE: &'static str = "revision";

// The tableId field denotes the aggregate tables (RevisionType::Person => 1 => "person_aggregate")
const CREATE_REVISION_TABLE: &'static str = formatcp!("
    CREATE TABLE IF NOT EXISTS {} (
        tableId INTEGER NOT NULL PRIMARY KEY,
        revision INTEGER NOT NULL
    )",
    REVISION_TABLE
);

const UPSERT_REVISION: &'static str = formatcp!("
    INSERT INTO {} (tableId, revision) VALUES (?, ?)
      ON CONFLICT(tableId) DO
      UPDATE SET revision = excluded.revision",
    REVISION_TABLE
);

const SELECT_REVISION : &'static str = formatcp!("
    SELECT revision FROM {} WHERE tableId = ?",
    REVISION_TABLE
);

pub fn create_revision_table(conn: &Connection) -> Result<()> {
    debug!("Execute {}", CREATE_REVISION_TABLE);
    conn.execute(CREATE_REVISION_TABLE, [])?;
    Ok(())
}

pub fn upsert_person_revision(tx: &Transaction, revision: u32) -> Result<()> {
    debug!("Execute {} with: {}", UPSERT_REVISION, revision);
    tx.execute(UPSERT_REVISION, params![RevisionType::Person as u32, revision])?;
    Ok(())
}

pub fn read_person_revision(tx: &Transaction) -> Result<u32> {
    let mut stmt = tx.prepare(SELECT_REVISION)?;
    let mut rows = stmt.query([RevisionType::Person as u32])?;
    match rows.next()? {
        Some(row) => Ok(row.get(0)?),
        None => Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use crate::database::revision_table::{create_revision_table, read_person_revision, upsert_person_revision};

    #[test]
    fn test_upsert_initial() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(upsert_person_revision(&tx, 100).is_ok());
        assert!(tx.commit().is_ok());

        check_result(&mut conn, 100);
    }

    #[test]
    fn test_upsert_conflict() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(upsert_person_revision(&tx, 100).is_ok());
        assert!(upsert_person_revision(&tx, 101).is_ok());
        assert!(tx.commit().is_ok());

        check_result(&mut conn, 101);
    }

    #[test]
    fn test_read_empty() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let revision = read_person_revision(&tx);
        assert!(tx.commit().is_ok());
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), 0);
    }

    fn create_connection_and_table() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        let conn = conn.unwrap();
        assert!(create_revision_table(&conn).is_ok());
        conn
    }

    fn check_result(conn: &mut Connection, ref_revision: u32) {
        let tx = conn.transaction().unwrap();
        let revision = read_person_revision(&tx);
        assert!(tx.commit().is_ok());
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), ref_revision);
    }
}
