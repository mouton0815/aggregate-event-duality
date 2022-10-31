use const_format::formatcp;
use rusqlite::{Connection, params, Result, Transaction};

enum RevisionType {
    Company = 1
}

const REVISION_TABLE: &'static str = "revision";

// The tableId field denotes the aggregate tables (RevisionType::Company => 1 => "company_aggregate")
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
    conn.execute(CREATE_REVISION_TABLE, [])?;
    Ok(())
}

pub fn upsert_company_revision(tx: &Transaction, revision: u32) -> Result<()> {
    tx.execute(UPSERT_REVISION, params![RevisionType::Company as u32, revision])?;
    Ok(())
}

pub fn read_company_revision(tx: &Transaction) -> Result<u32> {
    let mut stmt = tx.prepare(SELECT_REVISION)?;
    stmt.query_row([RevisionType::Company as u32], |row| row.get(0))
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use crate::database::revision_table::{create_revision_table, read_company_revision, upsert_company_revision};

    #[test]
    fn test_upsert_initial() {
        let mut conn = create_connection();
        assert!(create_revision_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();
        assert!(upsert_company_revision(&tx, 100).is_ok());
        assert!(tx.commit().is_ok());

        check_result(&mut conn, 100);
    }

    #[test]
    fn test_upsert_conflict() {
        let mut conn = create_connection();
        assert!(create_revision_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();
        assert!(upsert_company_revision(&tx, 100).is_ok());
        assert!(upsert_company_revision(&tx, 101).is_ok());
        assert!(tx.commit().is_ok());

        check_result(&mut conn, 101);
    }

    fn create_connection() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        conn.unwrap()
    }

    fn check_result(conn: &mut Connection, ref_revision: u32) {
        let tx = conn.transaction().unwrap();
        let revision = read_company_revision(&tx);
        assert!(tx.commit().is_ok());
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), ref_revision);
    }
}
