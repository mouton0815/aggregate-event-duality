use const_format::formatcp;
use rusqlite::{Connection, params, Result, Transaction};

#[derive(Copy, Clone)]
enum RevisionType {
    Company = 1
}

const COMPANY_REVISION_TABLE : &'static str = "company_revision";

// The tableId field denotes the aggregate tables (RevisionType::Company => 1 => "company_aggregate")
const CREATE_COMPANY_REVISION_TABLE : &'static str = formatcp!("
    CREATE TABLE IF NOT EXISTS {} (
        tableId INTEGER NOT NULL PRIMARY KEY,
        revision INTEGER NOT NULL
    )",
    COMPANY_REVISION_TABLE
);

const UPSERT_COMPANY_REVISION : &'static str = formatcp!("
    INSERT INTO {} (tableId, revision) VALUES (?, ?)
      ON CONFLICT(tableId) DO
      UPDATE SET revision = excluded.revision",
    COMPANY_REVISION_TABLE
);

const SELECT_REVISION : &'static str = formatcp!("
    SELECT revision FROM {} WHERE tableId = ?",
    COMPANY_REVISION_TABLE
);

pub struct CompanyRevisionDAO {
}

impl CompanyRevisionDAO {
    pub fn create_table(conn: &Connection) -> Result<()> {
        conn.execute(CREATE_COMPANY_REVISION_TABLE, [])?;
        Ok(())
    }

    pub fn upsert_company_revision(tx: &Transaction, revision: u32) -> Result<()> {
        tx.execute(UPSERT_COMPANY_REVISION, params![RevisionType::Company as u32, revision])?;
        Ok(())
    }

    pub fn get_company_revision(tx: &Transaction) -> Result<u32> {
        let mut stmt = tx.prepare(SELECT_REVISION)?;
        stmt.query_row([RevisionType::Company as u32], |row| row.get(0))
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::{Connection, Result, Transaction};
    use crate::database::company_revision_dao::{CompanyRevisionDAO, RevisionType};

    #[test]
    fn test_upsert() {
        let mut conn = create_connection();
        assert!(CompanyRevisionDAO::create_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();

        /*
        match CompanyRevisionDAO::upsert(&tx, 100) {
            Ok(()) => println!("OK !!!"),
            Err(e) => eprintln!("Error: {}", e),
        }
        */
        assert!(CompanyRevisionDAO::upsert_company_revision(&tx, 100).is_ok());
        //check_result(&mut conn, 100);
        assert!(CompanyRevisionDAO::upsert_company_revision(&tx, 101).is_ok());
        assert!(tx.commit().is_ok());

        // Obtain new transaction
        let tx = conn.transaction().unwrap();
        let revision = CompanyRevisionDAO::get_company_revision(&tx);
        assert!(tx.commit().is_ok());
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), 101);
    }

    fn create_connection() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        conn.unwrap()
    }

    fn check_result(conn: &mut Connection, ref_revision: u32) {
        let tx = conn.transaction().unwrap();
        let revision = CompanyRevisionDAO::get_company_revision(&tx);
        assert!(tx.commit().is_ok());
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), ref_revision);
    }
}
