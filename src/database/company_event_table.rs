use const_format::formatcp;
use rusqlite::{Connection, params, Result, Transaction};

const COMPANY_EVENT_TABLE : &'static str = "company_event";

const CREATE_COMPANY_EVENT_TABLE : &'static str = formatcp!("
    CREATE TABLE IF NOT EXISTS {} (
        revision INTEGER NOT NULL PRIMARY KEY,
        event TEXT NOT NULL
    )",
    COMPANY_EVENT_TABLE
);

const INSERT_COMPANY_EVENT : &'static str = formatcp!("
    INSERT INTO {} (event) VALUES (?)",
    COMPANY_EVENT_TABLE
);

// TODO: DELETE_COMPANY_EVENTS_BEFORE

const SELECT_COMPANY_EVENTS : &'static str = formatcp!("
    SELECT event FROM {} WHERE revision >= ? ORDER BY revision",
    COMPANY_EVENT_TABLE
);

pub fn create_company_event_table(conn: &Connection) -> Result<()> {
    conn.execute(CREATE_COMPANY_EVENT_TABLE, [])?;
    Ok(())
}

pub fn insert_company_event(tx: &Transaction, event: &str) -> Result<u32> {
    tx.execute(INSERT_COMPANY_EVENT, params![event])?;
    Ok(tx.last_insert_rowid() as u32)
}

pub fn read_company_events(tx: &Transaction, from_revision: i64) -> Result<Vec<String>> {
    let mut stmt = tx.prepare(SELECT_COMPANY_EVENTS)?;
    let rows = stmt.query_map([from_revision], |row| {
        let json: String = row.get(0)?;
        Ok(json)
    })?;
    let mut events : Vec<String> = Vec::new();
    for row in rows {
        events.push(row?);
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use crate::database::company_event_table::{create_company_event_table, insert_company_event, read_company_events};

    #[test]
    fn test_insert() {
        let mut conn = create_connection();
        assert!(create_company_event_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();
        let revision = insert_company_event(&tx, "Foo");
        assert!(tx.commit().is_ok());
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), 1);
    }

    #[test]
    fn test_read_from_empty() {
        let mut conn = create_connection();
        assert!(create_company_event_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();
        let events = read_company_events(&tx, 1);
        assert!(tx.commit().is_ok());
        assert!(events.is_ok());
        assert_eq!(events.unwrap().len(), 0);
    }

    #[test]
    fn test_read_from() {
        let mut conn = create_connection();
        assert!(create_company_event_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();
        assert!(insert_company_event(&tx, "Foo").is_ok());
        assert!(tx.commit().is_ok());

        let tx = conn.transaction().unwrap();
        let events = read_company_events(&tx, 1);
        assert!(tx.commit().is_ok());
        assert!(events.is_ok());
        let events = events.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0], "Foo");
    }

    fn create_connection() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        conn.unwrap()
    }
}
