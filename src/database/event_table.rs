use log::debug;
use rusqlite::{Connection, params, Result, Transaction};

pub type PersonEventTable = EventTable<0>;
pub type LocationEventTable = EventTable<1>;

// Generic implementation for stringified events for both persons and locations.
// NOTE: String and Enum type parameters are still experimental, only numeric constants work.
//       So we need an additional function that translates the constant to a table name.
//       https://rust-lang.github.io/rfcs/2000-const-generics.html
pub struct EventTable<const TABLE_TYPE: usize>;

impl<const TABLE_TYPE: usize> EventTable<TABLE_TYPE> {

    pub fn create_table(conn: &Connection) -> Result<()> {
        let stmt = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                revision INTEGER NOT NULL PRIMARY KEY,
                event TEXT NOT NULL
            )", Self::table_name(TABLE_TYPE));
        debug!("Execute\n{}", stmt);
        conn.execute(stmt.as_str(), [])?;
        Ok(())
    }

    pub fn insert(tx: &Transaction, event: &str) -> Result<u32> {
        let stmt = format!(
            "INSERT INTO {} (event) VALUES (?)",
            Self::table_name(TABLE_TYPE));
        debug!("Execute\n{}\nwith: {}", stmt, event);
        tx.execute(stmt.as_str(), params![event])?;
        Ok(tx.last_insert_rowid() as u32)
    }

    pub fn read(tx: &Transaction, from_revision: u32) -> Result<Vec<String>> {
        let stmt = format!(
            "SELECT event FROM {} WHERE revision >= ? ORDER BY revision",
            Self::table_name(TABLE_TYPE));
        debug!("Execute\n{} with: {}", stmt, from_revision);
        let mut stmt = tx.prepare(stmt.as_str())?;
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

    // TODO: Add method to delete all events before a revision

    // Necessary translation function between usize and str constants.
    // Can be removed once Rust stably supports const str generics.
    // https://rust-lang.github.io/rfcs/2000-const-generics.html
    fn table_name(table_type: usize) -> &'static str {
        match table_type {
            0 => "person_event",
            1 => "location_event",
            _ => panic!("Unknown event table type {}", table_type)
        }
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use crate::database::event_table::PersonEventTable;

    #[test]
    fn test_insert() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let revision = PersonEventTable::insert(&tx, "foo");
        assert!(tx.commit().is_ok());
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), 1);
    }

    #[test]
    fn test_read_from_empty() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let events = PersonEventTable::read(&tx, 1);
        assert!(tx.commit().is_ok());
        assert!(events.is_ok());
        assert_eq!(events.unwrap().len(), 0);
    }

    #[test]
    fn test_read_from() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(PersonEventTable::insert(&tx, "foo").is_ok());
        assert!(PersonEventTable::insert(&tx, "bar").is_ok());
        assert!(tx.commit().is_ok());

        let tx = conn.transaction().unwrap();
        let events = PersonEventTable::read(&tx, 2);
        assert!(tx.commit().is_ok());
        assert!(events.is_ok());
        let events = events.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0], "bar");
    }

    fn create_connection_and_table() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        let conn = conn.unwrap();
        assert!(PersonEventTable::create_table(&conn).is_ok());
        conn
    }
}