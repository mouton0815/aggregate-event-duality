use log::debug;
use rusqlite::{Connection, params, Result, Transaction};

pub enum EventTableType {
    PERSON,
    LOCATION
}

pub struct EventTable {
    table_name: &'static str
}

impl EventTable {
    pub fn new(conn: &Connection, event_table_type: EventTableType) -> Result<Self> {
        let table_name = match event_table_type {
            EventTableType::PERSON => "person_event",
            EventTableType::LOCATION => "location_event"
        };
        Self::create_table(conn, table_name)?;
        Ok(Self{ table_name })
    }

    fn create_table(conn: &Connection, table_name: &str) -> Result<()> {
        let stmt = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                revision INTEGER NOT NULL PRIMARY KEY,
                event TEXT NOT NULL
            )", table_name);
        debug!("Execute {}", stmt);
        conn.execute(stmt.as_str(), [])?;
        Ok(())
    }

    pub fn insert(&self, tx: &Transaction, event: &str) -> Result<u32> {
        let stmt = format!("INSERT INTO {} (event) VALUES (?)", self.table_name);
        debug!("Execute {} with: {}", stmt, event);
        tx.execute(stmt.as_str(), params![event])?;
        Ok(tx.last_insert_rowid() as u32)
    }

    pub fn read(&self, tx: &Transaction, from_revision: u32) -> Result<Vec<String>> {
        let stmt = format!("SELECT event FROM {} WHERE revision >= ? ORDER BY revision", self.table_name);
        debug!("Execute {} with: {}", stmt, from_revision);
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
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use crate::database::event_table::{EventTable, EventTableType};

    #[test]
    fn test_insert() {
        let (mut conn, table) = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let revision = table.insert(&tx, "foo");
        assert!(tx.commit().is_ok());
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), 1);
    }

    #[test]
    fn test_read_from_empty() {
        let (mut conn, table) = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let events = table.read(&tx, 1);
        assert!(tx.commit().is_ok());
        assert!(events.is_ok());
        assert_eq!(events.unwrap().len(), 0);
    }

    #[test]
    fn test_read_from() {
        let (mut conn, table) = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(table.insert(&tx, "foo").is_ok());
        assert!(table.insert(&tx, "bar").is_ok());
        assert!(tx.commit().is_ok());

        let tx = conn.transaction().unwrap();
        let events = table.read(&tx, 2);
        assert!(tx.commit().is_ok());
        assert!(events.is_ok());
        let events = events.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0], "bar");
    }

    fn create_connection_and_table() -> (Connection, EventTable) {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        let conn = conn.unwrap();
        let table = EventTable::new(&conn, EventTableType::PERSON);
        assert!(table.is_ok());
        (conn, table.unwrap())
    }
}