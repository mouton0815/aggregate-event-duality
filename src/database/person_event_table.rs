use const_format::formatcp;
use log::debug;
use rusqlite::{Connection, params, Result, Transaction};
use crate::domain::person_event::PersonEvent;

const PERSON_EVENT_TABLE : &'static str = "person_event";

const CREATE_PERSON_EVENT_TABLE : &'static str = formatcp!("
    CREATE TABLE IF NOT EXISTS {} (
        revision INTEGER NOT NULL PRIMARY KEY,
        event TEXT NOT NULL
    )",
    PERSON_EVENT_TABLE
);

const INSERT_PERSON_EVENT : &'static str = formatcp!("
    INSERT INTO {} (event) VALUES (?)",
    PERSON_EVENT_TABLE
);

// TODO: DELETE_PERSON_EVENTS_BEFORE

const SELECT_PERSON_EVENTS : &'static str = formatcp!("
    SELECT event FROM {} WHERE revision >= ? ORDER BY revision",
    PERSON_EVENT_TABLE
);

pub fn create_person_event_table(conn: &Connection) -> Result<()> {
    debug!("Execute {}", CREATE_PERSON_EVENT_TABLE);
    conn.execute(CREATE_PERSON_EVENT_TABLE, [])?;
    Ok(())
}

pub fn insert_person_event(tx: &Transaction, person_event: &PersonEvent) -> Result<u32> {
    match serde_json::to_string(&person_event) {
        Ok(json) => {
            debug!("Execute {} with: {}", INSERT_PERSON_EVENT, json);
            tx.execute(INSERT_PERSON_EVENT, params![json])?;
            Ok(tx.last_insert_rowid() as u32)
        },
        Err(error) => {
            Err(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        }
    }
}

pub fn read_person_events(tx: &Transaction, from_revision: u32) -> Result<Vec<String>> {
    debug!("Execute {} with: {}", SELECT_PERSON_EVENTS, from_revision);
    let mut stmt = tx.prepare(SELECT_PERSON_EVENTS)?;
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
    use crate::database::person_event_table::{create_person_event_table, insert_person_event, read_person_events};
    use crate::domain::person_event::PersonEvent;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;

    #[test]
    fn test_insert() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let person_event = create_person_event();
        let revision = insert_person_event(&tx, &person_event);
        assert!(tx.commit().is_ok());
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), 1);
    }

    #[test]
    fn test_read_from_empty() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let events = read_person_events(&tx, 1);
        assert!(tx.commit().is_ok());
        assert!(events.is_ok());
        assert_eq!(events.unwrap().len(), 0);
    }

    #[test]
    fn test_read_from() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let person_event = create_person_event();
        assert!(insert_person_event(&tx, &person_event).is_ok());
        assert!(tx.commit().is_ok());

        let tx = conn.transaction().unwrap();
        let events = read_person_events(&tx, 1);
        assert!(tx.commit().is_ok());
        assert!(events.is_ok());
        let events = events.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0], r#"{"5":{"name":"Hans","location":null}}"#);
    }

    fn create_connection_and_table() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        let conn = conn.unwrap();
        assert!(create_person_event_table(&conn).is_ok());
        conn
    }

    fn create_person_event() -> PersonEvent {
        PersonEvent::of(5, Some(PersonPatch {
            name: Some("Hans".to_string()),
            location: Patch::Absent,
            spouse_id: Patch::Absent
        }))
    }
}
