use const_format::formatcp;
use rusqlite::{Connection, params, Result, Transaction};
use crate::company_event::CompanyEvent;

const COMPANY_EVENT_TABLE : &'static str = "company_event";

const CREATE_EVENT_TABLE : &'static str = formatcp!("
    CREATE TABLE IF NOT EXISTS {} (
        revision INTEGER NOT NULL PRIMARY KEY,
        event TEXT NOT NULL
    )",
    COMPANY_EVENT_TABLE
);

const INSERT_EVENT : &'static str = formatcp!("
    INSERT INTO {} (event) VALUES (?)",
    COMPANY_EVENT_TABLE
);

// TODO: DELETE_EVENTS_BEFORE

const SELECT_EVENTS : &'static str = formatcp!("
    SELECT event FROM {} WHERE revision >= ? ORDER BY revision",
    COMPANY_EVENT_TABLE
);

pub struct CompanyEventDAO {
}

impl CompanyEventDAO {

    pub fn create_table(conn: &Connection) -> Result<()> {
        conn.execute(CREATE_EVENT_TABLE, [])?;
        Ok(())
    }

    pub fn insert(tx: &Transaction, event: &CompanyEvent) -> Result<u32> {
        let json = serde_json::to_string(&event);
        tx.execute(INSERT_EVENT, params![json.unwrap()])?;
        Ok(tx.last_insert_rowid() as u32)
    }

    pub fn get_from(tx: &Transaction, from_revision: i64) -> Result<Vec<CompanyEvent>> {
        let mut stmt = tx.prepare(SELECT_EVENTS)?;
        let rows = stmt.query_map([from_revision], |row| {
            let json: String = row.get(0)?;
            Ok(json)
        })?;
        let mut events : Vec<CompanyEvent> = Vec::new();
        for row in rows {
            let event: Result<CompanyEvent, serde_json::Error> = serde_json::from_str(row?.as_str());
            match event {
                Ok(evt) => events.push(evt),
                Err(_) => return Err(rusqlite::Error::InvalidQuery), // TODO: Better error?
            }
            // events.push(event?);
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::{Connection, Transaction};
    use crate::company_event::{CompanyEvent, CompanyData};
    use crate::database::company_event_dao::CompanyEventDAO;
    use crate::patch::Patch;

    #[test]
    fn test_insert() {
        let mut conn = create_connection();
        assert!(CompanyEventDAO::create_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();
        let event = create_event();
        let revision = CompanyEventDAO::insert(&tx, &event);
        assert!(tx.commit().is_ok());
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), 1);
    }

    #[test]
    fn test_get_from_empty() {
        let mut conn = create_connection();
        assert!(CompanyEventDAO::create_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();
        let events = CompanyEventDAO::get_from(&tx, 1);
        assert!(tx.commit().is_ok());
        assert!(events.is_ok());
        assert_eq!(events.unwrap().len(), 0);
    }

    #[test]
    fn test_get_from() {
        let mut conn = create_connection();
        assert!(CompanyEventDAO::create_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();
        let event = create_event();
        assert!(CompanyEventDAO::insert(&tx, &event).is_ok());
        assert!(tx.commit().is_ok());

        let tx = conn.transaction().unwrap();
        let events = CompanyEventDAO::get_from(&tx, 1);
        assert!(tx.commit().is_ok());
        assert!(events.is_ok());
        let events = events.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0], event);
    }

    fn create_connection() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        conn.unwrap()
    }

    fn create_event() -> CompanyEvent {
        CompanyEvent{
            tenant_id: 1,
            company_id: 10,
            data: Patch::Value(CompanyData {
                name: Some(String::from("Foo")),
                location: Patch::Absent,
                vat_id: Patch::Value(124),
                employees: Patch::Absent,
            })
        }
    }
}
