use log::debug;
use rusqlite::{Connection, OptionalExtension, params, Result, Row, Transaction};
use crate::domain::location_data::LocationData;
use crate::domain::location_map::LocationMap;

const CREATE_LOCATION_TABLE : &'static str =
    "CREATE TABLE IF NOT EXISTS location (
        name TEXT NOT NULL PRIMARY KEY,
        total INTEGER NOT NULL,
        married INTEGER NOT NULL
    )";

const UPSERT_LOCATION : &'static str =
    "INSERT INTO location (name, total, married) VALUES (?, ?, ?)
     ON CONFLICT(name) DO UPDATE SET total = excluded.total, married = excluded.married";

const DELETE_LOCATION : &'static str =
    "DELETE FROM location WHERE name = ?";

const SELECT_LOCATION : &'static str =
    "SELECT name, total, married FROM location WHERE name = ?";

const SELECT_LOCATIONS : &'static str =
    "SELECT name, total, married FROM location";

pub struct LocationTable;

impl LocationTable {

    pub fn create_table(conn: &Connection) -> Result<()> {
        debug!("Execute\n{}", CREATE_LOCATION_TABLE);
        conn.execute(CREATE_LOCATION_TABLE, [])?;
        Ok(())
    }

    pub fn upsert(tx: &Transaction, name: &str, location: &LocationData) -> Result<()> {
        debug!("Execute\n{}\nwith {}: {:?}", UPSERT_LOCATION, name, location);
        let values = params![name, location.total, location.married];
        tx.execute(UPSERT_LOCATION, values)?;
        Ok(())
    }

    pub fn delete(tx: &Transaction, name: &str) -> Result<bool> {
        debug!("Execute\n{} with: {}", DELETE_LOCATION, name);
        let row_count = tx.execute(DELETE_LOCATION, params![name])?;
        Ok(row_count == 1)
    }

    pub fn select_all(tx: &Transaction) -> Result<LocationMap> {
        debug!("Execute\n{}", SELECT_LOCATIONS);
        let mut stmt = tx.prepare(SELECT_LOCATIONS)?;
        let rows = stmt.query_map([], |row| {
            Self::row_to_location_data(row)
        })?;
        let mut location_map = LocationMap::new();
        for row in rows {
            let (name, location_data) = row?;
            location_map.put(&name, location_data);
        }
        Ok(location_map)
    }

    pub fn select_by_name(tx: &Transaction, name: &str) -> Result<Option<LocationData>> {
        debug!("Execute\n{} with: {}", SELECT_LOCATION, name);
        let mut stmt = tx.prepare(SELECT_LOCATION)?;
        stmt.query_row([name], |row | {
            Ok(Self::row_to_location_data(row)?.1)
        }).optional()
    }

    fn row_to_location_data(row: &Row) -> Result<(String, LocationData)> {
        Ok((row.get(0)?, LocationData {
            total: row.get(1)?,
            married: row.get(2)?
        }))
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use crate::database::location_table::LocationTable;
    use crate::domain::location_data::LocationData;

    #[test]
    fn test_upsert() {
        let location1 = LocationData::new(1, 3);
        let location2 = LocationData::new(2, 0);
        let location3 = LocationData::new(7, 5);

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(LocationTable::upsert(&tx, "foo", &location1).is_ok());
        assert!(LocationTable::upsert(&tx, "bar", &location2).is_ok());
        assert!(LocationTable::upsert(&tx, "foo", &location3).is_ok()); // Update
        assert!(tx.commit().is_ok());

        let ref_locations = [
            ("foo", &LocationData::new(7, 5)),
            ("bar", &LocationData::new(2, 0))
        ];
        check_results(&mut conn, &ref_locations);
    }

    #[test]
    fn test_delete() {
        let location = LocationData::new(1, 3);

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(LocationTable::upsert(&tx, "foo", &location).is_ok());
        let result = LocationTable::delete(&tx, "foo");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        assert!(tx.commit().is_ok());

        check_results(&mut conn, &[]);
    }

    #[test]
    fn test_delete_missing() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let result = LocationTable::delete(&tx, "foo");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
        assert!(tx.commit().is_ok());
    }

    fn create_connection_and_table() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        let conn = conn.unwrap();
        assert!(LocationTable::create_table(&conn).is_ok());
        conn
    }

    fn check_results(conn: &mut Connection, ref_locations: &[(&str, &LocationData)]) {
        let tx = conn.transaction().unwrap();

        let locations = LocationTable::select_all(&tx);
        assert!(locations.is_ok());
        assert!(tx.commit().is_ok());

        let locations = locations.unwrap();
        assert_eq!(locations.len(), ref_locations.len());

        for (_, &ref_location) in ref_locations.iter().enumerate() {
            let (name, location_data) = ref_location;
            let location = locations.get(name).unwrap();
            assert_eq!(location, location_data);
        }
    }
}