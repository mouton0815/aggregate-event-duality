use std::collections::BTreeMap;
use log::debug;
use rusqlite::{Connection, OptionalExtension, params, Result, Row, Transaction};
use crate::domain::location_data::LocationData;

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

// TODO: Should be encapsulated and moved to its own file in /domain
type LocationMap = BTreeMap<String, LocationData>;

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
            location_map.insert(name, location_data);
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

    /*
    pub fn select_all(tx: &Transaction) -> Result<LocationMap> {
        debug!("Execute {}", SELECT_LOCATIONS);
        let mut stmt = tx.prepare(SELECT_LOCATIONS)?;
        let rows = stmt.query_map([], |row| {
            Self::row_to_person_data(row) // TODO: Directly construct result map here? Or use cursor/iterator?
        })?;
        let mut location_map = LocationMap::new();
        let mut last_location : Option<String> = None;
        let mut person_map = PersonMap::new();
        for row in rows {
            let (location, person_id, person_data) = row?;
            if last_location.is_some() && last_location.as_ref().unwrap() != &location {
                location_map.put(last_location.unwrap().as_str(), person_map);
                person_map = PersonMap::new();
            }
            person_map.put(person_id, person_data);
            last_location = Some(location);
        }
        if person_map.len() > 0 {
            location_map.put(last_location.unwrap().as_str(), person_map);
        }
        Ok(location_map)
    }
    */

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

    /*
    use rusqlite::Connection;
    use crate::database::location_table::LocationView;
    use crate::database::person_table::PersonTable;
    use crate::domain::location_map::LocationMap;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_map::PersonMap;

    #[test]
    fn test_no_table() {
        let mut conn = create_connection();
        let tx = conn.transaction();
        assert!(tx.is_ok());
        let result = LocationView::select_all(&tx.unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_read_aggregate_for_no_entry() {
        let mut conn = create_connection_and_table();
        let result = read_locations(&mut conn);
        assert_eq!(result, LocationMap::new());
    }

    #[test]
    fn test_read_aggregate_for_no_location() {
        let person = PersonData::new("Hans", None, None);

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(PersonTable::insert(&tx, &person).is_ok());
        assert!(tx.commit().is_ok());

        let result = read_locations(&mut conn);
        assert_eq!(result, LocationMap::new());
    }

    #[test]
    fn test_read_aggregate_for_one() {
        let person = PersonData::new("Hans", Some("Somewhere"), None);

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(PersonTable::insert(&tx, &person).is_ok());
        assert!(tx.commit().is_ok());

        let mut person_map = PersonMap::new();
        person_map.put(1, person);
        let mut location_map = LocationMap::new();
        location_map.put("Somewhere", person_map);

        let result = read_locations(&mut conn);
        assert_eq!(result, location_map);
    }

    #[test]
    fn test_read_aggregate_for_one_batch() {
        let person1 = PersonData::new("Hans", Some("Somewhere"), None);
        let person2 = PersonData::new("Inge", Some("Somewhere"), None);

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(PersonTable::insert(&tx, &person1).is_ok());
        assert!(PersonTable::insert(&tx, &person2).is_ok());
        assert!(tx.commit().is_ok());

        let mut person_map = PersonMap::new();
        person_map.put(1, person1);
        person_map.put(2, person2);
        let mut location_map = LocationMap::new();
        location_map.put("Somewhere", person_map);

        let result = read_locations(&mut conn);
        assert_eq!(result, location_map);
    }


    #[test]
    fn test_read_aggregate_for_two_batches() {
        let person1 = PersonData::new("Hans", Some("Somewhere"), None);
        let person2 = PersonData::new("Inge", Some("Anywhere"), None);
        let person3 = PersonData::new("Fred", Some("Somewhere"), None);

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(PersonTable::insert(&tx, &person1).is_ok());
        assert!(PersonTable::insert(&tx, &person2).is_ok());
        assert!(PersonTable::insert(&tx, &person3).is_ok());
        assert!(tx.commit().is_ok());

        let mut person_map1 = PersonMap::new();
        let mut person_map2 = PersonMap::new();
        person_map1.put(1, person1);
        person_map2.put(2, person2);
        person_map1.put(3, person3);
        let mut location_map = LocationMap::new();
        location_map.put("Somewhere", person_map1);
        location_map.put("Anywhere", person_map2);

        let result = read_locations(&mut conn);
        assert_eq!(result, location_map);
    }

    fn create_connection() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        conn.unwrap()
    }

    fn create_connection_and_table() -> Connection {
        let conn = create_connection();
        assert!(PersonTable::create_table(&conn).is_ok());
        conn
    }

    fn read_locations(conn: &mut Connection) -> LocationMap {
        let tx = conn.transaction();
        assert!(tx.is_ok());
        let tx = tx.unwrap();
        let result = LocationView::select_all(&tx);
        assert!(tx.commit().is_ok());
        assert!(result.is_ok());
        result.unwrap()
    }
    */
}