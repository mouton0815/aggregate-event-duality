use const_format::formatcp;
use log::debug;
use rusqlite::{Result, Row, Transaction};
use crate::domain::location_map::LocationMap;
use crate::database::person_aggregate_table::PERSON_AGGREGATE_TABLE;
use crate::domain::person_data::PersonData;
use crate::domain::person_map::PersonMap;

const SELECT_LOCATIONS : &'static str = formatcp!("
    SELECT personId, name, location, spouseId FROM {} WHERE location IS NOT NULL ORDER BY location",
    PERSON_AGGREGATE_TABLE
);

pub fn read_location_aggregates(tx: &Transaction) -> Result<LocationMap> {
    debug!("Execute {}", SELECT_LOCATIONS);
    let mut stmt = tx.prepare(SELECT_LOCATIONS)?;
    let rows = stmt.query_map([], |row| {
        row_to_person_data(row) // TODO: Directly construct result map here? Or use cursor/iterator?
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

fn row_to_person_data(row: &Row) -> Result<(String, u32, PersonData)> {
    let person_id : u32 = row.get(0)?;
    let location : String = row.get(2)?; // Will exist as ensured by WHERE condition
    Ok((location.clone(), person_id, PersonData {
        name: row.get(1)?,
        location: Some(location),
        spouse_id: row.get(3)?
    }))
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use crate::database::location_aggregate_view::read_location_aggregates;
    use crate::database::person_aggregate_table::{create_person_aggregate_table, insert_person_aggregate};
    use crate::domain::location_map::LocationMap;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_map::PersonMap;

    #[test]
    fn test_no_table() {
        let mut conn = create_connection();
        let tx = conn.transaction();
        assert!(tx.is_ok());
        let result = read_location_aggregates(&tx.unwrap());
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
        let person = PersonData {
            name: String::from("Hans"),
            location: None, // No location
            spouse_id: None
        };

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(insert_person_aggregate(&tx, &person).is_ok());
        assert!(tx.commit().is_ok());

        let result = read_locations(&mut conn);
        assert_eq!(result, LocationMap::new());
    }

    #[test]
    fn test_read_aggregate_for_one() {
        let person = PersonData {
            name: String::from("Hans"),
            location: Some(String::from("Somewhere")),
            spouse_id: None
        };

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(insert_person_aggregate(&tx, &person).is_ok());
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
        let person1 = PersonData {
            name: String::from("Hans"),
            location: Some(String::from("Somewhere")),
            spouse_id: None
        };
        let person2 = PersonData {
            name: String::from("Inge"),
            location: Some(String::from("Somewhere")), // Same location as Hans
            spouse_id: None
        };

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(insert_person_aggregate(&tx, &person1).is_ok());
        assert!(insert_person_aggregate(&tx, &person2).is_ok());
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
        let person1 = PersonData {
            name: String::from("Hans"),
            location: Some(String::from("Somewhere")),
            spouse_id: None
        };
        let person2 = PersonData {
            name: String::from("Inge"),
            location: Some(String::from("Anywhere")),
            spouse_id: None
        };
        let person3 = PersonData {
            name: String::from("Fred"),
            location: Some(String::from("Somewhere")), // Same location as Hans
            spouse_id: None
        };

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(insert_person_aggregate(&tx, &person1).is_ok());
        assert!(insert_person_aggregate(&tx, &person2).is_ok());
        assert!(insert_person_aggregate(&tx, &person3).is_ok());
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
        assert!(create_person_aggregate_table(&conn).is_ok());
        conn
    }

    fn read_locations(conn: &mut Connection) -> LocationMap {
        let tx = conn.transaction();
        assert!(tx.is_ok());
        let tx = tx.unwrap();
        let result = read_location_aggregates(&tx);
        assert!(tx.commit().is_ok());
        assert!(result.is_ok());
        result.unwrap()
    }
}