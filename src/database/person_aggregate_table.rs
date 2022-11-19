use const_format::formatcp;
use log::{debug, error};
use rusqlite::{Connection, Error, OptionalExtension, params, Result, Row, ToSql, Transaction};
use crate::domain::person_data::PersonData;
use crate::domain::person_map::PersonMap;
use crate::domain::person_patch::PersonPatch;

const PERSON_AGGREGATE_TABLE: &'static str = "person_aggregate";

const CREATE_PERSON_TABLE : &'static str = formatcp!("
    CREATE TABLE IF NOT EXISTS {} (
        personId INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        location TEXT,
        spouseId INTEGER
    )",
    PERSON_AGGREGATE_TABLE
);

const INSERT_PERSON : &'static str = formatcp!("
    INSERT INTO {} (name, location, spouseId) VALUES (?, ?, ?)
    ON CONFLICT DO NOTHING",
    PERSON_AGGREGATE_TABLE
);

const DELETE_PERSON : &'static str = formatcp!("
    DELETE FROM {} WHERE personId = ?",
    PERSON_AGGREGATE_TABLE
);

const SELECT_PERSONS : &'static str = formatcp!("
    SELECT personId, name, location, spouseId FROM {} ORDER BY personId",
    PERSON_AGGREGATE_TABLE
);

const SELECT_PERSON : &'static str = formatcp!("
    SELECT personId, name, location, spouseId FROM {} WHERE personId = ?",
    PERSON_AGGREGATE_TABLE
);

pub fn create_person_aggregate_table(conn: &Connection) -> Result<()> {
    debug!("Execute {}", CREATE_PERSON_TABLE);
    conn.execute(CREATE_PERSON_TABLE, [])?;
    Ok(())
}

pub fn insert_person_aggregate(tx: &Transaction, person: &PersonData) -> Result<u32> {
    debug!("Execute {}\nwith: {:?}", INSERT_PERSON, person);
    let values = params![person.name, person.location, person.spouse_id];
    tx.execute(INSERT_PERSON, values)?;
    Ok(tx.last_insert_rowid() as u32)
}

pub fn update_person_aggregate(tx: &Transaction, person_id: u32, person: &PersonPatch) -> Result<bool> {
    let mut columns = Vec::new();
    let mut values: Vec<&dyn ToSql> = Vec::new();
    if !person.name.is_none() {
        columns.push("name=?");
        values.push(&person.name);
    }
    if !person.location.is_absent() {
        columns.push("location=?");
        values.push(&person.location);
    }
    if !person.spouse_id.is_absent() {
        columns.push("spouseId=?");
        values.push(&person.spouse_id);
    }
    if columns.is_empty() {
        error!("Do not run update query because all non-id values are missing");
        return Err(Error::InvalidParameterCount(0, 5));
    }
    let query = format!("UPDATE {} SET {} WHERE personId=?", PERSON_AGGREGATE_TABLE, columns.join(",").as_str());
    values.push(&person_id);
    debug!("Execute\n{}\nwith: {:?}", query, person);
    let row_count = tx.execute(query.as_str(), values.as_slice())?;
    Ok(row_count == 1)
}

pub fn delete_person_aggregate(tx: &Transaction, person_id: u32) -> Result<bool> {
    debug!("Execute {} with: {}", DELETE_PERSON, person_id);
    let row_count = tx.execute(DELETE_PERSON, params![person_id])?;
    Ok(row_count == 1)
}

pub fn read_person_aggregates(tx: &Transaction) -> Result<PersonMap> {
    debug!("Execute {}", SELECT_PERSONS);
    let mut stmt = tx.prepare(SELECT_PERSONS)?;
    let rows = stmt.query_map([], |row| {
        row_to_person_data(row)
    })?;
    let mut person_map = PersonMap::new();
    for row in rows {
        let (person_id, person_data) = row?;
        person_map.put(person_id, Some(person_data));
    }
    Ok(person_map)
}

pub fn read_person_aggregate(tx: &Transaction, person_id: u32) -> Result<Option<PersonData>> {
    debug!("Execute {} with: {}", SELECT_PERSON, person_id);
    let mut stmt = tx.prepare(SELECT_PERSON)?;
    stmt.query_row([person_id], |row | {
        Ok(row_to_person_data(row)?.1)
    }).optional()
}

fn row_to_person_data(row: &Row) -> Result<(u32, PersonData)> {
    Ok((row.get(0)?, PersonData {
        name: row.get(1)?,
        location: row.get(2)?,
        spouse_id: row.get(3)?
    }))
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use crate::database::person_aggregate_table::{create_person_aggregate_table, delete_person_aggregate, insert_person_aggregate, read_person_aggregate, read_person_aggregates, update_person_aggregate};
    use crate::domain::person_data::PersonData;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;

    #[test]
    fn test_insert() {
        let person1 = PersonData {
            name: String::from("Hans"),
            location: Some(String::from("Germany")),
            spouse_id: Some(123)
        };
        let person2 = PersonData {
            name: String::from("Inge"),
            location: Some(String::from("Spain")),
            spouse_id: None
        };

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let person_id1 = insert_person_aggregate(&tx, &person1);
        assert!(person_id1.is_ok());
        assert_eq!(person_id1.unwrap(), 1);
        let person_id2 = insert_person_aggregate(&tx, &person2);
        assert!(person_id2.is_ok());
        assert_eq!(person_id2.unwrap(), 2);
        assert!(tx.commit().is_ok());

        let ref_persons = [
            (1, &PersonData {
                name: String::from("Hans"),
                location: Some(String::from("Germany")),
                spouse_id: Some(123)
            }),
            (2, &PersonData {
                name: String::from("Inge"),
                location: Some(String::from("Spain")),
                spouse_id: None
            })
        ];
        check_results(&mut conn, &ref_persons);
        check_single_result(&mut conn, ref_persons[0]);
        check_single_result(&mut conn, ref_persons[1]);
    }

    #[test]
    fn test_update() {
        let person = PersonData {
            name: String::from("Hans"),
            location: Some(String::from("Germany")),
            spouse_id: Some(123)
        };

        let person_update = PersonPatch {
            name: None,
            location: Patch::Null,
            spouse_id: Patch::Value(100)
        };

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(insert_person_aggregate(&tx, &person).is_ok());
        let result = update_person_aggregate(&tx, 1, &person_update);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        assert!(tx.commit().is_ok());

        let ref_persons = [
            (1, &PersonData {
                name: String::from("Hans"),
                location: None,
                spouse_id: Some(100)
            })
        ];
        check_results(&mut conn, &ref_persons);
        check_single_result(&mut conn, ref_persons[0]);
    }
    #[test]

    fn test_update_missing() {
        let person_update = PersonPatch {
            name: None,
            location: Patch::Null,
            spouse_id: Patch::Absent
        };

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let result = update_person_aggregate(&tx, 1, &person_update);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_delete() {
        let person = PersonData {
            name: String::from("Hans"),
            location: Some(String::from("Germany")),
            spouse_id: Some(123)
        };

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(insert_person_aggregate(&tx, &person).is_ok());
        let result = delete_person_aggregate(&tx, 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        assert!(tx.commit().is_ok());

        check_results(&mut conn, &[]);
    }

    #[test]
    fn test_delete_missing() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let result = delete_person_aggregate(&tx, 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
        assert!(tx.commit().is_ok());
    }

    fn create_connection_and_table() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        let conn = conn.unwrap();
        assert!(create_person_aggregate_table(&conn).is_ok());
        conn
    }

    fn check_results(conn: &mut Connection, ref_persons: &[(u32, &PersonData)]) {
        let tx = conn.transaction().unwrap();

        let persons = read_person_aggregates(&tx);
        assert!(persons.is_ok());
        assert!(tx.commit().is_ok());

        let persons = persons.unwrap();
        assert_eq!(persons.len(), ref_persons.len());

        for (_, &ref_person) in ref_persons.iter().enumerate() {
            let (person_id, person_data) = ref_person;
            let person = persons.get(person_id);
            assert!(person.is_some());
            let person = person.unwrap();
            assert_eq!(person, person_data);
        }
    }

    fn check_single_result(conn: &mut Connection, ref_person: (u32, &PersonData)) {
        let tx = conn.transaction().unwrap();

        let person = read_person_aggregate(&tx, ref_person.0);
        assert!(person.is_ok());
        assert!(tx.commit().is_ok());

        let person = person.unwrap();
        assert!(person.is_some());
        assert_eq!(person.unwrap(), *ref_person.1);
    }
}