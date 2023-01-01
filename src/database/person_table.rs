use log::{debug, error};
use rusqlite::{Connection, Error, OptionalExtension, params, Result, Row, ToSql, Transaction};
use crate::domain::person_data::PersonData;
use crate::domain::person_id::PersonId;
use crate::domain::person_map::PersonMap;
use crate::domain::person_patch::PersonPatch;

const CREATE_PERSON_TABLE : &'static str =
    "CREATE TABLE IF NOT EXISTS person (
        personId INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        city TEXT,
        spouse INTEGER
    )";

const INSERT_PERSON : &'static str =
    "INSERT INTO person (name, city, spouse) VALUES (?, ?, ?)";

const DELETE_PERSON : &'static str =
    "DELETE FROM person WHERE personId = ?";

const SELECT_PERSONS : &'static str =
    "SELECT personId, name, city, spouse FROM person";

const SELECT_PERSON : &'static str =
    "SELECT personId, name, city, spouse FROM person WHERE personId = ?";


pub struct PersonTable;

impl PersonTable {
    pub fn create_table(conn: &Connection) -> Result<()> {
        debug!("Execute\n{}", CREATE_PERSON_TABLE);
        conn.execute(CREATE_PERSON_TABLE, [])?;
        Ok(())
    }

    pub fn insert(tx: &Transaction, person: &PersonData) -> Result<PersonId> {
        debug!("Execute\n{}\nwith: {:?}", INSERT_PERSON, person);
        let values = params![person.name, person.city, person.spouse];
        tx.execute(INSERT_PERSON, values)?;
        Ok(PersonId::from(tx.last_insert_rowid() as u64))
    }

    pub fn update(tx: &Transaction, person_id: PersonId, person: &PersonPatch) -> Result<PersonData> {
        let mut columns = Vec::new();
        let mut values: Vec<&dyn ToSql> = Vec::new();
        if !person.name.is_none() {
            columns.push("name=?");
            values.push(&person.name);
        }
        if !person.city.is_absent() {
            columns.push("city=?");
            values.push(&person.city);
        }
        if !person.spouse.is_absent() {
            columns.push("spouse=?");
            values.push(&person.spouse);
        }
        if columns.is_empty() {
            error!("Do not run update query because all non-id values are missing");
            return Err(Error::InvalidParameterCount(0, 5));
        }
        let query = format!("UPDATE person SET {} WHERE personId=?", columns.join(",").as_str());
        values.push(&person_id);
        debug!("Execute\n{}\nwith: {:?}", query, person);
        tx.execute(query.as_str(), values.as_slice())?;
        Self::select_by_id_internal(&tx, person_id)
    }

    pub fn delete(tx: &Transaction, person_id: PersonId) -> Result<bool> {
        debug!("Execute\n{} with: {}", DELETE_PERSON, person_id);
        let row_count = tx.execute(DELETE_PERSON, params![person_id])?;
        Ok(row_count == 1)
    }

    pub fn select_all(tx: &Transaction) -> Result<PersonMap> {
        debug!("Execute\n{}", SELECT_PERSONS);
        let mut stmt = tx.prepare(SELECT_PERSONS)?;
        let rows = stmt.query_map([], |row| {
            Self::row_to_person_data(row)
        })?;
        let mut person_map = PersonMap::new();
        for row in rows {
            let (person_id, person_data) = row?;
            person_map.put(person_id, person_data);
        }
        Ok(person_map)
    }

    pub fn select_by_id(tx: &Transaction, person_id: PersonId) -> Result<Option<PersonData>> {
        Self::select_by_id_internal(tx, person_id).optional()
    }

    pub fn select_by_id_internal(tx: &Transaction, person_id: PersonId) -> Result<PersonData> {
        debug!("Execute\n{} with: {}", SELECT_PERSON, person_id);
        let mut stmt = tx.prepare(SELECT_PERSON)?;
        stmt.query_row([person_id], |row | {
            Ok(Self::row_to_person_data(row)?.1)
        })
    }

    fn row_to_person_data(row: &Row) -> Result<(PersonId, PersonData)> {
        Ok((row.get(0)?, PersonData {
            name: row.get(1)?,
            city: row.get(2)?,
            spouse: row.get(3)?
        }))
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use crate::database::person_table::PersonTable;
    use crate::domain::person_data::PersonData;
    use crate::domain::person_id::PersonId;
    use crate::domain::person_patch::PersonPatch;
    use crate::util::patch::Patch;

    #[test]
    fn test_insert() {
        let person1 = PersonData::new("Hans", Some("Germany"), Some(PersonId::from(123)));
        let person2 = PersonData::new("Inge", Some("Spain"), None);

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let person_id1 = PersonTable::insert(&tx, &person1);
        assert!(person_id1.is_ok());
        assert_eq!(person_id1.unwrap(), PersonId::from(1));
        let person_id2 = PersonTable::insert(&tx, &person2);
        assert!(person_id2.is_ok());
        assert_eq!(person_id2.unwrap(), PersonId::from(2));
        assert!(tx.commit().is_ok());

        let ref_persons = [
            (PersonId::from(1), &PersonData::new("Hans", Some("Germany"), Some(PersonId::from(123)))),
            (PersonId::from(2), &PersonData::new("Inge", Some("Spain"), None))
        ];
        check_results(&mut conn, &ref_persons);
        check_single_result(&mut conn, ref_persons[0]);
        check_single_result(&mut conn, ref_persons[1]);
    }

    #[test]
    fn test_update() {
        let person = PersonData::new("Hans", Some("Germany"), Some(PersonId::from(123)));
        let person_update = PersonPatch::new(None, Patch::Null, Patch::Value(PersonId::from(100)));
        let person_ref = PersonData::new("Hans", None, Some(PersonId::from(100)));

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(PersonTable::insert(&tx, &person).is_ok());
        let result = PersonTable::update(&tx, PersonId::from(1), &person_update);
        assert!(result.is_ok());
        let result = result.as_ref().unwrap();
        assert_eq!(result, &person_ref);
        assert!(tx.commit().is_ok());

        let ref_persons = [(PersonId::from(1), &person_ref)];
        check_results(&mut conn, &ref_persons);
        check_single_result(&mut conn, ref_persons[0]);
    }

    #[test]
    fn test_update_missing() {
        let person_update = PersonPatch::new(None, Patch::Null, Patch::Absent);

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(PersonTable::update(&tx, PersonId::from(1), &person_update).is_err());
    }

    #[test]
    fn test_delete() {
        let person = PersonData::new("Hans", Some("Germany"), Some(PersonId::from(123)));

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(PersonTable::insert(&tx, &person).is_ok());
        let result = PersonTable::delete(&tx, PersonId::from(1));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
        assert!(tx.commit().is_ok());

        check_results(&mut conn, &[]);
    }

    #[test]
    fn test_delete_missing() {
        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let result = PersonTable::delete(&tx, PersonId::from(1));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
        assert!(tx.commit().is_ok());
    }

    fn create_connection_and_table() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        let conn = conn.unwrap();
        assert!(PersonTable::create_table(&conn).is_ok());
        conn
    }

    fn check_results(conn: &mut Connection, ref_persons: &[(PersonId, &PersonData)]) {
        let tx = conn.transaction().unwrap();

        let persons = PersonTable::select_all(&tx);
        assert!(persons.is_ok());
        assert!(tx.commit().is_ok());

        let persons = persons.unwrap();
        assert_eq!(persons.len(), ref_persons.len());

        for (_, &ref_person) in ref_persons.iter().enumerate() {
            let (person_id, person_data) = ref_person;
            let person = persons.get(person_id);
            assert_eq!(person, person_data);
        }
    }

    fn check_single_result(conn: &mut Connection, ref_person: (PersonId, &PersonData)) {
        let tx = conn.transaction().unwrap();

        let person = PersonTable::select_by_id(&tx, ref_person.0);
        assert!(person.is_ok());
        assert!(tx.commit().is_ok());

        let person = person.unwrap();
        assert!(person.is_some());
        assert_eq!(person.unwrap(), *ref_person.1);
    }
}