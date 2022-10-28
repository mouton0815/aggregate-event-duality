use const_format::formatcp;
use rusqlite::{Connection, params, Result, ToSql, Transaction};
use crate::company_aggregate::CompanyAggregate;
use crate::company_patch::CompanyPatch;

const COMPANY_AGGREGATE_TABLE : &'static str = "company_aggregate";

const CREATE_COMPANY_TABLE : &'static str = formatcp!("
    CREATE TABLE IF NOT EXISTS {} (
        companyId INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        location TEXT,
        vatId INTEGER,
        employees INTEGER
    )",
    COMPANY_AGGREGATE_TABLE
);

const INSERT_COMPANY : &'static str = formatcp!("
    INSERT INTO {} (companyId, name, location, vatId, employees) VALUES (?, ?, ?, ?, ?)
    ON CONFLICT DO NOTHING",
    COMPANY_AGGREGATE_TABLE
);

const DELETE_COMPANY : &'static str = formatcp!("
    DELETE FROM {} WHERE companyId = ?",
    COMPANY_AGGREGATE_TABLE
);

const SELECT_COMPANIES : &'static str = formatcp!("
    SELECT companyId, name, location, vatId, employees FROM {} ORDER BY companyId",
    COMPANY_AGGREGATE_TABLE
);

pub struct CompanyAggregateDAO {
}

impl CompanyAggregateDAO {

    pub fn create_table(conn: &Connection) -> Result<()> {
        conn.execute(CREATE_COMPANY_TABLE, [])?;
        Ok(())
    }

    pub fn insert(tx: &Transaction, company_id: u32, company: &CompanyPatch) -> Result<()> {
        let values = params![company_id, company.name, company.location, company.vat_id, company.employees];
        tx.execute(INSERT_COMPANY, values)?;
        Ok(())
    }

    pub fn update(tx: &Transaction, company_id: u32, company: &CompanyPatch) -> Result<()> {
        let mut columns = Vec::new();
        let mut values: Vec<&dyn ToSql> = Vec::new();
        if !company.name.is_none() {
            columns.push("name=?");
            values.push(&company.name);
        }
        if !company.location.is_absent() {
            columns.push("location=?");
            values.push(&company.location);
        }
        if !company.vat_id.is_absent() {
            columns.push("vatId=?");
            values.push(&company.vat_id);
        }
        if !company.employees.is_absent() {
            columns.push("employees=?");
            values.push(&company.employees);
        }
        if columns.is_empty() {
            println!("Do not run update query because all non-id values are missing");
            return Ok(())
        }
        let query = format!("UPDATE {} SET {} WHERE companyId=?", COMPANY_AGGREGATE_TABLE, columns.join(",").as_str());
        values.push(&company_id);
        tx.execute(query.as_str(), values.as_slice())?;
        Ok(())
    }

    pub fn delete(tx: &Transaction, company_id: u32) -> Result<()> {
        tx.execute(DELETE_COMPANY, params![company_id])?;
        Ok(())
    }

    fn get_all(tx: &Transaction) -> Result<Vec<CompanyAggregate>> {
        let mut stmt = tx.prepare(SELECT_COMPANIES)?;
        let rows = stmt.query_map([], |row| {
            Ok(CompanyAggregate {
                company_id: row.get(0)?,
                name: row.get(1)?,
                location: row.get(2)?,
                vat_id: row.get(3)?,
                employees: row.get(4)?
            })
        })?;
        let mut companies = Vec::new();
        for row in rows {
            companies.push(row?);
        }
        Ok(companies)
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::{Connection, Result, Transaction};
    use crate::company_aggregate::CompanyAggregate;
    use crate::database::company_aggregate_dao::CompanyAggregateDAO;
    use crate::company_patch::CompanyPatch;
    use crate::patch::Patch;

    #[test]
    fn test_insert() {
        let company10 = CompanyPatch{
            name: Some(String::from("Foo")),
            location: Patch::Value(String::from("Germany")),
            vat_id: Patch::Value(123),
            employees: Patch::Value(50)
        };

        let company20 = CompanyPatch{
            name: Some(String::from("Baz")),
            location: Patch::Value(String::from("Spain")),
            vat_id: Patch::Absent,
            employees: Patch::Value(100)
        };

        let mut conn = create_connection();
        assert!(CompanyAggregateDAO::create_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();
        assert!(CompanyAggregateDAO::insert(&tx, 10, &company10).is_ok());
        assert!(CompanyAggregateDAO::insert(&tx, 20, &company20).is_ok());
        assert!(tx.commit().is_ok());

        let ref_companies = [
            &CompanyAggregate{
                company_id: 10,
                name: String::from("Foo"),
                location: Some(String::from("Germany")),
                vat_id: Some(123),
                employees: Some(50)
            },
            &CompanyAggregate{
                company_id: 20,
                name: String::from("Baz"),
                location: Some(String::from("Spain")),
                vat_id: None,
                employees: Some(100)
            }
        ];
        check_results(&mut conn, &ref_companies);
    }

    #[test]
    fn test_update() {
        let company = CompanyPatch{
            name: Some(String::from("Foo")),
            location: Patch::Value(String::from("Germany")),
            vat_id: Patch::Value(123),
            employees: Patch::Value(50)
        };

        let company_update = CompanyPatch{
            name: None,
            location: Patch::Null,
            vat_id: Patch::Absent,
            employees: Patch::Value(100)
        };

        let mut conn = create_connection();
        assert!(CompanyAggregateDAO::create_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();
        assert!(CompanyAggregateDAO::insert(&tx, 10, &company).is_ok());
        assert!(CompanyAggregateDAO::update(&tx, 10, &company_update).is_ok());
        assert!(tx.commit().is_ok());

        let ref_companies = [
            &CompanyAggregate{
                company_id: 10,
                name: String::from("Foo"),
                location: None,
                vat_id: Some(123),
                employees: Some(100)
            }
        ];
        check_results(&mut conn, &ref_companies);
    }

    #[test]
    fn test_delete() {
        let company = CompanyPatch{
            name: Some(String::from("Foo")),
            location: Patch::Value(String::from("Germany")),
            vat_id: Patch::Value(123),
            employees: Patch::Value(50)
        };

        let mut conn = create_connection();
        assert!(CompanyAggregateDAO::create_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();
        assert!(CompanyAggregateDAO::insert(&tx, 10, &company).is_ok());
        assert!(CompanyAggregateDAO::delete(&tx, 10).is_ok());
        assert!(tx.commit().is_ok());

        check_results(&mut conn, &[]);
    }

    fn create_connection() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        conn.unwrap()
    }

    fn check_results(conn: &mut Connection, ref_companies: &[&CompanyAggregate]) {
        let tx = conn.transaction();
        assert!(tx.is_ok());
        let tx = tx.unwrap();

        let companies = CompanyAggregateDAO::get_all(&tx);
        assert!(companies.is_ok());
        assert!(tx.commit().is_ok());

        let companies = companies.unwrap();
        assert_eq!(companies.len(), ref_companies.len());

        for (index, &ref_company) in ref_companies.iter().enumerate() {
            assert_eq!(companies[index], *ref_company);
        }
    }
}