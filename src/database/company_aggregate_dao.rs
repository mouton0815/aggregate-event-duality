use const_format::formatcp;
use rusqlite::{Connection, params, Result, ToSql, Transaction};
use crate::company_aggregate::CompanyAggregate;
use crate::company_event::CompanyData;
use crate::domain::company_post::CompanyPost;
use crate::domain::company_put::CompanyPut;

const COMPANY_AGGREGATE_TABLE : &'static str = "company_aggregate";

const CREATE_COMPANY_TABLE : &'static str = formatcp!("
    CREATE TABLE IF NOT EXISTS {} (
        companyId INTEGER NOT NULL PRIMARY KEY,
        tenantId INTEGER NOT NULL,
        name TEXT NOT NULL,
        location TEXT,
        vatId INTEGER,
        employees INTEGER
    )",
    COMPANY_AGGREGATE_TABLE
);

const INSERT_COMPANY : &'static str = formatcp!("
    INSERT INTO {} (tenantId, name, location, vatId, employees) VALUES (?, ?, ?, ?, ?)
    ON CONFLICT DO NOTHING",
    COMPANY_AGGREGATE_TABLE
);

const DELETE_COMPANY : &'static str = formatcp!("
    DELETE FROM {} WHERE companyId = ?",
    COMPANY_AGGREGATE_TABLE
);

const SELECT_COMPANIES : &'static str = formatcp!("
    SELECT companyId, tenantId, name, location, vatId, employees FROM {} ORDER BY companyId",
    COMPANY_AGGREGATE_TABLE
);

const SELECT_COMPANY : &'static str = formatcp!("
    SELECT companyId, tenantId, name, location, vatId, employees FROM {} WHERE companyId = ?",
    COMPANY_AGGREGATE_TABLE
);

pub struct CompanyAggregateDAO {
}

impl CompanyAggregateDAO {

    pub fn create_table(conn: &Connection) -> Result<()> {
        conn.execute(CREATE_COMPANY_TABLE, [])?;
        Ok(())
    }

    pub fn insert(tx: &Transaction, company: &CompanyPost) -> Result<u32> {
        let values = params![company.tenant_id, company.name, company.location, company.vat_id, company.employees];
        tx.execute(INSERT_COMPANY, values)?;
        Ok(tx.last_insert_rowid() as u32)
    }

    pub fn update(tx: &Transaction, company_id: u32, company: &CompanyPut) -> Result<()> {
        let mut columns = Vec::new();
        let mut values: Vec<&dyn ToSql> = Vec::new();
        if !company.tenant_id.is_none() {
            columns.push("tenantId=?");
            values.push(&company.tenant_id);
        }
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

    pub fn read_all(tx: &Transaction) -> Result<Vec<CompanyAggregate>> {
        let mut stmt = tx.prepare(SELECT_COMPANIES)?;
        let rows = stmt.query_map([], |row| {
            Ok(CompanyAggregate {
                company_id: row.get(0)?,
                tenant_id: row.get(1)?,
                name: row.get(2)?,
                location: row.get(3)?,
                vat_id: row.get(4)?,
                employees: row.get(5)?
            })
        })?;
        let mut companies = Vec::new();
        for row in rows {
            companies.push(row?);
        }
        Ok(companies)
    }

    pub fn read(tx: &Transaction, company_id: u32) -> Result<CompanyAggregate> {
        let mut stmt = tx.prepare(SELECT_COMPANY)?;
        let row = stmt.query_row([company_id], |row| {
            Ok(CompanyAggregate {
                company_id: row.get(0)?,
                tenant_id: row.get(1)?,
                name: row.get(2)?,
                location: row.get(3)?,
                vat_id: row.get(4)?,
                employees: row.get(5)?
            })
        })?;
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::{Connection, Result, Transaction};
    use crate::company_aggregate::CompanyAggregate;
    use crate::company_event::CompanyData;
    use crate::database::company_aggregate_dao::CompanyAggregateDAO;
    use crate::domain::company_post::CompanyPost;
    use crate::domain::company_put::CompanyPut;
    use crate::patch::Patch;

    #[test]
    fn test_insert() {
        let company1 = CompanyPost{
            tenant_id: 10,
            name: String::from("Foo"),
            location: Some(String::from("Germany")),
            vat_id: Some(123),
            employees: Some(50)
        };
        let company2 = CompanyPost{
            tenant_id: 20,
            name: String::from("Baz"),
            location: Some(String::from("Spain")),
            vat_id: None,
            employees: Some(100)
        };

        let mut conn = create_connection();
        assert!(CompanyAggregateDAO::create_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();
        let company_id1 = CompanyAggregateDAO::insert(&tx, &company1);
        assert!(company_id1.is_ok());
        assert_eq!(company_id1.unwrap(), 1);
        let company_id2 = CompanyAggregateDAO::insert(&tx, &company2);
        assert!(company_id2.is_ok());
        assert_eq!(company_id2.unwrap(), 2);
        assert!(tx.commit().is_ok());

        let ref_companies = [
            &CompanyAggregate{
                company_id: 1,
                tenant_id: 10,
                name: String::from("Foo"),
                location: Some(String::from("Germany")),
                vat_id: Some(123),
                employees: Some(50)
            },
            &CompanyAggregate{
                company_id: 2,
                tenant_id: 20,
                name: String::from("Baz"),
                location: Some(String::from("Spain")),
                vat_id: None,
                employees: Some(100)
            }
        ];
        check_results(&mut conn, &ref_companies);
        check_single_result(&mut conn, 1, ref_companies[0]);
        check_single_result(&mut conn, 2, ref_companies[1]);
    }

    #[test]
    fn test_update() {
        let company = CompanyPost{
            tenant_id: 10,
            name: String::from("Foo"),
            location: Some(String::from("Germany")),
            vat_id: Some(123),
            employees: Some(50)
        };

        let company_update = CompanyPut{
            tenant_id: Some(20),
            name: None,
            location: Patch::Null,
            vat_id: Patch::Absent,
            employees: Patch::Value(100)
        };

        let mut conn = create_connection();
        assert!(CompanyAggregateDAO::create_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();
        assert!(CompanyAggregateDAO::insert(&tx, &company).is_ok());
        assert!(CompanyAggregateDAO::update(&tx, 1, &company_update).is_ok());
        assert!(tx.commit().is_ok());

        let ref_companies = [
            &CompanyAggregate{
                company_id: 1,
                tenant_id: 20,
                name: String::from("Foo"),
                location: None,
                vat_id: Some(123),
                employees: Some(100)
            }
        ];
        check_results(&mut conn, &ref_companies);
        check_single_result(&mut conn, 1, ref_companies[0]);
    }

    #[test]
    fn test_delete() {
        let company = CompanyPost{
            tenant_id: 10,
            name: String::from("Foo"),
            location: Some(String::from("Germany")),
            vat_id: Some(123),
            employees: Some(50)
        };

        let mut conn = create_connection();
        assert!(CompanyAggregateDAO::create_table(&conn).is_ok());

        let tx = conn.transaction().unwrap();
        assert!(CompanyAggregateDAO::insert(&tx, &company).is_ok());
        assert!(CompanyAggregateDAO::delete(&tx, 1).is_ok());
        assert!(tx.commit().is_ok());

        check_results(&mut conn, &[]);
    }

    fn create_connection() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        conn.unwrap()
    }

    fn check_results(conn: &mut Connection, ref_companies: &[&CompanyAggregate]) {
        let tx = conn.transaction().unwrap();

        let companies = CompanyAggregateDAO::read_all(&tx);
        assert!(companies.is_ok());
        assert!(tx.commit().is_ok());

        let companies = companies.unwrap();
        assert_eq!(companies.len(), ref_companies.len());

        for (index, &ref_company) in ref_companies.iter().enumerate() {
            assert_eq!(companies[index], *ref_company);
        }
    }

    fn check_single_result(conn: &mut Connection, company_id: u32, ref_company: &CompanyAggregate) {
        let tx = conn.transaction().unwrap();

        let company = CompanyAggregateDAO::read(&tx, company_id);
        assert!(company.is_ok());
        assert!(tx.commit().is_ok());

        let company = company.unwrap();
        assert_eq!(company, *ref_company);
    }
}