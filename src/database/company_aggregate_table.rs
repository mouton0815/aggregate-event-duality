use const_format::formatcp;
use log::{debug, error};
use rusqlite::{Connection, Error, params, Result, Row, ToSql, Transaction};
use crate::domain::company_aggregate::CompanyAggregate;
use crate::domain::company_rest::{CompanyPost, CompanyPatch};

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

pub fn create_company_aggregate_table(conn: &Connection) -> Result<()> {
    debug!("Execute {}", CREATE_COMPANY_TABLE);
    conn.execute(CREATE_COMPANY_TABLE, [])?;
    Ok(())
}

pub fn insert_company_aggregate(tx: &Transaction, company: &CompanyPost) -> Result<u32> {
    debug!("Execute {}\nwith: {:?}", INSERT_COMPANY, company);
    let values = params![company.tenant_id, company.name, company.location, company.vat_id, company.employees];
    tx.execute(INSERT_COMPANY, values)?;
    Ok(tx.last_insert_rowid() as u32)
}

pub fn update_company_aggregate(tx: &Transaction, company_id: u32, company: &CompanyPatch) -> Result<bool> {
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
        error!("Do not run update query because all non-id values are missing");
        return Err(Error::InvalidParameterCount(0, 5));
    }
    let query = format!("UPDATE {} SET {} WHERE companyId=?", COMPANY_AGGREGATE_TABLE, columns.join(",").as_str());
    values.push(&company_id);
    debug!("Execute\n{}\nwith: {:?}", query, company);
    let row_count = tx.execute(query.as_str(), values.as_slice())?;
    Ok(row_count == 1)
}

pub fn delete_company_aggregate(tx: &Transaction, company_id: u32) -> Result<bool> {
    debug!("Execute {} with: {}", DELETE_COMPANY, company_id);
    let row_count = tx.execute(DELETE_COMPANY, params![company_id])?;
    Ok(row_count == 1)
}

pub fn read_company_aggregates(tx: &Transaction) -> Result<Vec<CompanyAggregate>> {
    debug!("Execute {}", SELECT_COMPANIES);
    let mut stmt = tx.prepare(SELECT_COMPANIES)?;
    let rows = stmt.query_map([], |row| {
        row_to_company_aggregate(row)
    })?;
    let mut companies = Vec::new();
    for row in rows {
        companies.push(row?);
    }
    Ok(companies)
}

pub fn read_company_aggregate(tx: &Transaction, company_id: u32) -> Result<CompanyAggregate> {
    debug!("Execute {} with: {}", SELECT_COMPANY, company_id);
    let mut stmt = tx.prepare(SELECT_COMPANY)?;
    let row = stmt.query_row([company_id], |row| {
        row_to_company_aggregate(row)
    })?;
    Ok(row)
}

fn row_to_company_aggregate(row: &Row) -> Result<CompanyAggregate> {
    Ok(CompanyAggregate {
        company_id: row.get(0)?,
        tenant_id: row.get(1)?,
        name: row.get(2)?,
        location: row.get(3)?,
        vat_id: row.get(4)?,
        employees: row.get(5)?
    })
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use crate::database::company_aggregate_table::{create_company_aggregate_table, delete_company_aggregate, insert_company_aggregate, read_company_aggregate, read_company_aggregates, update_company_aggregate};
    use crate::domain::company_aggregate::CompanyAggregate;
    use crate::domain::company_rest::{CompanyPost, CompanyPatch};
    use crate::util::patch::Patch;

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

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let company_id1 = insert_company_aggregate(&tx, &company1);
        assert!(company_id1.is_ok());
        assert_eq!(company_id1.unwrap(), 1);
        let company_id2 = insert_company_aggregate(&tx, &company2);
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

        let company_update = CompanyPatch {
            tenant_id: Some(20),
            name: None,
            location: Patch::Null,
            vat_id: Patch::Absent,
            employees: Patch::Value(100)
        };

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(insert_company_aggregate(&tx, &company).is_ok());
        let result = update_company_aggregate(&tx, 1, &company_update);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
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

    fn test_update_missing() {
        let company_update = CompanyPatch {
            tenant_id: Some(20),
            name: None,
            location: Patch::Null,
            vat_id: Patch::Absent,
            employees: Patch::Value(100)
        };

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        let result = update_company_aggregate(&tx, 1, &company_update);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_delete() { // TODO: Check return value!
        let company = CompanyPost{
            tenant_id: 10,
            name: String::from("Foo"),
            location: Some(String::from("Germany")),
            vat_id: Some(123),
            employees: Some(50)
        };

        let mut conn = create_connection_and_table();
        let tx = conn.transaction().unwrap();
        assert!(insert_company_aggregate(&tx, &company).is_ok());
        assert!(delete_company_aggregate(&tx, 1).is_ok());
        assert!(tx.commit().is_ok());

        check_results(&mut conn, &[]);
    }

    fn create_connection_and_table() -> Connection {
        let conn = Connection::open(":memory:");
        assert!(conn.is_ok());
        let conn = conn.unwrap();
        assert!(create_company_aggregate_table(&conn).is_ok());
        conn
    }

    fn check_results(conn: &mut Connection, ref_companies: &[&CompanyAggregate]) {
        let tx = conn.transaction().unwrap();

        let companies = read_company_aggregates(&tx);
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

        let company = read_company_aggregate(&tx, company_id);
        assert!(company.is_ok());
        assert!(tx.commit().is_ok());

        let company = company.unwrap();
        assert_eq!(company, *ref_company);
    }
}