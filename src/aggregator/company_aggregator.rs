use std::error::Error;
use rusqlite::Connection;
use crate::company_aggregate::CompanyAggregate;
use crate::database::company_aggregate_dao::CompanyAggregateDAO;
use crate::domain::company_post::CompanyPost;
use crate::domain::company_put::CompanyPut;

pub struct CompanyAggregator {
    conn: Connection
}

impl CompanyAggregator {
    pub fn new(db_path: &str) -> Result<CompanyAggregator, Box<dyn Error>> {
        let conn = Connection::open(db_path)?;
        CompanyAggregateDAO::create_table(&conn)?;
        Ok(CompanyAggregator{ conn })
    }

    pub fn create(&mut self, company: &CompanyPost) -> Result<CompanyAggregate, Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        let company_id = CompanyAggregateDAO::insert(&tx, &company)?;
        let company_aggregate = CompanyAggregateDAO::read(&tx, company_id)?;
        tx.commit()?;
        Ok(company_aggregate)
    }

    pub fn update(&mut self, company_id: u32, company: &CompanyPut) -> Result<CompanyAggregate, Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        CompanyAggregateDAO::update(&tx, company_id, &company)?;
        let company_aggregate = CompanyAggregateDAO::read(&tx, company_id)?;
        tx.commit()?;
        Ok(company_aggregate)
    }

    pub fn delete(&mut self, company_id: u32) -> Result<(), Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        CompanyAggregateDAO::delete(&tx, company_id)?;
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregator::company_aggregator::CompanyAggregator;
    use crate::company_aggregate::CompanyAggregate;
    use crate::domain::company_post::CompanyPost;
    use crate::domain::company_put::CompanyPut;
    use crate::patch::Patch;

    #[test]
    pub fn test_create() {
        let company = CompanyPost {
            tenant_id: 10,
            name: String::from("Foo"),
            location: None,
            vat_id: None,
            employees: Some(75)
        };

        let aggregator = CompanyAggregator::new(":memory:");
        assert!(aggregator.is_ok());
        let mut aggregator = aggregator.unwrap();

        let company_res = aggregator.create(&company);
        assert!(company_res.is_ok());

        let company_ref = CompanyAggregate {
            company_id: 1,
            tenant_id: 10,
            name: String::from("Foo"),
            location: None,
            vat_id: None,
            employees: Some(75)
        };

        assert_eq!(company_res.unwrap(), company_ref);
    }

    pub fn test_update() {
        let company = CompanyPost {
            tenant_id: 10,
            name: String::from("Foo"),
            location: None,
            vat_id: None,
            employees: Some(75)
        };
        let company_update = CompanyPut {
            tenant_id: Some(20),
            name: Some(String::from("Bar")),
            location: Patch::Value(String::from("Nowhere")),
            vat_id: Patch::Value(12345),
            employees: Patch::Null
        };

        let aggregator = CompanyAggregator::new(":memory:");
        assert!(aggregator.is_ok());
        let mut aggregator = aggregator.unwrap();

        let company_res = aggregator.create(&company);
        assert!(company_res.is_ok());
        let company_res = aggregator.update(1, &company_update);
        assert!(company_res.is_ok());

        let company_ref = CompanyAggregate {
            company_id: 1,
            tenant_id: 20,
            name: String::from("Foo"),
            location: Some(String::from("Nowhere")),
            vat_id: Some(12345),
            employees: None
        };

        assert_eq!(company_res.unwrap(), company_ref);
    }
}