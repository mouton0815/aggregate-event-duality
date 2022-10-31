use std::error::Error;
use rusqlite::{Connection, Transaction};
use crate::database::company_aggregate_table::{create_company_aggregate_table, delete_company_aggregate, insert_company_aggregate, read_company_aggregate, update_company_aggregate};
use crate::database::company_event_table::{create_company_event_table, insert_company_event};
use crate::database::revision_table::{create_revision_table, upsert_company_revision};
use crate::domain::company_aggregate::CompanyAggregate;
use crate::domain::company_event::{CompanyData, CompanyEvent};
use crate::domain::company_rest::{CompanyPost, CompanyPut};
use crate::util::patch::Patch;

pub struct CompanyAggregator {
    conn: Connection
}

impl CompanyAggregator {
    pub fn new(db_path: &str) -> Result<CompanyAggregator, Box<dyn Error>> {
        let conn = Connection::open(db_path)?;
        create_company_aggregate_table(&conn)?;
        create_company_event_table(&conn)?;
        create_revision_table(&conn)?;
        Ok(CompanyAggregator{ conn })
    }

    pub fn create(&mut self, company: &CompanyPost) -> Result<CompanyAggregate, Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        let company_id = insert_company_aggregate(&tx, &company)?;
        let aggregate = read_company_aggregate(&tx, company_id)?;
        let event = Self::create_event_for_post(company_id, company);
        Self::write_event_and_revision(&tx, &event)?;
        tx.commit()?;
        Ok(aggregate)
    }

    pub fn update(&mut self, company_id: u32, company: &CompanyPut) -> Result<CompanyAggregate, Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        update_company_aggregate(&tx, company_id, &company)?;
        let aggregate = read_company_aggregate(&tx, company_id)?;
        let event = Self::create_event_for_put(company_id, aggregate.tenant_id, company);
        Self::write_event_and_revision(&tx, &event)?;
        tx.commit()?;
        Ok(aggregate)
    }

    pub fn delete(&mut self, company_id: u32) -> Result<CompanyAggregate, Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        let aggregate = read_company_aggregate(&tx, company_id)?;
        delete_company_aggregate(&tx, company_id)?;
        let event = Self::create_event_for_delete(company_id, aggregate.tenant_id);
        Self::write_event_and_revision(&tx, &event)?;
        tx.commit()?;
        Ok(aggregate)
    }

    fn create_event_for_post(company_id: u32, company: &CompanyPost) -> CompanyEvent {
        CompanyEvent{
            company_id,
            tenant_id: company.tenant_id,
            data: Patch::Value(CompanyData{
                name: Some(company.name.clone()),
                location: match &company.location {
                    Some(x) => Patch::Value(x.clone()),
                    None => Patch::Absent
                },
                vat_id: match company.vat_id {
                    Some(x) => Patch::Value(x),
                    None => Patch::Absent
                },
                employees: match company.employees {
                    Some(x) => Patch::Value(x),
                    None => Patch::Absent
                }
            })
        }
    }

    fn create_event_for_put(company_id: u32, tenant_id: u32, company: &CompanyPut) -> CompanyEvent {
        CompanyEvent{
            company_id,
            tenant_id,
            data: Patch::Value(CompanyData{
                name: company.name.clone(),
                location: company.location.clone(),
                vat_id: company.vat_id.clone(),
                employees: company.employees.clone()
            })
        }
    }

    fn create_event_for_delete(company_id: u32, tenant_id: u32) -> CompanyEvent {
        CompanyEvent{
            company_id,
            tenant_id,
            data: Patch::Null
        }
    }

    fn write_event_and_revision(tx: &Transaction, event: &CompanyEvent) -> Result<u32, Box<dyn Error>> {
        let json = serde_json::to_string(&event)?;
        let revision = insert_company_event(&tx, json.as_str())?;
        upsert_company_revision(&tx, revision)?;
        Ok(revision)
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregator::company_aggregator::CompanyAggregator;
    use crate::database::company_event_table::read_company_events;
    use crate::database::revision_table::read_company_revision;
    use crate::domain::company_aggregate::CompanyAggregate;
    use crate::domain::company_rest::{CompanyPost, CompanyPut};
    use crate::util::patch::Patch;

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

        check_events_and_revision(&mut aggregator, 1);
    }

    #[test]
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
            name: String::from("Bar"),
            location: Some(String::from("Nowhere")),
            vat_id: Some(12345),
            employees: None
        };

        assert_eq!(company_res.unwrap(), company_ref);

        check_events_and_revision(&mut aggregator, 2);
    }

    #[test]
    pub fn test_delete() {
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
        let company_res = aggregator.delete(1);
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

        check_events_and_revision(&mut aggregator, 2);
    }

    fn check_events_and_revision(aggregator: &mut CompanyAggregator, revision_ref: u32) {
        let tx = aggregator.conn.transaction().unwrap();
        let revision = read_company_revision(&tx);
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), revision_ref);
        let events = read_company_events(&tx, 0);
        assert!(events.is_ok());
        assert_eq!(events.unwrap().len(), revision_ref as usize);
    }
}