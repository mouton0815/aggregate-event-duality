use std::error::Error;
use log::{info, warn};
use rusqlite::{Connection, Transaction};
use crate::database::company_aggregate_table::{create_company_aggregate_table, delete_company_aggregate, insert_company_aggregate, read_company_aggregate, read_company_aggregates, update_company_aggregate};
use crate::database::company_event_table::{create_company_event_table, insert_company_event, read_company_events};
use crate::database::revision_table::{create_revision_table, read_company_revision, upsert_company_revision};
use crate::domain::company_aggregate::CompanyAggregate;
use crate::domain::company_event::{CompanyData, CompanyEvent};
use crate::domain::company_rest::{CompanyPost, CompanyPatch};
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
        let aggregate = read_company_aggregate(&tx, company_id)?.unwrap(); // Must exist
        let event = Self::create_event_for_post(company_id, company);
        Self::write_event_and_revision(&tx, &event)?;
        tx.commit()?;
        info!("Created {:?} from {:?}", aggregate, company);
        Ok(aggregate)
    }

    pub fn update(&mut self, company_id: u32, company: &CompanyPatch) -> Result<Option<CompanyAggregate>, rusqlite::Error> {
        let tx = self.conn.transaction()?;
        if update_company_aggregate(&tx, company_id, &company)? {
            let aggregate = read_company_aggregate(&tx, company_id)?.unwrap(); // Must exist
            let event = Self::create_event_for_patch(company_id, aggregate.tenant_id, company);
            Self::write_event_and_revision(&tx, &event)?;
            tx.commit()?;
            info!("Updated {:?} from {:?}", aggregate, company);
            Ok(Some(aggregate))
        } else {
            tx.rollback()?; // There should be no changes, so tx.commit() would also work
            warn!("Company aggregate {} not found", company_id);
            Ok(None)
        }
    }

    pub fn delete(&mut self, company_id: u32) -> Result<Option<CompanyAggregate>, Box<dyn Error>> {
        let tx = self.conn.transaction()?;
        match read_company_aggregate(&tx, company_id)? { // Read the aggregate first because we need the tenant_id
            Some(aggregate) => {
                delete_company_aggregate(&tx, company_id)?;
                let event = Self::create_event_for_delete(company_id, aggregate.tenant_id);
                Self::write_event_and_revision(&tx, &event)?;
                tx.commit()?;
                info!("Deleted {:?}", aggregate);
                Ok(Some(aggregate))
            },
            None => {
                tx.rollback()?; // There should be no changes, so tx.commit() would also work
                warn!("Company aggregate {} not found", company_id);
                Ok(None)
            }
        }
    }

    pub fn get_aggregates(&mut self) -> Result<(u32, Vec<CompanyAggregate>), Box<dyn Error>> {
        let tx = self.conn.transaction()?; // TODO: Can we have read-only transactions?
        let revision = read_company_revision(&tx)?;
        let companies = read_company_aggregates(&tx)?;
        tx.commit()?;
        Ok((revision, companies))
    }

    pub fn get_events(&mut self, from_revision: u32) -> Result<Vec<String>, Box<dyn Error>> {
        let tx = self.conn.transaction()?; // TODO: Can we have read-only transactions?
        let events = read_company_events(&tx, from_revision)?;
        tx.commit()?;
        Ok(events)
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

    fn create_event_for_patch(company_id: u32, tenant_id: u32, company: &CompanyPatch) -> CompanyEvent {
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

    fn write_event_and_revision(tx: &Transaction, event: &CompanyEvent) -> Result<u32, rusqlite::Error> {
        match serde_json::to_string(&event) {
            Ok(json) => {
                let revision = insert_company_event(&tx, json.as_str())?;
                upsert_company_revision(&tx, revision)?;
                Ok(revision)
            },
            Err(error) => {
                Err(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregator::company_aggregator::CompanyAggregator;
    use crate::database::company_event_table::read_company_events;
    use crate::database::revision_table::read_company_revision;
    use crate::domain::company_aggregate::CompanyAggregate;
    use crate::domain::company_rest::{CompanyPost, CompanyPatch};
    use crate::util::patch::Patch;

    #[test]
    pub fn test_create() {
        let mut aggregator = create_aggregator();

        let company = create_company_post();
        let company_res = aggregator.create(&company);
        assert!(company_res.is_ok());

        let company_ref = create_company_ref();
        assert_eq!(company_res.unwrap(), company_ref);

        check_events_and_revision(&mut aggregator, 1);
    }

    #[test]
    pub fn test_update() {
        let mut aggregator = create_aggregator();

        let company = create_company_post();
        let company_update = create_company_patch();
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

        assert_eq!(company_res.unwrap(), Some(company_ref));

        check_events_and_revision(&mut aggregator, 2);
    }

    #[test]
    pub fn test_update_missing() {
        let mut aggregator = create_aggregator();

        let company_update = create_company_patch();
        let company_res = aggregator.update(1, &company_update);
        assert!(company_res.is_ok());
        assert_eq!(company_res.unwrap(), None);
    }

    #[test]
    pub fn test_delete() {
        let mut aggregator = create_aggregator();

        let company = create_company_post();
        let company_res = aggregator.create(&company);
        assert!(company_res.is_ok());
        let company_res = aggregator.delete(1);
        assert!(company_res.is_ok());
        let company_res = company_res.unwrap();
        assert!(company_res.is_some());

        let company_ref = create_company_ref();
        assert_eq!(company_res.unwrap(), company_ref);

        check_events_and_revision(&mut aggregator, 2);
    }

    #[test]
    pub fn test_get_aggregates_empty() {
        let mut aggregator = create_aggregator();

        let companies_res = aggregator.get_aggregates();
        assert!(companies_res.is_ok());

        let company_ref = (0, Vec::new());
        assert_eq!(companies_res.unwrap(), company_ref);
    }

    #[test]
    pub fn test_get_aggregates() {
        let mut aggregator = create_aggregator();

        let company = create_company_post();
        assert!(aggregator.create(&company).is_ok());
        let companies_res = aggregator.get_aggregates();
        assert!(companies_res.is_ok());

        let company_ref = (1, vec!(create_company_ref()));
        assert_eq!(companies_res.unwrap(), company_ref);
    }

    #[test]
    pub fn test_get_events() {
        let mut aggregator = create_aggregator();

        let company = create_company_post();
        let company_update = create_company_patch();
        assert!(aggregator.create(&company).is_ok());
        assert!(aggregator.update(1, &company_update).is_ok());

        let event_ref1 = r#"{"tenantId":10,"companyId":1,"data":{"name":"Foo","employees":75}}"#;
        let event_ref2 = r#"{"tenantId":20,"companyId":1,"data":{"name":"Bar","location":"Nowhere","vatId":12345,"employees":null}}"#;
        get_events_and_compare(&mut aggregator, 0, &[&event_ref1, &event_ref2]);
        get_events_and_compare(&mut aggregator, 1, &[&event_ref1, &event_ref2]);
        get_events_and_compare(&mut aggregator, 2, &[&event_ref2]);
        get_events_and_compare(&mut aggregator, 3, &[]);
    }

    fn create_aggregator() -> CompanyAggregator {
        let aggregator = CompanyAggregator::new(":memory:");
        assert!(aggregator.is_ok());
        aggregator.unwrap()
    }

    fn create_company_post() -> CompanyPost {
        CompanyPost {
            tenant_id: 10,
            name: String::from("Foo"),
            location: None,
            vat_id: None,
            employees: Some(75)
        }
    }

    fn create_company_patch() -> CompanyPatch {
        CompanyPatch {
            tenant_id: Some(20),
            name: Some(String::from("Bar")),
            location: Patch::Value(String::from("Nowhere")),
            vat_id: Patch::Value(12345),
            employees: Patch::Null
        }
    }

    fn create_company_ref() -> CompanyAggregate {
        CompanyAggregate {
            company_id: 1,
            tenant_id: 10,
            name: String::from("Foo"),
            location: None,
            vat_id: None,
            employees: Some(75)
        }
    }

    fn get_events_and_compare(aggregator: &mut CompanyAggregator, from_revision: u32, ref_events: &[&str]) {
        let events = aggregator.get_events(from_revision);
        assert!(events.is_ok());
        let events = events.unwrap();
        assert_eq!(events.len(), ref_events.len());
        for (index, &ref_event) in ref_events.iter().enumerate() {
            assert_eq!(events[index], *ref_event);
        }
    }

    fn check_events_and_revision(aggregator: &mut CompanyAggregator, revision_ref: u32) {
        let tx = aggregator.conn.transaction().unwrap();
        let revision = read_company_revision(&tx);
        assert!(revision.is_ok());
        assert_eq!(revision.unwrap(), revision_ref);
        // TODO: Better use aggregator.get_events(0), but this means duplicate borrowing
        let events = read_company_events(&tx, 0);
        assert!(events.is_ok());
        assert_eq!(events.unwrap().len(), revision_ref as usize);
    }
}