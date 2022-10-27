use serde::{Serialize, Deserialize};
use crate::patch::Patch;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum CompanyEventType {
    Create,
    Update,
    Delete
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CompanyEventData {
    #[serde(default)]
    #[serde(skip_serializing_if = "Patch::is_absent")]
    pub name: Patch<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Patch::is_absent")]
    pub location: Patch<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Patch::is_absent")]
    pub vat_id: Patch<u32>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Patch::is_absent")]
    pub employees: Patch<u32>
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CompanyEvent {
    pub event_type: CompanyEventType,
    pub tenant_id: u32,
    pub company_id: u32,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<CompanyEventData>
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use serde::{Deserialize, Serialize};
    use crate::patch::Patch;
    use crate::company_event::{CompanyEvent, CompanyEventData, CompanyEventType};

    #[test]
    pub fn test_serde_company_create_event() {
        let company_ref = CompanyEvent{
            event_type: CompanyEventType::Create,
            tenant_id: 1,
            company_id: 10,
            payload: Some(CompanyEventData{
                name: Patch::Value(String::from("Foo & Bar")),
                location: Patch::Value(String::from("Nowhere")),
                vat_id: Patch::Value(12345),
                employees: Patch::Value(75)
            })
        };

        let json_ref = r#"{"eventType":"Create","tenantId":1,"companyId":10,"payload":{"name":"Foo & Bar","location":"Nowhere","vatId":12345,"employees":75}}"#;

        serde_and_verify(&company_ref, json_ref);
    }

    #[test]
    pub fn test_serde_company_update_event() {
        let company_ref = CompanyEvent{
            event_type: CompanyEventType::Update,
            tenant_id: 1,
            company_id: 10,
            payload: Some(CompanyEventData{
                name: Patch::Null,
                location: Patch::Absent,
                vat_id: Patch::Null,
                employees: Patch::Absent
            })
        };

        let json_ref = r#"{"eventType":"Update","tenantId":1,"companyId":10,"payload":{"name":null,"vatId":null}}"#;

        serde_and_verify(&company_ref, json_ref);
    }

    #[test]
    pub fn test_serde_company_delete_event() {
        let company_ref = CompanyEvent{
            event_type: CompanyEventType::Delete,
            tenant_id: 1,
            company_id: 10,
            payload: None
        };

        let json_ref = r#"{"eventType":"Delete","tenantId":1,"companyId":10}"#;

        serde_and_verify(&company_ref, json_ref);
    }

    fn serde_and_verify<'a, CompanyEvent>(company_ref: &CompanyEvent, json_ref: &'a str)
        where CompanyEvent: Serialize + Deserialize<'a> + PartialEq + Debug {

        // 1. Serialize company_ref and string-compare it to json_ref
        let json = serde_json::to_string(&company_ref);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), String::from(json_ref));

        // 2. Deserialize the serialized json and compare it with company_ref
        let company: Result<CompanyEvent, serde_json::Error> = serde_json::from_str(json_ref);
        assert!(company.is_ok());
        assert_eq!(company.unwrap(), *company_ref);
    }
}
